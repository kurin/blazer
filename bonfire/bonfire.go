// Copyright 2018, Google
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package bonfire implements the B2 service.
package bonfire

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc/metadata"

	"github.com/golang/protobuf/proto"
	"github.com/kurin/blazer/internal/b2types"
	"github.com/kurin/blazer/internal/pyre"
)

func getAuth(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("no metadata")
	}
	data := md.Get("authentication")
	if len(data) == 0 {
		return "", nil
	}
	return data[0], nil
}

type Bonfire struct {
	Root       string
	mu         sync.Mutex
	buckets    map[int][]byte
	nextBucket int
	files      map[string][]byte
	nextFile   int
}

type LargeFileServer struct{}

type uploadPartRequest struct {
	File string `json:"fileId"`
	Part int    `json:"partNumber"`
	Size int64  `json:"contentLength"`
	Hash string `json:"contentSha1"`
}

func parseUploadPartHeaders(r *http.Request) (uploadPartRequest, error) {
	var ur uploadPartRequest
	ur.Hash = r.Header.Get("X-Bz-Content-Sha1")
	part, err := strconv.ParseInt(r.Header.Get("X-Bz-Part-Number"), 10, 64)
	if err != nil {
		return ur, err
	}
	ur.Part = int(part)
	size, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return ur, err
	}
	ur.Size = size
	return ur, nil
}

func (fs *LargeFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := parseUploadPartHeaders(r)
	if err != nil {
		http.Error(w, err.Error(), 500)
		fmt.Println("oh no")
		return
	}
	if err := json.NewEncoder(w).Encode(req); err != nil {
		fmt.Println("oh no")
	}
	fmt.Println("served!")
}

type SimpleFileServer struct{}

type uploadRequest struct {
	name        string
	contentType string
	size        int64
	sha1        string
	info        map[string]string
}

func parseUploadHeaders(r *http.Request) (*uploadRequest, error) {
	ur := &uploadRequest{info: make(map[string]string)}
	ur.name = r.Header.Get("X-Bz-File-Name")
	ur.contentType = r.Header.Get("Content-Type")
	ur.sha1 = r.Header.Get("X-Bz-Content-Sha1")
	size, err := strconv.ParseInt(r.Header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, err
	}
	ur.size = size
	for k := range r.Header {
		if !strings.HasPrefix("X-Bz-Info-", k) {
			continue
		}
		name := strings.TrimPrefix("X-Bz-Info-", k)
		ur.info[name] = r.Header.Get(k)
	}
	return ur, nil
}

func (fs *SimpleFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := parseUploadHeaders(r)
	if err != nil {
		http.Error(w, err.Error(), 500)
		fmt.Println("oh no")
		return
	}
	resp := &b2types.UploadFileResponse{
		Name: req.name,
		Size: req.size,
		Info: req.info,
	}
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		fmt.Println("oh no")
	}
	fmt.Println("served!")
}

func (b *Bonfire) AuthorizeAccount(ctx context.Context, req *pyre.AuthorizeAccountRequest) (*pyre.AuthorizeAccountResponse, error) {
	return &pyre.AuthorizeAccountResponse{
		ApiUrl: b.Root,
	}, nil
}

func (b *Bonfire) ListBuckets(context.Context, *pyre.ListBucketsRequest) (*pyre.ListBucketsResponse, error) {
	resp := &pyre.ListBucketsResponse{}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, bs := range b.buckets {
		var bucket pyre.Bucket
		if err := proto.Unmarshal(bs, &bucket); err != nil {
			return nil, err
		}
		resp.Buckets = append(resp.Buckets, &bucket)
	}
	return resp, nil
}

func (b *Bonfire) CreateBucket(ctx context.Context, req *pyre.Bucket) (*pyre.Bucket, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	n := b.nextBucket
	b.nextBucket++
	req.BucketId = fmt.Sprintf("%d", n)
	bs, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	if b.buckets == nil {
		b.buckets = make(map[int][]byte)
	}
	b.buckets[n] = bs
	return req, nil
}

func (b *Bonfire) DeleteBucket(ctx context.Context, req *pyre.Bucket) (*pyre.Bucket, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, err := strconv.ParseInt(req.BucketId, 10, 64)
	if err != nil {
		return nil, err
	}
	bs, ok := b.buckets[int(idx)]
	if !ok {
		return nil, fmt.Errorf("no such bucket: %v", req.BucketId)
	}
	if err := proto.Unmarshal(bs, req); err != nil {
		return nil, err
	}
	delete(b.buckets, int(idx))
	return req, nil
}

func (b *Bonfire) GetUploadUrl(ctx context.Context, req *pyre.GetUploadUrlRequest) (*pyre.GetUploadUrlResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, err := strconv.ParseInt(req.BucketId, 10, 64)
	if err != nil {
		return nil, err
	}
	if _, ok := b.buckets[int(idx)]; !ok {
		return nil, fmt.Errorf("no such bucket: %v", req.BucketId)
	}
	b.nextFile++
	return &pyre.GetUploadUrlResponse{
		UploadUrl: fmt.Sprintf("%s/b2api/v1/b2_upload_file/%s/%d", b.Root, req.BucketId, b.nextFile),
		BucketId:  req.BucketId,
	}, nil
}

func (b *Bonfire) StartLargeFile(ctx context.Context, req *pyre.StartLargeFileRequest) (*pyre.StartLargeFileResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return &pyre.StartLargeFileResponse{
		BucketId: req.BucketId,
	}, nil
}

func (b *Bonfire) GetUploadPartUrl(ctx context.Context, req *pyre.GetUploadPartUrlRequest) (*pyre.GetUploadPartUrlResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return &pyre.GetUploadPartUrlResponse{
		UploadUrl: fmt.Sprintf("%s/b2api/v1/b2_upload_part/wooooo", b.Root),
	}, nil
}
