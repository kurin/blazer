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
package pyre

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/kurin/blazer/internal/pyre/proto"
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

func RegisterServerOnMux(ctx context.Context, srv *apiServer, mux *http.ServeMux) error {
	rmux := runtime.NewServeMux(serveMuxOptions()...)
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return err
	}
	gsrv := grpc.NewServer()
	if err := pb.RegisterPyreServiceHandlerFromEndpoint(ctx, rmux, l.Addr().String(), []grpc.DialOption{grpc.WithInsecure()}); err != nil {
		return err
	}
	mux.Handle("/b2api/v1/", rmux)
	go gsrv.Serve(l)
	go func() {
		<-ctx.Done()
		gsrv.GracefulStop()
	}()
	return nil
}

type apiServer struct {
	Root       string
	mu         sync.Mutex
	buckets    map[int][]byte
	nextBucket int
	files      map[string][]byte
	nextFile   int
}

func (b *apiServer) AuthorizeAccount(ctx context.Context, req *pb.AuthorizeAccountRequest) (*pb.AuthorizeAccountResponse, error) {
	return &pb.AuthorizeAccountResponse{
		ApiUrl: b.Root,
	}, nil
}

func (b *apiServer) ListBuckets(context.Context, *pb.ListBucketsRequest) (*pb.ListBucketsResponse, error) {
	resp := &pb.ListBucketsResponse{}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, bs := range b.buckets {
		var bucket pb.Bucket
		if err := proto.Unmarshal(bs, &bucket); err != nil {
			return nil, err
		}
		resp.Buckets = append(resp.Buckets, &bucket)
	}
	return resp, nil
}

func (b *apiServer) CreateBucket(ctx context.Context, req *pb.Bucket) (*pb.Bucket, error) {
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

func (b *apiServer) DeleteBucket(ctx context.Context, req *pb.Bucket) (*pb.Bucket, error) {
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

func (b *apiServer) GetUploadUrl(ctx context.Context, req *pb.GetUploadUrlRequest) (*pb.GetUploadUrlResponse, error) {
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
	return &pb.GetUploadUrlResponse{
		UploadUrl: fmt.Sprintf("%s/b2api/v1/b2_upload_file/%s/%d", b.Root, req.BucketId, b.nextFile),
		BucketId:  req.BucketId,
	}, nil
}

func (b *apiServer) StartLargeFile(ctx context.Context, req *pb.StartLargeFileRequest) (*pb.StartLargeFileResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return &pb.StartLargeFileResponse{
		BucketId: req.BucketId,
	}, nil
}

func (b *apiServer) GetUploadPartUrl(ctx context.Context, req *pb.GetUploadPartUrlRequest) (*pb.GetUploadPartUrlResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return &pb.GetUploadPartUrlResponse{
		UploadUrl: fmt.Sprintf("%s/b2api/v1/b2_upload_part/wooooo", b.Root),
	}, nil
}
