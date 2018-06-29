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

package pyre

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "github.com/kurin/blazer/internal/pyre/proto"
)

func serveMuxOptions() []runtime.ServeMuxOption {
	var opts []runtime.ServeMuxOption
	opts = append(opts, runtime.WithMarshalerOption("*", &runtime.JSONPb{}))
	return opts
}

func getAuth(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", errors.New("no metadata")
	}
	data := md.Get("authorization")
	if len(data) == 0 {
		return "", nil
	}
	return data[0], nil
}

func RegisterServerOnMux(ctx context.Context, srv *Server, mux *http.ServeMux) error {
	rmux := runtime.NewServeMux(serveMuxOptions()...)
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return err
	}
	gsrv := grpc.NewServer()
	if err := pb.RegisterPyreServiceHandlerFromEndpoint(ctx, rmux, l.Addr().String(), []grpc.DialOption{grpc.WithInsecure()}); err != nil {
		return err
	}
	pb.RegisterPyreServiceServer(gsrv, srv)
	mux.Handle("/b2api/v1/", rmux)
	go gsrv.Serve(l)
	go func() {
		<-ctx.Done()
		gsrv.GracefulStop()
	}()
	return nil
}

type AccountManager interface {
	Authorize(acct, key string) (string, error)
	CheckCreds(token, api string) error
	APIRoot(acct string) string
	DownloadRoot(acct string) string
}

type BucketManager interface {
	Add(id string, bs []byte) error
	Remove(id string) error
	Update(id string, rev int, bs []byte) error
	List(acct string) ([][]byte, error)
	Get(id string) ([]byte, error)
	SimpleUploadHost(id string) (string, error) // does this belong here?
}

type LargeFileOrganizer interface {
	Sizes(acct string) (recommended, minimum int32)
	Host(fileId string) (string, error)
	Start(bucketId, fileId string, bs []byte) error
	Get(fileId string) ([]byte, error)
	Finish(fileId string) error
}

type Server struct {
	Account   AccountManager
	Bucket    BucketManager
	LargeFile LargeFileOrganizer
}

func (s *Server) AuthorizeAccount(ctx context.Context, req *pb.AuthorizeAccountRequest) (*pb.AuthorizeAccountResponse, error) {
	auth, err := getAuth(ctx)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(auth, "Basic ") {
		return nil, errors.New("basic auth required")
	}
	auth = strings.TrimPrefix(auth, "Basic ")
	bs, err := base64.StdEncoding.DecodeString(auth)
	if err != nil {
		return nil, err
	}
	split := strings.Split(string(bs), ":")
	if len(split) != 2 {
		return nil, errors.New("bad auth")
	}
	acct, key := split[0], split[1]
	token, err := s.Account.Authorize(acct, key)
	if err != nil {
		return nil, err
	}
	rec, min := s.LargeFile.Sizes(acct)
	return &pb.AuthorizeAccountResponse{
		AuthorizationToken:      token,
		ApiUrl:                  s.Account.APIRoot(acct),
		DownloadUrl:             s.Account.DownloadRoot(acct),
		RecommendedPartSize:     rec,
		MinimumPartSize:         rec,
		AbsoluteMinimumPartSize: min,
	}, nil
}

func (s *Server) ListBuckets(ctx context.Context, req *pb.ListBucketsRequest) (*pb.ListBucketsResponse, error) {
	resp := &pb.ListBucketsResponse{}
	buckets, err := s.Bucket.List(req.AccountId)
	if err != nil {
		return nil, err
	}
	for _, bs := range buckets {
		var bucket pb.Bucket
		if err := proto.Unmarshal(bs, &bucket); err != nil {
			return nil, err
		}
		resp.Buckets = append(resp.Buckets, &bucket)
	}
	return resp, nil
}

func (s *Server) CreateBucket(ctx context.Context, req *pb.Bucket) (*pb.Bucket, error) {
	req.BucketId = uuid.New().String()
	bs, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	if err := s.Bucket.Add(req.BucketId, bs); err != nil {
		return nil, err
	}
	return req, nil
}

func (s *Server) DeleteBucket(ctx context.Context, req *pb.Bucket) (*pb.Bucket, error) {
	bs, err := s.Bucket.Get(req.BucketId)
	if err != nil {
		return nil, err
	}
	var bucket pb.Bucket
	if err := proto.Unmarshal(bs, &bucket); err != nil {
		return nil, err
	}
	if err := s.Bucket.Remove(req.BucketId); err != nil {
		return nil, err
	}
	return &bucket, nil
}

func (s *Server) GetUploadUrl(ctx context.Context, req *pb.GetUploadUrlRequest) (*pb.GetUploadUrlResponse, error) {
	host, err := s.Bucket.SimpleUploadHost(req.BucketId)
	if err != nil {
		return nil, err
	}
	return &pb.GetUploadUrlResponse{
		UploadUrl: fmt.Sprintf("%s/b2api/v1/b2_upload_file/%s", host, req.BucketId),
		BucketId:  req.BucketId,
	}, nil
}

func (s *Server) StartLargeFile(ctx context.Context, req *pb.StartLargeFileRequest) (*pb.StartLargeFileResponse, error) {
	return &pb.StartLargeFileResponse{
		FileId:      uuid.New().String(),
		FileName:    req.FileName,
		BucketId:    req.BucketId,
		ContentType: req.ContentType,
		FileInfo:    req.FileInfo,
	}, nil
}

func (s *Server) GetUploadPartUrl(ctx context.Context, req *pb.GetUploadPartUrlRequest) (*pb.GetUploadPartUrlResponse, error) {
	host, err := s.LargeFile.Host(req.FileId)
	if err != nil {
		return nil, err
	}
	return &pb.GetUploadPartUrlResponse{
		UploadUrl: fmt.Sprintf("%s/b2api/v1/b2_upload_part/%s", host, req.FileId),
	}, nil
}

func (s *Server) FinishLargeFile(ctx context.Context, req *pb.FinishLargeFileRequest) (*pb.FinishLargeFileResponse, error) {
	return &pb.FinishLargeFileResponse{}, nil
}
