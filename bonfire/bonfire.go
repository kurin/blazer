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
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/kurin/blazer/internal/pyre"
)

func MuxOpts() []runtime.ServeMuxOption {
	var opts []runtime.ServeMuxOption
	opts = append(opts, runtime.WithIncomingHeaderMatcher(func(s string) (string, bool) {
		if m, ok := runtime.DefaultHeaderMatcher(s); ok {
			return m, ok
		}
		switch strings.ToLower(s) {
		case "x-bz-file-name", "content-type", "content-length", "x-bz-content-sha1":
			return s, true
		}
		if strings.HasPrefix(s, "X-Bz-Info-") {
			return s, true
		}
		return "", false
	}))
	return opts
}

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
}

func (b *Bonfire) AuthorizeAccount(ctx context.Context, req *pyre.AuthorizeAccountRequest) (*pyre.AuthorizeAccountResponse, error) {
	auth, err := getAuth(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Println(auth)
	return &pyre.AuthorizeAccountResponse{}, nil
}

func (b *Bonfire) GetUploadUrl(context.Context, *pyre.GetUploadUrlRequest) (*pyre.GetUploadUrlResponse, error) {
	return &pyre.GetUploadUrlResponse{}, nil
}

func (b *Bonfire) UploadFile(context.Context, *pyre.UploadFileRequest) (*pyre.UploadFileResponse, error) {
	return &pyre.UploadFileResponse{}, nil
}
