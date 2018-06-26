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

	"github.com/kurin/blazer/internal/pyre"
)

type Bonfire struct{}

func (b *Bonfire) AuthorizeAccount(context.Context, *pyre.AuthorizeAccountRequest) (*pyre.AuthorizeAccountResponse, error) {
	return &pyre.AuthorizeAccountResponse{}, nil
}

func (b *Bonfire) GetUploadUrl(context.Context, *pyre.GetUploadUrlRequest) (*pyre.GetUploadUrlResponse, error) {
	return &pyre.GetUploadUrlResponse{}, nil
}
