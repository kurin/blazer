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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

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
