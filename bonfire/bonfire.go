// Copyright 2016, Google
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

// Bonfire is a (not-production-worthy) implementation of the B2 API.  It is
// intended to be used for testing API clients against.
package main

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/kurin/blazer/internal/b2types"
)

func main() {
	f := &fire{
		users: map[string]string{
			"me": "myself",
		},
	}
	http.HandleFunc(b2types.V1api+"b2_authorize_account", f.authorizeAccount)
	glog.Fatal(http.ListenAndServe(":8080", nil))
}

type token struct {
	user    string
	expires time.Time
}

type fire struct {
	root   string            // The root directory; default is os.TempDir().
	users  map[string]string // A map of usernames to passwords.
	tokens map[string]token
}

func unauthorized(rw http.ResponseWriter) {
	rw.WriteHeader(http.StatusUnauthorized)
	err := &b2types.ErrorMessage{
		Status: http.StatusUnauthorized,
		Msg:    "unauthorized or no such user",
		Code:   "bad_user_no_account",
	}
	enc := json.NewEncoder(rw)
	if err := enc.Encode(err); err != nil {
		glog.Error(err)
	}
}

func (f *fire) authorizeAccount(rw http.ResponseWriter, req *http.Request) {
	user, pass, ok := req.BasicAuth()
	wpass, pok := f.users[user]
	if !ok || !pok || pass != wpass {
		unauthorized(rw)
		return
	}
	if f.tokens == nil {
		f.tokens = make(map[string]token)
	}
	f.tokens["ok"] = token{
		user:    user,
		expires: time.Now().Add(5 * time.Minute),
	}
	b2resp := &b2types.AuthorizeAccountResponse{
		AccountID:   user,
		AuthToken:   "ok",
		URI:         "http://localhost:8080",
		DownloadURI: "http://localhost:8080/file",
		MinPartSize: 1e3,
	}
	enc := json.NewEncoder(rw)
	if err := enc.Encode(b2resp); err != nil {
		glog.Error(err)
	}
}
