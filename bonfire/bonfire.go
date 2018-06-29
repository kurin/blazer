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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type FS string

func (f FS) open(fp string) (io.WriteCloser, error) {
	if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
		return nil, err
	}
	return os.Create(fp)
}

func (f FS) PartWriter(id string, part int) (io.WriteCloser, error) {
	fp := filepath.Join(string(f), id, fmt.Sprintf("%d", part))
	return f.open(fp)
}

func (f FS) Writer(bucket, name, id string) (io.WriteCloser, error) {
	fp := filepath.Join(string(f), bucket, name, id)
	return f.open(fp)
}

type Localhost int

func (l Localhost) String() string                                        { return fmt.Sprintf("http://localhost:%d", l) }
func (Localhost) Authorize(string, string) (string, error)                { return "ok", nil }
func (Localhost) CheckCreds(string, string) error                         { return nil }
func (l Localhost) APIRoot(string) string                                 { return l.String() }
func (l Localhost) DownloadRoot(string) string                            { return l.String() }
func (Localhost) PartSizes(string) (int32, int32)                         { return 1e5, 1 }
func (l Localhost) PartHost(fileId string) (string, error)                { return l.String(), nil }
func (Localhost) StartLargeFile(bucketId, fileId string, bs []byte) error { return nil }

type LocalBucket struct {
	Port int

	mux sync.Mutex
	b   map[string][]byte
}

func (lb *LocalBucket) Add(id string, bs []byte) error {
	lb.mux.Lock()
	defer lb.mux.Unlock()

	if lb.b == nil {
		lb.b = make(map[string][]byte)
	}

	lb.b[id] = bs
	return nil
}

func (lb *LocalBucket) Remove(id string) error {
	lb.mux.Lock()
	defer lb.mux.Unlock()

	if lb.b == nil {
		lb.b = make(map[string][]byte)
	}

	delete(lb.b, id)
	return nil
}

func (lb *LocalBucket) Update(id string, rev int, bs []byte) error {
	return lb.Add(id, bs)
}

func (lb *LocalBucket) List(acct string) ([][]byte, error) {
	lb.mux.Lock()
	defer lb.mux.Unlock()

	var bss [][]byte
	for _, bs := range lb.b {
		bss = append(bss, bs)
	}
	return bss, nil
}

func (lb *LocalBucket) Get(id string) ([]byte, error) {
	lb.mux.Lock()
	defer lb.mux.Unlock()

	bs, ok := lb.b[id]
	if !ok {
		return nil, errors.New("not found")
	}
	return bs, nil
}

func (lb LocalBucket) SimpleUploadHost(id string) (string, error) {
	return fmt.Sprintf("http://localhost:%d", lb.Port), nil
}
