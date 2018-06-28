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
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type FS string

func (f FS) PartWriter(id string, part int) (io.WriteCloser, error) {
	fp := filepath.Join(string(f), id, fmt.Sprintf("%d", part))
	if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
		return nil, err
	}
	return os.Create(fp)
}

func (f FS) Writer(bucket, name, id string) (io.WriteCloser, error) {
	fp := filepath.Join(string(f), bucket, name, id)
	if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
		return nil, err
	}
	return os.Create(fp)
}
