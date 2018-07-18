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
	"os"
	"path/filepath"

	bolt "github.com/coreos/bbolt"
)

type LocalDiskManager struct {
	root string
	db   *bolt.DB
}

func New(rootDir string) (*LocalDiskManager, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(rootDir, "db.bolt"), 0, nil)
	if err != nil {
		return nil, err
	}
	return &LocalDiskManager{
		db:   db,
		root: rootDir,
	}, nil
}

//func (l *LocalDiskManager)
