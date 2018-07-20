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
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/google/uuid"
	"github.com/kurin/blazer/internal/bdb"
	"github.com/kurin/blazer/internal/pyre"
)

type LocalDiskManager struct {
	root string
	db   *bolt.DB
}

func New(rootDir string) (*LocalDiskManager, error) {
	if err := os.MkdirAll(rootDir, 0755); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(rootDir, "db.bolt"), 0600, nil)
	if err != nil {
		return nil, err
	}
	return &LocalDiskManager{
		db:   db,
		root: rootDir,
	}, nil
}

func (l *LocalDiskManager) APIRoot(acct string) string           { return "http://localhost:8822" }
func (l *LocalDiskManager) DownloadRoot(acct string) string      { return "http://localhost:8822" }
func (l *LocalDiskManager) Sizes(acct string) (int32, int32)     { return 1e8, 5e6 }
func (l *LocalDiskManager) UploadHost(id string) (string, error) { return "http://localhost:8822", nil }
func (l *LocalDiskManager) UploadPartHost(id string) (string, error) {
	return "http://localhost:8822", nil
}

type tokenInfo struct {
	Expire time.Time
}

func (l *LocalDiskManager) Authorize(acct, key string) (string, error) {
	/*if err := l.db.View(func(tx *bolt.Tx) error {
		acctKey, err := readBucketValue(tx, fmt.Sprintf("/accounts/%s", acct))
		if err != nil {
			return err
		}
		if string(acctKey) != key {
			return fmt.Errorf("auth error")
		}
		return nil
	}); err != nil {
		return "", err
	}*/
	token := uuid.New().String()
	bs, err := json.Marshal(tokenInfo{Expire: time.Now().Add(time.Hour * 24)})
	if err != nil {
		return "", err
	}
	tx := bdb.New(l.db)
	tx.Put(bs, "accounts", acct, "tokens", token)
	if err := tx.Run(); err != nil {
		return "", err
	}
	return token, nil
}

func (l *LocalDiskManager) CheckCreds(token, api string) error {
	// todo: this
	return nil
}

func (l *LocalDiskManager) AddBucket(acct, id, name string, bs []byte) error {
	tx := bdb.New(l.db)
	tx.Put([]byte(acct), "buckets", "by-id", id, "acct")
	tx.Put([]byte(name), "buckets", "by-id", id, "name")
	tx.Put([]byte(id), "buckets", "by-name", name, "id")
	tx.Put([]byte(name), "accounts", acct, "buckets", id, "name")
	tx.Put(bs, "accounts", acct, "buckets", id, "data")
	return tx.Run()
}

func (l *LocalDiskManager) GetBucket(id string) ([]byte, error) {
	tx := bdb.New(l.db)
	acct := tx.Read("buckets", id, "acct")
	data := tx.Read("accounts", acct, "buckets", id, "data")
	if err := tx.Run(); err != nil {
		return nil, err
	}
	return data.Bytes(), nil
}

func (l *LocalDiskManager) ListBuckets(acct string) ([][]byte, error) {
	var out [][]byte
	tx := bdb.New(l.db)
	tx.ForEach(func(k, v []byte) error {
		out = append(out, v)
		return nil
	}, "accounts", acct, "buckets")
	if err := tx.Run(); err != nil {
		if bdb.BucketNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return out, nil
}

func (l *LocalDiskManager) RemoveBucket(id string) error {
	tx := bdb.New(l.db)
	acct := tx.Read("buckets", "by-id", id, "acct")
	name := tx.Read("buckets", "by-id", id, "name")
	tx.Delete("buckets", "by-id", id)
	tx.Delete("buckets", "by-name", name)
	tx.Delete("accounts", acct, "buckets", id)
	return tx.Run()
}

func (l *LocalDiskManager) UpdateBucket(id string, rev int, bs []byte) error {
	tx := bdb.New(l.db)
	acct := tx.Read("buckets", "by-id", id, "acct")
	tx.Put(bs, "accounts", acct, "buckets", id, "data")
	return tx.Run()
}

type simpleWriter struct {
	io.WriteCloser
	bucket, name, id string
	db               *bolt.DB
}

func (s simpleWriter) Close() error {
	tx := bdb.New(s.db)
	acct := tx.Read("buckets", "by-id", s.bucket, "acct")
	bucketName := tx.Read("buckets", "by-id", s.bucket, "name")
	data := tx.Read("in-progress", s.id)
	tx.Delete("in-progress", s.id)
	tx.Mod(acct.Bytes, "files", "by-id", s.id, "acct")
	tx.Mod(data.Bytes, "accounts", acct, "buckets", s.bucket, "files", "by-id", s.id, "meta")
	tx.Put([]byte(s.id), "buckets", "by-name", bucketName, "live", s.name)
	tx.Inc("accounts", acct, "buckets", s.bucket, "files", "by-name", s.name, s.id)
	if err := tx.Run(); err != nil {
		return err
	}
	return s.WriteCloser.Close()
}

func (l *LocalDiskManager) Writer(bucket, name, id string, data []byte) (io.WriteCloser, error) {
	tx := bdb.New(l.db)
	tx.Put(data, "in-progress", id)
	if err := tx.Run(); err != nil {
		return nil, err
	}
	wc, err := os.Create(filepath.Join(l.root, id))
	if err != nil {
		return nil, err
	}
	return simpleWriter{
		WriteCloser: wc,
		bucket:      bucket,
		name:        name,
		id:          id,
		db:          l.db,
	}, nil
}

func (l *LocalDiskManager) Delete(id string) error { return nil }

func (l *LocalDiskManager) StartLarge(bucketID, name, id string, bs []byte) error {
	tx := bdb.New(l.db)
	tx.Put(bs, "in-progress-large", id, "meta")
	tx.Put([]byte(name), "in-progress-large", id, "name")
	tx.Put([]byte(bucketID), "in-progress-large", id, "bucket")
	return tx.Run()
}

func (l *LocalDiskManager) Parts(id string) ([]string, error) {
	m := map[string]string{}
	tx := bdb.New(l.db)
	tx.ForEach(func(k, v []byte) error {
		m[string(k)] = string(v)
		return nil
	}, "in-progress-large", id, "parts")
	if err := tx.Run(); err != nil {
		return nil, err
	}
	parts := make([]string, len(m))
	for num, sha := range m {
		n, err := strconv.ParseInt(num, 10, 64)
		if err != nil {
			return nil, err
		}
		parts[int(n)-1] = sha
	}
	return parts, nil
}

func (l *LocalDiskManager) FinishLarge(id string) error {
	tx := bdb.New(l.db)
	bucket := tx.Read("in-progress-large", id, "bucket")
	name := tx.Read("in-progress-large", id, "name")
	data := tx.Read("in-progress-large", id, "meta")
	acct := tx.Read("buckets", "by-id", bucket, "acct")
	bucketName := tx.Read("buckets", "by-id", bucket, "name")
	tx.Delete("in-progress-large", id)
	tx.Mod(acct.Bytes, "files", "by-id", id, "acct")
	tx.Mod(data.Bytes, "accounts", acct, "buckets", bucket, "files", "by-id", id, "meta")
	tx.Put([]byte(id), "buckets", "by-name", bucketName, "live", name)
	tx.Inc("accounts", acct, "buckets", bucket, "files", "by-name", name, id)
	/*tx.Atomic(func() error {
		return nil
	})*/
	return tx.Run()
}

func (l *LocalDiskManager) GetFile(id string) ([]byte, error) { return nil, nil }
func (l *LocalDiskManager) NextN(bucketID, name, pfx, spfx string, n int) ([]pyre.VersionedObject, error) {
	return nil, nil
}

type partObj struct {
	f    *os.File
	db   *bolt.DB
	id   string
	part int
	h    hash.Hash
}

func (p partObj) Write(b []byte) (int, error) {
	return io.MultiWriter(p.f, p.h).Write(b)
}

func (p partObj) Close() error {
	tx := bdb.New(p.db)
	tx.Put([]byte(fmt.Sprintf("%x", p.h.Sum(nil))), "in-progress-large", p.id, "parts", fmt.Sprintf("%d", p.part))
	if err := tx.Run(); err != nil {
		p.f.Close()
		return err
	}
	return p.f.Close()
}

func (l *LocalDiskManager) PartWriter(id string, part int) (io.WriteCloser, error) {
	if err := os.MkdirAll(filepath.Join(l.root, id), 0755); err != nil {
		return nil, err
	}
	path := filepath.Join(l.root, id, fmt.Sprintf("%d", part))
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	tx := bdb.New(l.db)
	tx.Put([]byte(path), "files", "by-id", id, "parts", fmt.Sprintf("%d"))
	if err := tx.Run(); err != nil {
		f.Close()
		return nil, err
	}
	return partObj{
		f:    f,
		db:   l.db,
		id:   id,
		part: part,
		h:    sha1.New(),
	}, nil
}

type obj struct {
	*os.File
	size int64
}

func (o obj) Size() int64 { return o.size }

func (l *LocalDiskManager) Download(bucket, name string) (pyre.DownloadableObject, error) {
	tx := bdb.New(l.db)
	live := tx.Read("buckets", "by-name", bucket, "live", name)
	if err := tx.Run(); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(l.root, live.String()))
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return obj{File: f, size: fi.Size()}, nil
}
