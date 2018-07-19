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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/google/uuid"
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
	db, err := bolt.Open(filepath.Join(rootDir, "db.bolt"), 0, nil)
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
	if err := l.db.View(func(tx *bolt.Tx) error {
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
	}
	token := uuid.New().String()
	if err := l.db.Update(func(tx *bolt.Tx) error {
		bs, err := json.Marshal(tokenInfo{Expire: time.Now().Add(time.Hour * 24)})
		if err != nil {
			return err
		}
		return writeBucketValue(tx, fmt.Sprintf("/accounts/%s/tokens/%s", acct, token), bs)
	}); err != nil {
		return "", err
	}
	return token, nil
}

func (l *LocalDiskManager) CheckCreds(token, api string) error {
	// todo: this
	return nil
}

func (l *LocalDiskManager) AddBucket(acct, id, name string, bs []byte) error {
	return l.db.Update(func(tx *bolt.Tx) error {
		// TODO: fail on double create
		if err := writeBucketValue(tx, fmt.Sprintf("/buckets/id/%s", id), []byte(acct)); err != nil {
			return err
		}
		if err := writeBucketValue(tx, fmt.Sprintf("/buckets/name/%s", name), []byte(acct)); err != nil {
			return err
		}
		if err := writeBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/name/%s", acct, name), []byte(id)); err != nil {
			return err
		}
		if err := writeBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/id/%s", acct, id), []byte(name)); err != nil {
			return err
		}
		return writeBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/data/%s", acct, id), bs)
	})
}

func (l *LocalDiskManager) GetBucket(id string) ([]byte, error) {
	var bs []byte
	if err := l.db.View(func(tx *bolt.Tx) error {
		acct, err := readBucketValue(tx, fmt.Sprintf("/buckets/%s", id))
		if err != nil {
			return err
		}
		data, err := readBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/data/%s", string(acct), id))
		if err != nil {
			return err
		}
		bs = data
		return nil
	}); err != nil {
		return nil, err
	}
	return bs, nil
}

func (l *LocalDiskManager) ListBuckets(acct string) ([][]byte, error) {
	var out [][]byte
	if err := l.db.View(func(tx *bolt.Tx) error {
		return forEach(tx, fmt.Sprintf("/accounts/%s/buckets/name", acct), func(k, v []byte) error {
			id := string(v)
			data, err := readBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/data/%s", acct, id))
			if err != nil {
				return err
			}
			out = append(out, data)
			return nil
		})
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (l *LocalDiskManager) RemoveBucket(id string) error {
	return l.db.Update(func(tx *bolt.Tx) error {
		acctb, err := readBucketValue(tx, fmt.Sprintf("/buckets/id/%s", id))
		if err != nil {
			return err
		}
		acct := string(acctb)
		nameb, err := readBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/id/%s", acct, id))
		if err != nil {
			return err
		}
		name := string(nameb)
		if err := deleteBucketValue(tx, fmt.Sprintf("/buckets/id/%s", id)); err != nil {
			return err
		}
		if err := deleteBucketValue(tx, fmt.Sprintf("/buckets/name/%s", name)); err != nil {
			return err
		}
		if err := deleteBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/name/%s", acct, name)); err != nil {
			return err
		}
		if err := deleteBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/id/%s", acct, id)); err != nil {
			return err
		}
		return deleteBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/data/%s", acct, id))
	})
}

func (l *LocalDiskManager) UpdateBucket(id string, rev int, bs []byte) error {
	return l.db.Update(func(tx *bolt.Tx) error {
		// todo: enforce rev
		acct, err := readBucketValue(tx, fmt.Sprintf("/buckets/id/%s", id))
		if err != nil {
			return err
		}
		return writeBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/data/%s", string(acct), id), bs)
	})
}

func (l *LocalDiskManager) GetBucketID(name string) (string, error) {
	var id string
	if err := l.db.View(func(tx *bolt.Tx) error {
		acctb, err := readBucketValue(tx, fmt.Sprintf("/buckets/name/%s", name))
		if err != nil {
			return err
		}
		idb, err := readBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/name/%s", string(acctb), name))
		if err != nil {
			return err
		}
		id = string(idb)
		return nil
	}); err != nil {
		return "", err
	}
	return id, nil
}

type simpleWriter struct {
	io.WriteCloser
	bucket, name, id string
	db               *bolt.DB
}

func (s simpleWriter) Close() error {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		data, err := readBucketValue(tx, fmt.Sprintf("/in-progress/%d", s.id))
		if err != nil {
			return err
		}
		acctb, err := readBucketValue(tx, fmt.Sprintf("/buckets/name/%s", s.bucket))
		if err != nil {
			return err
		}
		acct := string(acctb)
		bucketb, err := readBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/name/%s", acct, s.bucket))
		if err != nil {
			return err
		}
		bucketID := string(bucketb)
		// Look up file by ID.
		if err := writeBucketValue(tx, fmt.Sprintf("/files/id/%d", s.id), acctb); err != nil {
			return err
		}
		// Look up bucket by file ID.
		if err := writeBucketValue(tx, fmt.Sprintf("/accounts/%s/files/id/%s", acct, s.id), []byte(s.bucket)); err != nil {
			return err
		}
		// Look up file data by file ID.
		if err := writeBucketValue(tx, fmt.Sprintf("/accounts/%s/buckets/%s/files/id/%s", acct, bucketID, s.id), data); err != nil {
			return err
		}
		// Look up file ID by bucket and file name.
		if err := writeSequence(tx, fmt.Sprintf("/accounts/%s/buckets/%s/files/name/%s/%s", acct, bucketID, s.name, s.id)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		s.WriteCloser.Close()
		return err
	}
	return s.WriteCloser.Close()
}

func (l *LocalDiskManager) Writer(bucket, name, id string, data []byte) (io.WriteCloser, error) {
	if err := l.db.Update(func(tx *bolt.Tx) error {
		return writeBucketValue(tx, fmt.Sprintf("/in-progress/%s", id), data)
	}); err != nil {
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

func (l *LocalDiskManager) Delete(id string) error                                { return nil }
func (l *LocalDiskManager) StartLarge(bucketID, name, id string, bs []byte) error { return nil }
func (l *LocalDiskManager) Parts(id string) ([]string, error)                     { return nil, nil }
func (l *LocalDiskManager) FinishLarge(id string) error                           { return nil }
func (l *LocalDiskManager) GetFile(id string) ([]byte, error)                     { return nil, nil }
func (l *LocalDiskManager) NextN(bucketID, name, pfx, spfx string, n int) ([]pyre.VersionedObject, error) {
	return nil, nil
}
func (l *LocalDiskManager) PartWriter(id string, part int) (io.WriteCloser, error) { return nil, nil }
func (l *LocalDiskManager) ObjectByName(bucketID, name string) (pyre.DownloadableObject, error) {
	return nil, nil
}

func getBucket(tx *bolt.Tx, path string) (*bolt.Bucket, error) {
	for strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
	}
	parts := strings.Split(path, "/")
	var b *bolt.Bucket
	for _, part := range parts[:len(parts)-1] {
		if b == nil {
			b = tx.Bucket([]byte(part))
		} else {
			b = b.Bucket([]byte(part))
		}
		if b == nil {
			return nil, fmt.Errorf("no such value")
		}
	}
	return b, nil
}

func readBucketValue(tx *bolt.Tx, key string) ([]byte, error) {
	bucket, err := getBucket(tx, path.Dir(key))
	if err != nil {
		return nil, err
	}
	val := bucket.Get([]byte(path.Base(key)))
	if val == nil {
		return nil, fmt.Errorf("no such value")
	}
	return val, nil
}

func deleteBucketValue(tx *bolt.Tx, key string) error {
	bucket, err := getBucket(tx, path.Dir(key))
	if err != nil {
		return err
	}
	return bucket.Delete([]byte(path.Base(key)))
}

func writeBucketValue(tx *bolt.Tx, path string, val []byte) error {
	for strings.HasPrefix(path, "/") {
		path = strings.TrimPrefix(path, "/")
	}
	parts := strings.Split(path, "/")
	var b *bolt.Bucket
	for _, part := range parts[:len(parts)-1] {
		var err error
		if b == nil {
			b, err = tx.CreateBucketIfNotExists([]byte(part))
		} else {
			b, err = b.CreateBucketIfNotExists([]byte(part))
		}
		if err != nil {
			return err
		}
	}
	return b.Put([]byte(parts[len(parts)-1]), []byte(val))
}

func forEach(tx *bolt.Tx, path string, f func(k, v []byte) error) error {
	bucket, err := getBucket(tx, path)
	if err != nil {
		return err
	}
	return bucket.ForEach(f)
}

func writeSequence(tx *bolt.Tx, key string) error {
	bucket, err := getBucket(tx, path.Dir(key))
	if err != nil {
		return err
	}
	n, err := bucket.NextSequence()
	if err != nil {
		return err
	}
	return bucket.Put([]byte(path.Base(key)), []byte(fmt.Sprintf("%d", n)))
}
