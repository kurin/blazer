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

// Package bdb wraps the bolt DB to make it easier to work with in the no-doubt
// broken way that I use it.
package bdb

import (
	"errors"
	"fmt"

	bolt "github.com/coreos/bbolt"
)

// Value represents a read from a bolt key.  It is not valid until after Run has
// been called.
type Value struct {
	bs []byte
}

func (v *Value) Bytes() []byte { return v.bs }

// New returns a new Tx.
func New(db *bolt.DB) *Tx { return &Tx{db: db} }

type Tx struct {
	db     *bolt.DB
	ops    []func(*bolt.Tx) error
	mutate bool
}

// Run runs all the operations on the Tx in the order in which they were given,
// stopping at and returning the first error, if any.  Run is atomic: if any
// error is returned, then the transaction is rolled back.
func (b *Tx) Run() error {
	runner := b.db.View
	if b.mutate {
		runner = b.db.Update
	}
	return runner(func(tx *bolt.Tx) error {
		for _, op := range b.ops {
			if err := op(tx); err != nil {
				return err
			}
		}
		return nil
	})
}

// BucketNotExist is returned for bucket non-existence.
var BucketNotExist = errors.New("no such bucket")

func (b *Tx) bucket(tx *bolt.Tx, parts []string) (*bolt.Bucket, error) {
	var bt *bolt.Bucket
	for i, p := range parts {
		if i == 0 {
			bt = tx.Bucket([]byte(p))
		} else {
			bt = bt.Bucket([]byte(p))
		}
		if bt == nil {
			return nil, BucketNotExist
		}
	}
	if bt == nil {
		return nil, BucketNotExist
	}
	return bt, nil
}

func (b *Tx) mkBucket(tx *bolt.Tx, parts []string) (*bolt.Bucket, error) {
	var bt *bolt.Bucket
	for i, p := range parts {
		var err error
		if i == 0 {
			bt, err = tx.CreateBucketIfNotExists([]byte(p))
		} else {
			bt, err = bt.CreateBucketIfNotExists([]byte(p))
		}
		if err != nil {
			return nil, err
		}
	}
	return bt, nil
}

func (b *Tx) Read(path ...string) *Value {
	val := &Value{}
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		bt, err := b.bucket(tx, path[:len(path)-1])
		if err != nil {
			return err
		}
		val.bs = bt.Get([]byte(path[len(path)-1]))
		return nil
	})
	return val
}

func (b *Tx) Put(val []byte, path ...string) {
	b.Mod(func() []byte { return val }, path...)
}

func (b *Tx) Mod(val func() []byte, path ...string) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		bt, err := b.mkBucket(tx, path[:len(path)-1])
		if err != nil {
			return err
		}
		return bt.Put([]byte(path[len(path)-1]), val())
	})
}

func (b *Tx) Delete(path ...string) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		bucket, err := b.bucket(tx, path[:len(path)-1])
		if err != nil {
			return err
		}
		return bucket.Delete([]byte(path[len(path)-1]))
	})
}

func (b *Tx) ForEach(f func(k, v []byte) error, path ...string) {
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		bucket, err := b.bucket(tx, path)
		if err != nil {
			return err
		}
		return bucket.ForEach(f)
	})
}

func (b *Tx) Inc(path ...string) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		bucket, err := b.mkBucket(tx, path[:len(path)-1])
		if err != nil {
			return err
		}
		n, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(path[len(path)-1]), []byte(fmt.Sprintf("%d", n)))
	})
}
