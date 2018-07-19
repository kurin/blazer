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

func (v *Value) Bytes() []byte  { return v.bs }
func (v *Value) String() string { return string(v.bs) }

func fpath(p []interface{}) ([]string, error) {
	var out []string
	for _, part := range p {
		switch pt := part.(type) {
		case string:
			out = append(out, pt)
		case *Value:
			out = append(out, pt.String())
		default:
			return nil, errors.New("paths should be string or Value arguments")
		}
	}
	return out, nil
}

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

func (b *Tx) Read(path ...interface{}) *Value {
	val := &Value{}
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		parts, err := fpath(path)
		if err != nil {
			return err
		}
		bt, err := b.bucket(tx, parts[:len(parts)-1])
		if err != nil {
			return err
		}
		val.bs = bt.Get([]byte(parts[len(parts)-1]))
		return nil
	})
	return val
}

func (b *Tx) Put(val []byte, path ...interface{}) {
	b.Mod(func() []byte { return val }, path...)
}

// Mod is like Put, but it allows the caller to pass a Value.Bytes.
func (b *Tx) Mod(val func() []byte, path ...interface{}) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		parts, err := fpath(path)
		if err != nil {
			return err
		}
		bt, err := b.mkBucket(tx, parts[:len(parts)-1])
		if err != nil {
			return err
		}
		return bt.Put([]byte(parts[len(parts)-1]), val())
	})
}

func (b *Tx) Delete(path ...interface{}) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		parts, err := fpath(path)
		if err != nil {
			return err
		}
		bucket, err := b.bucket(tx, parts[:len(parts)-1])
		if err != nil {
			return err
		}
		return bucket.Delete([]byte(parts[len(parts)-1]))
	})
}

func (b *Tx) ForEach(f func(k, v []byte) error, path ...interface{}) {
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		parts, err := fpath(path)
		if err != nil {
			return err
		}
		bucket, err := b.bucket(tx, parts)
		if err != nil {
			return err
		}
		return bucket.ForEach(f)
	})
}

func (b *Tx) Inc(path ...interface{}) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		parts, err := fpath(path)
		if err != nil {
			return err
		}
		bucket, err := b.mkBucket(tx, parts[:len(parts)-1])
		if err != nil {
			return err
		}
		n, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		return bucket.Put([]byte(parts[len(parts)-1]), []byte(fmt.Sprintf("%d", n)))
	})
}
