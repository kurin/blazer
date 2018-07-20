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
	"strings"

	bolt "github.com/coreos/bbolt"
)

type Path struct {
	spec       Spec
	bucketPath [][]byte
	key        []byte
	args       []fmt.Stringer
}

// Spec represents a path to a bucket key.  It should be a /-delemited string,
// beginning with /, that denotes the bucket in which to save or from which to
// retrieve a value.  The path must begin with /.  The final element in the
// path is the key; intermediate elements are buckets.
//
// If path elements begin with %, then they are not literally given, but are
// set at runtime with Bind.  This allows callers to use Values as path
// elements.
type Spec string

// Bind assigns the given arguments to a PathSpec and returns a Path.
func (s Spec) Bind(args ...fmt.Stringer) *Path {
	return &Path{
		spec: s,
		args: args,
	}
}

func (p *Path) parse() error {
	spec := string(p.spec)
	if !strings.HasPrefix(spec, "/") {
		fmt.Errorf("%q: malformed path", spec)
	}
	parts := strings.Split(spec, "/")
	var arg int
	for _, part := range parts[1 : len(parts)-1] {
		if part == "" {
			return fmt.Errorf("%q: malformed path", spec)
		}
		if strings.HasPrefix(part, "%") {
			if arg >= len(p.args) {
				return fmt.Errorf("%q: not enough arguments bound to spec", spec)
			}
			bound := p.args[arg].String()
			if bound == "" {
				return fmt.Errorf("%q: error binding %q: empty argument", spec, part)
			}
			part = bound
			arg++
		}
		if part == "" {
			return fmt.Errorf("%q: malformed path", spec)
		}
		p.bucketPath = append(p.bucketPath, []byte(part))
	}
	last := parts[len(parts)-1]
	if strings.HasPrefix(last, "%") {
		if arg >= len(p.args) {
			return fmt.Errorf("%q: not enough arguments bound to spec", spec)
		}
		bound := p.args[arg].String()
		if bound == "" {
			return fmt.Errorf("%q: error binding %q: empty argument", spec, last)
		}
		last = bound
		return nil
	}
	if last == "" {
		return fmt.Errorf("%q: malformed path", spec)
	}
	p.key = []byte(last)
	return nil
}

// Value represents a read from a bolt key.  It is not valid until after Run has
// been called.
type Value struct {
	bs    []byte
	valid bool
}

func (v *Value) set(b []byte) {
	v.bs = b
	v.valid = true
}

func (v *Value) Bytes() []byte {
	if !v.valid {
		panic("not valid")
	}
	return v.bs
}

func (v *Value) String() string {
	if !v.valid {
		panic("not valid")
	}
	return string(v.bs)
}

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

type bucketErr struct {
	msg   string
	exist bool
}

func (b bucketErr) Error() string { return b.msg }

// BucketNotExist is returned for bucket non-existence.
func BucketNotExist(err error) bool {
	berr, ok := err.(bucketErr)
	if !ok {
		return false
	}
	return berr.exist
}

func (b *Tx) bucket(tx *bolt.Tx, parts [][]byte) (*bolt.Bucket, error) {
	var bt *bolt.Bucket
	for i, p := range parts {
		if i == 0 {
			bt = tx.Bucket(p)
		} else {
			bt = bt.Bucket(p)
		}
		if bt == nil {
			return nil, bucketErr{msg: fmt.Sprintf("%s: no such bucket", string(p)), exist: true}
		}
	}
	if bt == nil {
		return nil, bucketErr{msg: fmt.Sprintf("(unnamed): no such bucket"), exist: true}
	}
	return bt, nil
}

func (b *Tx) mkBucket(tx *bolt.Tx, parts [][]byte) (*bolt.Bucket, error) {
	var bt *bolt.Bucket
	for i, p := range parts {
		var err error
		if i == 0 {
			bt, err = tx.CreateBucketIfNotExists(p)
		} else {
			bt, err = bt.CreateBucketIfNotExists(p)
		}
		if err != nil {
			return nil, err
		}
	}
	return bt, nil
}

func (b *Tx) Read(p *Path) *Value {
	val := &Value{}
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		if err := p.parse(); err != nil {
			return err
		}
		bt, err := b.bucket(tx, p.bucketPath)
		if err != nil {
			return err
		}
		val.set(bt.Get(p.key))
		return nil
	})
	return val
}

func (b *Tx) Put(p *Path, val []byte) {
	b.Mod(p, &Value{bs: val, valid: true})
}

// Mod is like Put, but it allows the caller to pass a Value.Bytes.
func (b *Tx) Mod(p *Path, v *Value) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		if err := p.parse(); err != nil {
			return err
		}
		bt, err := b.mkBucket(tx, p.bucketPath)
		if err != nil {
			return err
		}
		return bt.Put(p.key, v.Bytes())
	})
}

func (b *Tx) Delete(p *Path) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		if err := p.parse(); err != nil {
			return err
		}
		bucket, err := b.bucket(tx, p.bucketPath)
		if err != nil {
			return err
		}
		if bucket.Bucket(p.key) == nil {
			return bucket.Delete(p.key)
		}
		return bucket.DeleteBucket(p.key)
	})
}

func (b *Tx) ForEach(p *Path, f func(k, v []byte) error) {
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		if err := p.parse(); err != nil {
			return err
		}
		bucket, err := b.bucket(tx, append(p.bucketPath, p.key))
		if err != nil {
			return err
		}
		return bucket.ForEach(f)
	})
}

func (b *Tx) Inc(p *Path) {
	b.mutate = true
	b.ops = append(b.ops, func(tx *bolt.Tx) error {
		if err := p.parse(); err != nil {
			return err
		}
		bucket, err := b.mkBucket(tx, p.bucketPath)
		if err != nil {
			return err
		}
		n, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		return bucket.Put(p.key, []byte(fmt.Sprintf("%d", n)))
	})
}

func (b *Tx) Atomic(fn func() error) {
	b.ops = append(b.ops, func(*bolt.Tx) error { return fn() })
}
