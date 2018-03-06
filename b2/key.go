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

package b2

import (
	"context"
	"errors"
	"time"
)

// Key is a B2 application key.  A Key grants limited access on a global or
// per-bucket basis.
type Key struct {
	c *Client
	k beKeyInterface
}

// Delete removes the key from B2.
func (k *Key) Delete(ctx context.Context) error { return k.k.del(ctx) }

type keyOptions struct {
	caps     []string
	prefix   string
	lifetime time.Duration
}

// KeyOption specifies desired properties for application keys.
type KeyOption func(*keyOptions)

// Lifetime requests a key with the given lifetime.
func Lifetime(d time.Duration) KeyOption {
	return func(k *keyOptions) {
		k.lifetime = d
	}
}

// Deadline requests a key that expires after the given date.
func Deadline(t time.Time) KeyOption {
	d := t.Sub(time.Now())
	return Lifetime(d)
}

// Capability requests a key with the given capability.
func Capability(cap string) KeyOption {
	return func(k *keyOptions) {
		k.caps = append(k.caps, cap)
	}
}

// Prefix limits the requested application key to be valid only for objects
// that begin with prefix.  This can only be used when requesting an
// application key within a specific bucket.
func Prefix(prefix string) KeyOption {
	return func(k *keyOptions) {
		k.prefix = prefix
	}
}

// CreateKey creates a global application key that is valid for all buckets
// in this project.
func (c *Client) CreateKey(ctx context.Context, name string, opts ...KeyOption) (*Key, error) {
	var ko keyOptions
	for _, o := range opts {
		o(&ko)
	}
	if ko.prefix != "" {
		return nil, errors.New("Prefix is not a valid option for global application keys")
	}
	ki, err := c.backend.createKey(ctx, name, ko.caps, ko.lifetime, "", "")
	if err != nil {
		return nil, err
	}
	return &Key{
		c: c,
		k: ki,
	}, nil
}

// CreateKey creates a scoped application key that is valid only for this bucket.
func (b *Bucket) CreateKey(ctx context.Context, name string, opts ...KeyOption) (*Key, error) {
	var ko keyOptions
	for _, o := range opts {
		o(&ko)
	}
	ki, err := b.r.createKey(ctx, name, ko.caps, ko.lifetime, b.b.id(), ko.prefix)
	if err != nil {
		return nil, err
	}
	return &Key{
		c: b.c,
		k: ki,
	}, nil
}
