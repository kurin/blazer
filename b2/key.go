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

type Key struct {
	c *Client
	k beKeyInterface
}

func (k *Key) Delete(ctx context.Context) error { return k.k.del(ctx) }

type keyOptions struct {
	caps     []string
	prefix   string
	lifetime time.Duration
}

type KeyOption func(*keyOptions)

func Lifetime(d time.Duration) KeyOption {
	return func(k *keyOptions) {
		k.lifetime = d
	}
}

func Capability(cap string) KeyOption {
	return func(k *keyOptions) {
		k.caps = append(k.caps, cap)
	}
}

func Prefix(prefix string) KeyOption {
	return func(k *keyOptions) {
		k.prefix = prefix
	}
}

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
