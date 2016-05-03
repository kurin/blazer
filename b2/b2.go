// Copyright 2016, Google
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
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"

	"golang.org/x/net/context"
)

// B2 is a Backblaze client.
type Client struct {
	backend beRootInterface
}

// AuthorizeAccount logs into the Backblaze B2 service.  It is necessary to
// call this on a new client.
func (c *Client) AuthorizeAccount(ctx context.Context, account, key string) error {
	if c.backend == nil {
		c.backend = &beRoot{
			b2i: &b2Root{},
		}
	}
	return c.backend.authorizeAccount(ctx, account, key)
}

// Bucket is a reference to a B2 bucket.
type Bucket struct {
	b beBucketInterface
}

// Bucket returns the named bucket, if it exists.
func (c *Client) Bucket(ctx context.Context, name string) (*Bucket, error) {
	buckets, err := c.backend.listBuckets(ctx)
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if bucket.name() == name {
			return &Bucket{
				b: bucket,
			}, nil
		}
	}
	b, err := c.backend.createBucket(ctx, name, "")
	if err != nil {
		return nil, err
	}
	return &Bucket{b}, err
}

func (b *Bucket) Delete(ctx context.Context) error {
	return b.b.deleteBucket(ctx)
}

// NewWriter returns a new writer for the given file.
func (b *Bucket) NewWriter(ctx context.Context, name string) *Writer {
	bw := &Writer{
		bucket: b.b,
		name:   name,
		Info:   make(map[string]string),
		chsh:   sha1.New(),
		cbuf:   &bytes.Buffer{},
		ctx:    ctx,
	}
	bw.w = io.MultiWriter(bw.chsh, bw.cbuf)
	return bw
}

func (b *Bucket) getFile(ctx context.Context, name string) (beFileInterface, error) {
	files, _, err := b.b.listFileNames(ctx, 1, name)
	if err != nil {
		return nil, err
	}
	if len(files) != 1 {
		return nil, errors.New("no files found")
	}
	if files[0].name() != name {
		return nil, fmt.Errorf("not found: %s", name)
	}
	return files[0], nil
}

// NewReader returns a reader for the given file.
func (b *Bucket) NewReader(ctx context.Context, name string) (*Reader, error) {
	file, err := b.getFile(ctx, name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	return &Reader{
		ctx:    ctx,
		cancel: cancel,
		bucket: b.b,
		name:   name,
		size:   file.size(),
		chunks: make(map[int]*bytes.Buffer),
	}, nil
}

func (b *Bucket) DeleteFile(ctx context.Context, name string) error {
	file, err := b.getFile(ctx, name)
	if err != nil {
		return err
	}
	return file.deleteFileVersion(ctx)
}
