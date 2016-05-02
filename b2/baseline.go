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
	"io"
	"time"

	"github.com/kurin/gozer/base"
	"golang.org/x/net/context"
)

// This file wraps the base package in a thin layer, for testing.  It should be
// the only file in b2 that imports base.

type b2RootInterface interface {
	authorizeAccount(context.Context, string, string) error
	backoff(error) (time.Duration, bool)
	reauth(error) bool
	createBucket(context.Context, string, string) (b2BucketInterface, error)
	listBuckets(context.Context) ([]b2BucketInterface, error)
}

type b2BucketInterface interface {
	deleteBucket(context.Context) error
	getUploadURL(context.Context) (b2URLInterface, error)
	name() string
}

type b2URLInterface interface {
	uploadFile(context.Context, io.Reader, int, string, string, string, map[string]string) (b2FileInterface, error)
}

type b2FileInterface interface{}

type b2Root struct {
	b *base.B2
}

type b2Bucket struct {
	b *base.Bucket
}

type b2URL struct {
	b *base.URL
}

type b2File struct {
	b *base.File
}

func (r b2Root) authorizeAccount(ctx context.Context, account, key string) error {
	b, err := base.AuthorizeAccount(ctx, account, key)
	if err != nil {
		return err
	}
	if r.b == nil {
		r.b = b
		return nil
	}
	r.b.Update(b)
	return nil
}

func (r b2Root) backoff(err error) (time.Duration, bool) {
	if base.Action(err) != base.Retry {
		return 0, false
	}
	return base.Backoff(err)
}

func (r b2Root) reauth(err error) bool {
	return base.Action(err) == base.ReAuthenticate
}

func (b b2Root) createBucket(ctx context.Context, name, btype string) (b2BucketInterface, error) {
	bucket, err := b.b.CreateBucket(ctx, name, btype)
	if err != nil {
		return nil, err
	}
	return b2Bucket{bucket}, nil
}

func (b b2Root) listBuckets(ctx context.Context) ([]b2BucketInterface, error) {
	buckets, err := b.b.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	var rtn []b2BucketInterface
	for _, bucket := range buckets {
		rtn = append(rtn, b2Bucket{bucket})
	}
	return rtn, err
}

func (b b2Bucket) deleteBucket(ctx context.Context) error {
	return b.b.DeleteBucket(ctx)
}

func (b b2Bucket) name() string {
	return b.b.Name
}

func (b b2Bucket) getUploadURL(ctx context.Context) (b2URLInterface, error) {
	url, err := b.b.GetUploadURL(ctx)
	if err != nil {
		return nil, err
	}
	return b2URL{url}, nil
}

func (b b2URL) uploadFile(ctx context.Context, r io.Reader, size int, name, contentType, sha1 string, info map[string]string) (b2FileInterface, error) {
	file, err := b.b.UploadFile(ctx, r, size, name, contentType, sha1, info)
	if err != nil {
		return nil, err
	}
	return b2File{file}, nil
}
