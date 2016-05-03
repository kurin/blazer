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

	"github.com/kurin/blazer/base"

	"golang.org/x/net/context"
)

// This file wraps the base package in a thin layer, for testing.  It should be
// the only file in b2 that imports base.

type b2RootInterface interface {
	authorizeAccount(context.Context, string, string) error
	transient(error) bool
	backoff(error) (time.Duration, bool)
	reauth(error) bool
	createBucket(context.Context, string, string) (b2BucketInterface, error)
	listBuckets(context.Context) ([]b2BucketInterface, error)
}

type b2BucketInterface interface {
	name() string
	deleteBucket(context.Context) error
	getUploadURL(context.Context) (b2URLInterface, error)
	startLargeFile(ctx context.Context, name, contentType string, info map[string]string) (b2LargeFileInterface, error)
}

type b2URLInterface interface {
	reload(context.Context) error
	uploadFile(context.Context, io.Reader, int, string, string, string, map[string]string) (b2FileInterface, error)
}

type b2FileInterface interface {
	deleteFileVersion(context.Context) error
}

type b2LargeFileInterface interface {
	finishLargeFile(context.Context) (b2FileInterface, error)
	getUploadPartURL(context.Context) (b2FileChunkInterface, error)
}

type b2FileChunkInterface interface {
	reload(context.Context) error
	uploadPart(context.Context, io.Reader, string, int, int) (int, error)
}

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

type b2LargeFile struct {
	b *base.LargeFile
}

type b2FileChunk struct {
	b *base.FileChunk
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

func (r b2Root) transient(err error) bool {
	return base.Action(err) != base.Punt
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

func (b b2Bucket) startLargeFile(ctx context.Context, name, ct string, info map[string]string) (b2LargeFileInterface, error) {
	lf, err := b.b.StartLargeFile(ctx, name, ct, info)
	if err != nil {
		return nil, err
	}
	return b2LargeFile{lf}, nil
}

func (b b2URL) uploadFile(ctx context.Context, r io.Reader, size int, name, contentType, sha1 string, info map[string]string) (b2FileInterface, error) {
	file, err := b.b.UploadFile(ctx, r, size, name, contentType, sha1, info)
	if err != nil {
		return nil, err
	}
	return b2File{file}, nil
}

func (b b2URL) reload(ctx context.Context) error {
	return b.b.Reload(ctx)
}

func (b b2File) deleteFileVersion(ctx context.Context) error {
	return b.b.DeleteFileVersion(ctx)
}

func (b b2LargeFile) finishLargeFile(ctx context.Context) (b2FileInterface, error) {
	f, err := b.b.FinishLargeFile(ctx)
	if err != nil {
		return nil, err
	}
	return b2File{f}, nil
}

func (b b2LargeFile) getUploadPartURL(ctx context.Context) (b2FileChunkInterface, error) {
	c, err := b.b.GetUploadPartURL(ctx)
	if err != nil {
		return nil, err
	}
	return b2FileChunk{c}, nil
}

func (b b2FileChunk) reload(ctx context.Context) error {
	return b.b.Reload(ctx)
}

func (b b2FileChunk) uploadPart(ctx context.Context, r io.Reader, sha1 string, size, index int) (int, error) {
	return b.b.UploadPart(ctx, r, sha1, size, index)
}
