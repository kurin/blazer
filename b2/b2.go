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

// Package b2 provides a high-level interface to Backblaze's B2 cloud storage
// service.
package b2

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"io"
	"time"

	"golang.org/x/net/context"
)

// Client is a Backblaze B2 client.
type Client struct {
	backend beRootInterface
}

// NewClient creates and returns a new Client with valid B2 service account
// tokens.
func NewClient(ctx context.Context, account, key string) (*Client, error) {
	c := &Client{
		backend: &beRoot{
			b2i: &b2Root{},
		},
	}
	if err := c.backend.authorizeAccount(ctx, account, key); err != nil {
		return nil, err
	}
	return c, nil
}

// Bucket is a reference to a B2 bucket.
type Bucket struct {
	b beBucketInterface
	r beRootInterface
}

// Bucket returns the named bucket.  If the bucket already exists (and belongs
// to this account), it is reused.  Otherwise a new bucket is created.
func (c *Client) Bucket(ctx context.Context, name string) (*Bucket, error) {
	buckets, err := c.backend.listBuckets(ctx)
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if bucket.name() == name {
			return &Bucket{
				b: bucket,
				r: c.backend,
			}, nil
		}
	}
	b, err := c.backend.createBucket(ctx, name, "")
	if err != nil {
		return nil, err
	}
	return &Bucket{
		b: b,
		r: c.backend,
	}, err
}

// Delete removes a bucket.  The bucket must be empty.
func (b *Bucket) Delete(ctx context.Context) error {
	return b.b.deleteBucket(ctx)
}

// Object represents a B2 object.
type Object struct {
	attrs *Attrs
	name  string
	f     beFileInterface
	b     *Bucket
}

// Attrs holds an object's metadata.
type Attrs struct {
	Name            string            // Not used on upload.
	Size            int64             // Not used on upload.
	ContentType     string            // Used on upload, default is "application/octet-stream".
	Status          ObjectState       // Not used on upload.
	UploadTimestamp time.Time         // Not used on upload.
	SHA1            string            // Not used on upload. Can be "none" for large files.
	Info            map[string]string // Limited to 10 keys.
}

// Attrs returns an object's attributes.
func (o *Object) Attrs(ctx context.Context) (*Attrs, error) {
	if err := o.ensure(ctx); err != nil {
		return nil, err
	}
	fi, err := o.f.getFileInfo(ctx)
	if err != nil {
		return nil, err
	}
	name, sha, size, ct, info, st, stamp := fi.stats()
	var state ObjectState
	switch st {
	case "upload":
		state = Uploaded
	case "start":
		state = Started
	case "hide":
		state = Hider
	}
	return &Attrs{
		Name:            name,
		Size:            size,
		ContentType:     ct,
		UploadTimestamp: stamp,
		SHA1:            sha,
		Info:            info,
		Status:          state,
	}, nil
}

// ObjectState represents the various states an object can be in.
type ObjectState int

const (
	Unknown ObjectState = iota
	// Started represents a large upload that has been started but not finished
	// or canceled.
	Started
	// Uploaded represents an object that has finished uploading and is complete.
	Uploaded
	// Hider represents an object that exists only to hide another object.  It
	// cannot in itself be downloaded and, in particular, is not a hidden object.
	Hider
)

// Object returns a reference to the named object in the bucket.  Hidden
// objects cannot be referenced in this manner; they can only be found by
// finding the appropriate reference in ListObjects.
func (b *Bucket) Object(name string) *Object {
	return &Object{
		name: name,
		b:    b,
	}
}

// NewWriter returns a new writer for the given object.  Objects that are
// overwritten are not deleted, but are "hidden".
func (o *Object) NewWriter(ctx context.Context) *Writer {
	ctx, cancel := context.WithCancel(ctx)
	bw := &Writer{
		o:      o,
		name:   o.name,
		chsh:   sha1.New(),
		cbuf:   &bytes.Buffer{},
		ctx:    ctx,
		cancel: cancel,
	}
	bw.w = io.MultiWriter(bw.chsh, bw.cbuf)
	return bw
}

// NewReader returns a reader for the given object.
func (o *Object) NewReader(ctx context.Context) *Reader {
	ctx, cancel := context.WithCancel(ctx)
	return &Reader{
		ctx:    ctx,
		cancel: cancel,
		o:      o,
		name:   o.name,
		chunks: make(map[int]*bytes.Buffer),
	}
}

func (o *Object) ensure(ctx context.Context) error {
	if o.f == nil {
		f, err := o.b.getObject(ctx, o.name)
		if err != nil {
			return err
		}
		o.f = f.f
	}
	return nil
}

// Delete removes the given object.
func (o *Object) Delete(ctx context.Context) error {
	if err := o.ensure(ctx); err != nil {
		return err
	}
	return o.f.deleteFileVersion(ctx)
}

// Cursor is passed to ListObjects to return subsequent pages.
type Cursor struct {
	name string
	id   string
}

// ListObjects returns all objects in the bucket, including multiple versions
// of the same object.  Cursor may be nil; when passed to a subsequent query,
// it will continue the listing.
//
// ListObjects will return io.EOF when there are no objects left in the bucket,
// however it may do so concurrently with the last objects.
func (b *Bucket) ListObjects(ctx context.Context, count int, c *Cursor) ([]*Object, *Cursor, error) {
	if c == nil {
		c = &Cursor{}
	}
	fs, name, id, err := b.b.listFileVersions(ctx, count, c.name, c.id)
	if err != nil {
		return nil, nil, err
	}
	var next *Cursor
	if name != "" && id != "" {
		next = &Cursor{
			name: name,
			id:   id,
		}
	}
	var objects []*Object
	for _, f := range fs {
		objects = append(objects, &Object{
			name: f.name(),
			f:    f,
			b:    b,
		})
	}
	var rtnErr error
	if len(objects) == 0 || next == nil {
		rtnErr = io.EOF
	}
	return objects, next, rtnErr
}

// ListCurrentObjects is similar to ListObjects, except that it returns only
// current, unhidden objects in the bucket.
func (b *Bucket) ListCurrentObjects(ctx context.Context, count int, c *Cursor) ([]*Object, *Cursor, error) {
	if c == nil {
		c = &Cursor{}
	}
	fs, name, err := b.b.listFileNames(ctx, count, c.name)
	if err != nil {
		return nil, nil, err
	}
	var next *Cursor
	if name != "" {
		next = &Cursor{
			name: name,
		}
	}
	var objects []*Object
	for _, f := range fs {
		objects = append(objects, &Object{
			name: f.name(),
			f:    f,
			b:    b,
		})
	}
	var rtnErr error
	if len(objects) == 0 || next == nil {
		rtnErr = io.EOF
	}
	return objects, next, rtnErr
}

// Hide hides the object from name-based listing.
func (o *Object) Hide(ctx context.Context) error {
	if err := o.ensure(ctx); err != nil {
		return err
	}
	_, err := o.b.b.hideFile(ctx, o.name)
	return err
}

// Reveal unhides (if hidden) the named object.  If there are multiple objects
// of a given name, it will reveal the most recent.
func (b *Bucket) Reveal(ctx context.Context, name string) error {
	cur := &Cursor{
		name: name,
	}
	objs, _, err := b.ListObjects(ctx, 1, cur)
	if err != nil && err != io.EOF {
		return err
	}
	if len(objs) < 1 || objs[0].name != name {
		return fmt.Errorf("%s: not found", name)
	}
	obj := objs[0]
	if obj.f.status() != "hide" {
		return nil
	}
	return obj.Delete(ctx)
}

func (b *Bucket) getObject(ctx context.Context, name string) (*Object, error) {
	fs, _, err := b.b.listFileNames(ctx, 1, name)
	if err != nil {
		return nil, err
	}
	if len(fs) < 1 {
		return nil, fmt.Errorf("%s: not found", name)
	}
	f := fs[0]
	if f.name() != name {
		return nil, fmt.Errorf("%s: not found", name)
	}
	return &Object{
		name: name,
		f:    f,
		b:    b,
	}, nil
}
