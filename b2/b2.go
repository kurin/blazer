package b2

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"

	"golang.org/x/net/context"

	"github.com/kurin/gozer/base"
)

// B2 is a Backblaze client.
type Client struct {
	b2 *base.B2
}

// NewClient returns a new Backblaze B2 client.
func NewClient(ctx context.Context, account, key string) (*Client, error) {
	b2, err := base.B2AuthorizeAccount(ctx, account, key)
	if err != nil {
		return nil, err
	}
	return &Client{
		b2: b2,
	}, nil
}

// Bucket is a reference to a B2 bucket.
type Bucket struct {
	b *base.Bucket
}

// Bucket returns the named bucket, if it exists.
func (c *Client) Bucket(ctx context.Context, name string) (*Bucket, error) {
	buckets, err := c.b2.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if bucket.Name == name {
			return &Bucket{
				b: bucket,
			}, nil
		}
	}
	b, err := c.b2.CreateBucket(ctx, name, "")
	if err != nil {
		return nil, err
	}
	return &Bucket{b}, err
}

func (b *Bucket) Delete(ctx context.Context) error {
	return b.b.DeleteBucket(ctx)
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

func (b *Bucket) getFile(ctx context.Context, name string) (*base.File, error) {
	files, _, err := b.b.ListFileNames(ctx, 1, name)
	if err != nil {
		return nil, err
	}
	if len(files) != 1 {
		return nil, errors.New("no files found")
	}
	if files[0].Name != name {
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
		size:   file.Size,
		chunks: make(map[int]*bytes.Buffer),
	}, nil
}

func (b *Bucket) DeleteObject(ctx context.Context, name string) error {
	file, err := b.getFile(ctx, name)
	if err != nil {
		return err
	}
	return file.DeleteFileVersion(ctx)
}
