package b2

import (
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
	name() string
}

type b2Root struct {
	b *base.B2
}

type b2Bucket struct {
	b *base.Bucket
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
