package b2

import (
	"math/rand"
	"time"

	"golang.org/x/net/context"
)

// This file wraps the baseline interfaces with backoff and retry semantics.

type beRootInterface interface {
	backoff(error) (time.Duration, bool)
	reauth(error) bool
	authorizeAccount(context.Context, string, string) error
	reauthorizeAccount(context.Context) error
	createBucket(ctx context.Context, name, btype string) (beBucketInterface, error)
	listBuckets(context.Context) ([]beBucketInterface, error)
}

type beRoot struct {
	account, key string
	b2i          b2RootInterface
}

type beBucketInterface interface {
	name() string
	deleteBucket(context.Context) error
}

type beBucket struct {
	b2bucket b2BucketInterface
	ri       beRootInterface
}

func (r *beRoot) backoff(err error) (time.Duration, bool) {
	return r.b2i.backoff(err)
}

func (r *beRoot) reauth(err error) bool {
	return r.b2i.reauth(err)
}

func (r *beRoot) authorizeAccount(ctx context.Context, account, key string) error {
	f := func() (bool, error) {
		if err := r.b2i.authorizeAccount(ctx, account, key); err != nil {
			return false, err
		}
		r.account = account
		r.key = key
		return true, nil
	}
	return withBackoff(ctx, r, f)
}

func (r *beRoot) reauthorizeAccount(ctx context.Context) error {
	return r.authorizeAccount(ctx, r.account, r.key)
}

func (r *beRoot) createBucket(ctx context.Context, name, btype string) (beBucketInterface, error) {
	var bi beBucketInterface
	f := func() (bool, error) {
		g := func() error {
			bucket, err := r.b2i.createBucket(ctx, name, btype)
			if err != nil {
				return err
			}
			bi = &beBucket{
				b2bucket: bucket,
				ri:       r,
			}
			return nil
		}
		if err := withReauth(ctx, r, g); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := withBackoff(ctx, r, f); err != nil {
		return nil, err
	}
	return bi, nil
}

func (r *beRoot) listBuckets(ctx context.Context) ([]beBucketInterface, error) {
	var buckets []beBucketInterface
	f := func() (bool, error) {
		g := func() error {
			bs, err := r.b2i.listBuckets(ctx)
			if err != nil {
				return err
			}
			for _, b := range bs {
				buckets = append(buckets, &beBucket{
					b2bucket: b,
					ri:       r,
				})
			}
			return nil
		}
		if err := withReauth(ctx, r, g); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := withBackoff(ctx, r, f); err != nil {
		return nil, err
	}
	return buckets, nil
}

func (b *beBucket) name() string {
	return b.b2bucket.name()
}

func (b *beBucket) deleteBucket(ctx context.Context) error {
	f := func() (bool, error) {
		g := func() error {
			return b.b2bucket.deleteBucket(ctx)
		}
		if err := withReauth(ctx, b.ri, g); err != nil {
			return false, err
		}
		return true, nil
	}
	return withBackoff(ctx, b.ri, f)
}

func jitter(d time.Duration) time.Duration {
	f := float64(d)
	f /= 50
	f += f * (rand.Float64() - 0.5)
	return time.Duration(f)
}

func getBackoff(d time.Duration) time.Duration {
	if d > 15*time.Second {
		return d + jitter(d)
	}
	return d*2 + jitter(d*2)
}

func withBackoff(ctx context.Context, ri beRootInterface, f func() (bool, error)) error {
	backoff := 500 * time.Millisecond
	for {
		final, err := f()
		if final {
			return err
		}
		bo, ok := ri.backoff(err)
		if ok {
			backoff = bo
		} else {
			backoff = getBackoff(backoff)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
}

func withReauth(ctx context.Context, ri beRootInterface, f func() error) error {
	err := f()
	if ri.reauth(err) {
		if err := ri.reauthorizeAccount(ctx); err != nil {
			return err
		}
		err = f()
	}
	return err
}
