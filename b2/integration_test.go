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
	"os"
	"testing"
	"time"

	"github.com/kurin/blazer/base"

	"golang.org/x/net/context"
)

const (
	apiID  = "B2_ACCOUNT_ID"
	apiKey = "B2_SECRET_KEY"
)

func TestReadWriteLive(t *testing.T) {
	id := os.Getenv(apiID)
	key := os.Getenv(apiKey)
	if id == "" || key == "" {
		t.Logf("B2_ACCOUNT_ID or B2_SECRET_KEY unset; skipping integration tests")
		return
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	base.FailSomeUploads = true

	client, err := NewClient(ctx, id, key)
	if err != nil {
		t.Fatal(err)
	}

	bucket, err := client.Bucket(ctx, bucketName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := bucket.Delete(ctx); err != nil {
			t.Error(err)
		}
	}()

	t.Logf("writing %q", smallFileName)
	sobj, wsha, err := writeFile(ctx, bucket, smallFileName, 1e6+42, 1e8)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("successfully wrote file %q", smallFileName)

	t.Logf("writing %q", largeFileName)
	lobj, wshaL, err := writeFile(ctx, bucket, largeFileName, 5e8-5e7, 1e8)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("successfully wrote file %q", largeFileName)

	if err := readFile(ctx, lobj, wshaL, 1e7, 10); err != nil {
		t.Error(err)
	}
	if err := readFile(ctx, sobj, wsha, 1e5, 10); err != nil {
		t.Error(err)
	}

	var cur *Cursor
	for {
		objs, c, err := bucket.ListObjects(ctx, 100, cur)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		for _, o := range objs {
			if err := o.Delete(ctx); err != nil {
				t.Error(err)
			}
		}
		if err == io.EOF {
			break
		}
		cur = c
	}
}

func TestHideShowLive(t *testing.T) {
	id := os.Getenv(apiID)
	key := os.Getenv(apiKey)
	if id == "" || key == "" {
		t.Logf("B2_ACCOUNT_ID or B2_SECRET_KEY unset; skipping integration tests")
		return
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	client, err := NewClient(ctx, id, key)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := client.Bucket(ctx, bucketName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for c := range listObjects(ctx, bucket.ListObjects) {
			if c.err != nil {
				t.Error(err)
				continue
			}
			if err := c.o.Delete(ctx); err != nil {
				t.Error(err)
			}
		}
		if err := bucket.Delete(ctx); err != nil {
			t.Error(err)
		}
	}()

	// write a file
	obj, _, err := writeFile(ctx, bucket, smallFileName, 1e6+42, 1e8)
	if err != nil {
		t.Fatal(err)
	}

	got, err := countObjects(ctx, bucket.ListCurrentObjects)
	if err != nil {
		t.Error(err)
	}
	if got != 1 {
		t.Fatalf("got %d objects, wanted 1", got)
	}

	// hide the file
	if err := obj.Hide(ctx); err != nil {
		t.Fatal(err)
	}

	got, err = countObjects(ctx, bucket.ListCurrentObjects)
	if err != nil {
		t.Error(err)
	}
	if got != 0 {
		t.Fatalf("got %d objects, wanted 0", got)
	}

	// unhide the file
	if err := bucket.Reveal(ctx, smallFileName); err != nil {
		t.Fatal(err)
	}

	// count see the object again
	got, err = countObjects(ctx, bucket.ListCurrentObjects)
	if err != nil {
		t.Error(err)
	}
	if got != 1 {
		t.Fatalf("got %d objects, wanted 1", got)
	}
}

type object struct {
	o   *Object
	err error
}

func countObjects(ctx context.Context, f func(context.Context, int, *Cursor) ([]*Object, *Cursor, error)) (int, error) {
	var got int
	ch := listObjects(ctx, f)
	for c := range ch {
		if c.err != nil {
			return 0, c.err
		}
		got++
	}
	return got, nil
}

func listObjects(ctx context.Context, f func(context.Context, int, *Cursor) ([]*Object, *Cursor, error)) <-chan object {
	ch := make(chan object)
	go func() {
		defer close(ch)
		var cur *Cursor
		for {
			objs, c, err := f(ctx, 100, cur)
			if err != nil && err != io.EOF {
				ch <- object{err: err}
				return
			}
			for _, o := range objs {
				ch <- object{o: o}
			}
			if err == io.EOF {
				return
			}
			cur = c
		}
	}()
	return ch
}
