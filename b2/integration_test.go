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
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/context"
)

const (
	apiID  = "B2_ACCOUNT_ID"
	apiKey = "B2_SECRET_KEY"
)

func TestReadWriteLive(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	bucket, done, err := startLiveTest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer done()

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
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	bucket, done, err := startLiveTest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer done()

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

func TestResumeWriter(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	bucket, _, err := startLiveTest(ctx)
	if err != nil {
		t.Fatal(err)
	}

	w := bucket.Object("foo").NewWriter(ctx)
	r := io.LimitReader(zReader{}, 3e8)
	go func() {
		// Cancel the context after the first chunk has been written.
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		defer cancel()
		for range ticker.C {
			if w.cidx > 1 {
				return
			}
		}
	}()
	if _, err := io.Copy(w, r); err != context.Canceled {
		t.Fatalf("io.Copy should have resulted in a canceled context")
	}

	ctx2 := context.Background()
	ctx2, cancel2 := context.WithTimeout(ctx2, 10*time.Minute)
	defer cancel2()
	bucket2, done, err := startLiveTest(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	w2 := bucket2.Object("foo").NewWriter(ctx2)
	r2 := io.LimitReader(zReader{}, 3e8)
	h1 := sha1.New()
	tr := io.TeeReader(r2, h1)
	w2.Resume = true
	w2.ConcurrentUploads = 2
	if _, err := io.Copy(w2, tr); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}
	begSHA := fmt.Sprintf("%x", h1.Sum(nil))

	objR := bucket2.Object("foo").NewReader(ctx2)
	objR.ConcurrentDownloads = 3
	h2 := sha1.New()
	if _, err := io.Copy(h2, objR); err != nil {
		t.Fatal(err)
	}
	if err := objR.Close(); err != nil {
		t.Error(err)
	}
	endSHA := fmt.Sprintf("%x", h2.Sum(nil))
	if endSHA != begSHA {
		t.Errorf("got conflicting hashes: got %q, want %q", endSHA, begSHA)
	}
}

func TestAttrs(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	bucket, done, err := startLiveTest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer done()

	attrs := &Attrs{
		ContentType: "jpeg/stream",
		Info: map[string]string{
			"one": "a",
			"two": "b",
		},
	}

	table := []struct {
		name string
		size int64
	}{
		{
			name: "small",
			size: 1e3,
		},
		{
			name: "large",
			size: 1e8 + 4,
		},
	}

	for _, e := range table {
		w := bucket.Object(e.name).NewWriter(ctx).WithAttrs(attrs)
		if _, err := io.Copy(w, io.LimitReader(zReader{}, e.size)); err != nil {
			t.Error(err)
			continue
		}
		if err := w.Close(); err != nil {
			t.Error(err)
			continue
		}
		gotAttrs, err := bucket.Object(e.name).Attrs(ctx)
		if err != nil {
			t.Error(err)
			continue
		}
		if gotAttrs.ContentType != attrs.ContentType {
			t.Errorf("bad content-type for %s: got %q, want %q", e.name, gotAttrs.ContentType, attrs.ContentType)
		}
		if !reflect.DeepEqual(gotAttrs.Info, attrs.Info) {
			t.Errorf("bad info for %s: got %v, want %v", e.name, gotAttrs.Info, attrs.Info)
		}
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

func startLiveTest(ctx context.Context) (*Bucket, func(), error) {
	id := os.Getenv(apiID)
	key := os.Getenv(apiKey)
	if id == "" || key == "" {
		return nil, nil, errors.New("B2_ACCOUNT_ID or B2_SECRET_KEY unset; skipping integration tests")
	}
	client, err := NewClient(ctx, id, key)
	if err != nil {
		return nil, nil, err
	}
	bucket, err := client.Bucket(ctx, id+bucketName)
	if err != nil {
		return nil, nil, err
	}
	f := func() {
		for c := range listObjects(ctx, bucket.ListObjects) {
			if c.err != nil {
				continue
			}
			if err := c.o.Delete(ctx); err != nil {
			}
		}
		if err := bucket.Delete(ctx); err != nil {
		}
	}
	return bucket, f, nil
}
