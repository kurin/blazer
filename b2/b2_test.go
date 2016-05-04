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
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
)

const (
	bucketName    = "MahBucket"
	smallFileName = "TeenyTiny"
	largeFileName = "BigBytes"
)

type testError struct {
	retry   bool
	backoff time.Duration
	reauth  bool
}

func (t testError) Error() string {
	return fmt.Sprintf("retry %v; backoff %v; reauth %v", t.retry, t.backoff, t.reauth)
}

type errCont struct {
	errMap map[string]map[int]error
	opMap  map[string]int
}

func (e *errCont) getError(name string) error {
	if e.errMap == nil {
		return nil
	}
	if e.opMap == nil {
		e.opMap = make(map[string]int)
	}
	i := e.opMap[name]
	e.opMap[name]++
	return e.errMap[name][i]
}

type testRoot struct {
	errs      *errCont
	auths     int
	bucketMap map[string]map[string]string
}

func (t *testRoot) authorizeAccount(context.Context, string, string) error {
	t.auths++
	return nil
}

func (t *testRoot) backoff(err error) time.Duration {
	e, ok := err.(testError)
	if !ok {
		return 0
	}
	return e.backoff
}

func (t *testRoot) reauth(err error) bool {
	e, ok := err.(testError)
	if !ok {
		return false
	}
	return e.reauth
}

func (t *testRoot) transient(err error) bool {
	e, ok := err.(testError)
	if !ok {
		return false
	}
	return e.retry || e.backoff > 0
}

func (t *testRoot) createBucket(_ context.Context, name, _ string) (b2BucketInterface, error) {
	if err := t.errs.getError("createBucket"); err != nil {
		return nil, err
	}
	if _, ok := t.bucketMap[name]; ok {
		return nil, fmt.Errorf("%s: bucket exists", name)
	}
	m := make(map[string]string)
	t.bucketMap[name] = m
	return &testBucket{
		n:     name,
		files: m,
	}, nil
}

func (t *testRoot) listBuckets(context.Context) ([]b2BucketInterface, error) {
	var b []b2BucketInterface
	for k, v := range t.bucketMap {
		b = append(b, &testBucket{
			n:     k,
			files: v,
		})
	}
	return b, nil
}

type testBucket struct {
	n     string
	files map[string]string
}

func (t *testBucket) name() string                       { return t.n }
func (t *testBucket) deleteBucket(context.Context) error { return nil }

func (t *testBucket) getUploadURL(context.Context) (b2URLInterface, error) {
	return &testURL{
		files: t.files,
	}, nil
}

func (t *testBucket) startLargeFile(_ context.Context, name, _ string, _ map[string]string) (b2LargeFileInterface, error) {
	return &testLargeFile{
		name:  name,
		parts: make(map[int][]byte),
		files: t.files,
	}, nil
}

func (t *testBucket) listFileNames(ctx context.Context, count int, cont string) ([]b2FileInterface, string, error) {
	var f []string
	for name := range t.files {
		f = append(f, name)
	}
	sort.Strings(f)
	idx := sort.SearchStrings(f, cont)
	var b []b2FileInterface
	var next string
	for i := idx; i < len(f) && i-idx < count; i++ {
		b = append(b, &testFile{
			n:     f[i],
			s:     int64(len(t.files[f[i]])),
			files: t.files,
		})
		if i+1 < len(f) {
			next = f[i+1]
		}
		if i+1 == len(f) {
			next = ""
		}
	}
	return b, next, nil
}

func (t *testBucket) listFileVersions(ctx context.Context, count int, a, b string) ([]b2FileInterface, string, string, error) {
	x, y, z := t.listFileNames(ctx, count, a)
	return x, y, "", z
}

func (t *testBucket) downloadFileByName(_ context.Context, name string, _, _ int64) (b2FileReaderInterface, error) {
	return &testFileReader{
		b: ioutil.NopCloser(bytes.NewBufferString(t.files[name])),
	}, nil
}

type testURL struct {
	files map[string]string
}

func (t *testURL) reload(context.Context) error { return nil }

func (t *testURL) uploadFile(_ context.Context, r io.Reader, _ int, name, _, _ string, _ map[string]string) (b2FileInterface, error) {
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, r); err != nil {
		return nil, err
	}
	t.files[name] = buf.String()
	return nil, nil
}

type testLargeFile struct {
	name  string
	mux   sync.Mutex
	parts map[int][]byte
	files map[string]string
}

func (t *testLargeFile) finishLargeFile(context.Context) (b2FileInterface, error) {
	var total []byte
	for i := 1; i <= len(t.parts); i++ {
		total = append(total, t.parts[i]...)
	}
	t.files[t.name] = string(total)
	return &testFile{
		n:     t.name,
		s:     int64(len(total)),
		files: t.files,
	}, nil
}

func (t *testLargeFile) getUploadPartURL(context.Context) (b2FileChunkInterface, error) {
	return &testFileChunk{
		parts: t.parts,
		mux:   &t.mux,
	}, nil
}

type testFileChunk struct {
	mux   *sync.Mutex
	parts map[int][]byte
}

func (t *testFileChunk) reload(context.Context) error { return nil }

func (t *testFileChunk) uploadPart(_ context.Context, r io.Reader, _ string, _, index int) (int, error) {
	buf := &bytes.Buffer{}
	if i, err := io.Copy(buf, r); err != nil {
		return int(i), err
	}
	t.mux.Lock()
	t.parts[index] = buf.Bytes()
	t.mux.Unlock()
	return 0, nil
}

type testFile struct {
	n     string
	s     int64
	files map[string]string
}

func (t *testFile) name() string { return t.n }
func (t *testFile) size() int64  { return t.s }
func (t *testFile) deleteFileVersion(context.Context) error {
	delete(t.files, t.n)
	return nil
}

type testFileReader struct {
	b io.ReadCloser
	s int64
}

func (t *testFileReader) Read(p []byte) (int, error)                      { return t.b.Read(p) }
func (t *testFileReader) Close() error                                    { return nil }
func (t *testFileReader) stats() (int, string, string, map[string]string) { return 0, "", "", nil }

type zReader struct{}

var pattern = []byte{0x02, 0x80, 0xff, 0x1a, 0xcc, 0x63, 0x22}

func (zReader) Read(p []byte) (int, error) {
	copy(p, pattern)
	return len(p), nil
}

func TestReauth(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	root := &testRoot{
		bucketMap: make(map[string]map[string]string),
		errs: &errCont{
			errMap: map[string]map[int]error{
				"createBucket": {0: testError{reauth: true}},
			},
		},
	}
	client := &Client{
		backend: &beRoot{
			b2i: root,
		},
	}
	auths := root.auths
	if _, err := client.Bucket(ctx, "fun"); err != nil {
		t.Errorf("bucket should not err, got %v", err)
	}
	if root.auths != auths+1 {
		t.Errorf("client should have re-authenticated; did not")
	}
}

func TestBackoff(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var calls []time.Duration
	ch := make(chan time.Time)
	close(ch)
	after = func(d time.Duration) <-chan time.Time {
		calls = append(calls, d)
		return ch
	}

	root := &testRoot{
		bucketMap: make(map[string]map[string]string),
		errs: &errCont{
			errMap: map[string]map[int]error{
				"createBucket": {
					0: testError{backoff: time.Second},
					1: testError{backoff: 2 * time.Second},
				},
			},
		},
	}
	client := &Client{
		backend: &beRoot{
			b2i: root,
		},
	}
	if _, err := client.Bucket(ctx, "fun"); err != nil {
		t.Errorf("bucket should not err, got %v", err)
	}
	if len(calls) != 2 {
		t.Errorf("wrong number of backoff calls; got %d, want 2", len(calls))
	}
}

func TestBackoffWithoutRetryAfter(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var calls []time.Duration
	ch := make(chan time.Time)
	close(ch)
	after = func(d time.Duration) <-chan time.Time {
		calls = append(calls, d)
		return ch
	}

	root := &testRoot{
		bucketMap: make(map[string]map[string]string),
		errs: &errCont{
			errMap: map[string]map[int]error{
				"createBucket": {
					0: testError{retry: true},
					1: testError{retry: true},
				},
			},
		},
	}
	client := &Client{
		backend: &beRoot{
			b2i: root,
		},
	}
	if _, err := client.Bucket(ctx, "fun"); err != nil {
		t.Errorf("bucket should not err, got %v", err)
	}
	if len(calls) != 2 {
		t.Errorf("wrong number of backoff calls; got %d, want 2", len(calls))
	}
}

func TestReadWrite(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	client := &Client{
		backend: &beRoot{
			b2i: &testRoot{
				bucketMap: make(map[string]map[string]string),
				errs:      &errCont{},
			},
		},
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

	wsha, err := writeFile(ctx, bucket, smallFileName, 1e6+42, 1e8)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := bucket.DeleteFile(ctx, smallFileName); err != nil {
			t.Error(err)
		}
	}()

	if err := readFile(ctx, bucket, smallFileName, wsha, 1e5, 10); err != nil {
		t.Error(err)
	}

	wshaL, err := writeFile(ctx, bucket, largeFileName, 1e6-1e5, 1e4)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := bucket.DeleteFile(ctx, largeFileName); err != nil {
			t.Error(err)
		}
	}()

	if err := readFile(ctx, bucket, largeFileName, wshaL, 1e7, 10); err != nil {
		t.Error(err)
	}
}

func writeFile(ctx context.Context, bucket *Bucket, name string, size int64, csize int) (string, error) {
	r := io.LimitReader(zReader{}, size)
	f := bucket.NewWriter(ctx, name)
	h := sha1.New()
	w := io.MultiWriter(f, h)
	f.ConcurrentUploads = 5
	f.csize = csize
	if _, err := io.Copy(w, r); err != nil {
		return "", err
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func readFile(ctx context.Context, bucket *Bucket, name, sha string, chunk, concur int) error {
	r, err := bucket.NewReader(ctx, name)
	if err != nil {
		return err
	}
	r.ChunkSize = chunk
	r.ConcurrentDownloads = concur
	h := sha1.New()
	if _, err := io.Copy(h, r); err != nil {
		return err
	}
	if err := r.Close(); err != nil {
		return err
	}
	rsha := fmt.Sprintf("%x", h.Sum(nil))
	if sha != rsha {
		return fmt.Errorf("bad hash: got %s, want %s", rsha, sha)
	}
	return nil
}
