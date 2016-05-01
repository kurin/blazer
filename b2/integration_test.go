package b2

import (
	"crypto/sha1"
	"fmt"
	"io"
	"os"
	"testing"

	"golang.org/x/net/context"
)

const (
	apiID  = "B2_ACCOUNT_ID"
	apiKey = "B2_SECRET_KEY"
)

const (
	bucketName    = "MahBucket"
	smallFileName = "TeenyTiny"
	largeFileName = "BigBytes"
)

type zReader struct{}

var pattern = []byte{0x02, 0x80, 0xff, 0x1a, 0xcc, 0x63, 0x22}

func (zReader) Read(p []byte) (int, error) {
	copy(p, pattern)
	return len(p), nil
}

func TestReadWrite(t *testing.T) {
	id := os.Getenv(apiID)
	key := os.Getenv(apiKey)
	if id == "" || key == "" {
		t.Logf("B2_ACCOUNT_ID or B2_SECRET_KEY unset; skipping integration tests")
		return
	}
	ctx := context.Background()

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

	wsha, err := writeFile(ctx, bucket, smallFileName, 1e6+42)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := bucket.DeleteObject(ctx, smallFileName); err != nil {
			t.Error(err)
		}
	}()

	if err := readFile(ctx, bucket, smallFileName, wsha, 1e5, 10); err != nil {
		t.Error(err)
	}

	wshaL, err := writeFile(ctx, bucket, largeFileName, 4e8-50)
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err := bucket.DeleteObject(ctx, largeFileName); err != nil {
			t.Error(err)
		}
	}()

	if err := readFile(ctx, bucket, largeFileName, wshaL, 1e7, 10); err != nil {
		t.Error(err)
	}
}

func writeFile(ctx context.Context, bucket *Bucket, name string, size int64) (string, error) {
	r := io.LimitReader(zReader{}, size)
	f := bucket.NewWriter(ctx, name)
	h := sha1.New()
	w := io.MultiWriter(f, h)
	f.ConcurrentUploads = 5
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
