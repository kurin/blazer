package base

import (
	"bytes"
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

func (zReader) Read(p []byte) (int, error) {
	return len(p), nil
}

func TestStorage(t *testing.T) {
	id := os.Getenv(apiID)
	key := os.Getenv(apiKey)
	if id == "" || key == "" {
		t.Logf("B2_ACCOUNT_ID or B2_SECRET_KEY unset; skipping integration tests")
		return
	}
	ctx := context.Background()

	// b2_authorize_account
	b2, err := B2AuthorizeAccount(ctx, id, key)
	if err != nil {
		t.Fatal(err)
	}

	// b2_create_bucket
	bucket, err := b2.CreateBucket(ctx, bucketName, "")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		// b2_delete_bucket
		if err := bucket.DeleteBucket(ctx); err != nil {
			t.Error(err)
		}
	}()

	// b2_list_buckets
	buckets, err := b2.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, bucket := range buckets {
		if bucket.Name == bucketName {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("%s: new bucket not found", bucketName)
	}

	// b2_get_upload_url
	ue, err := bucket.GetUploadURL(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// b2_upload_file
	smallFile := io.LimitReader(zReader{}, 1024*50) // 50k
	hash := sha1.New()
	buf := &bytes.Buffer{}
	w := io.MultiWriter(hash, buf)
	if _, err := io.Copy(w, smallFile); err != nil {
		t.Error(err)
	}
	file, err := ue.UploadFile(ctx, buf, buf.Len(), smallFileName, "application/octet-stream", fmt.Sprintf("%x", hash.Sum(nil)), nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		// b2_delete_file_version
		if err := file.DeleteFileVersion(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	// b2_start_large_file
	lf, err := bucket.StartLargeFile(ctx, largeFileName, "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}

	// b2_get_upload_part_url
	fc, err := lf.GetUploadPartURL(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// b2_upload_part
	largeFile := io.LimitReader(zReader{}, 2*1e8) // 200M
	for i := 0; i < 2; i++ {
		r := io.LimitReader(largeFile, 1e8) // 100M
		hash := sha1.New()
		buf := &bytes.Buffer{}
		w := io.MultiWriter(hash, buf)
		if _, err := io.Copy(w, r); err != nil {
			t.Error(err)
		}
		if _, err := fc.UploadPart(ctx, buf, fmt.Sprintf("%x", hash.Sum(nil)), buf.Len(), i+1); err != nil {
			t.Error(err)
		}
	}

	// b2_finish_large_file
	lfile, err := lf.FinishLargeFile(ctx)
	if err != nil {
		t.Error(err)
	}

	defer func() {
		if err := lfile.DeleteFileVersion(ctx); err != nil {
			t.Error(err)
		}
	}()

	clf, err := bucket.StartLargeFile(ctx, largeFileName, "application/octet-stream", nil)
	if err != nil {
		t.Fatal(err)
	}

	// b2_cancel_large_file
	if err := clf.CancelLargeFile(ctx); err != nil {
		t.Fatal(err)
	}

	// b2_list_file_names
	files, _, err := bucket.ListFileNames(ctx, 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2 files, got %d: %v", len(files), files)
	}
}
