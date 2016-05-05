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

	time.Sleep(10 * time.Second) // give backblaze time to catch up

	if err := readFile(ctx, lobj, wshaL, 1e7, 10); err != nil {
		t.Error(err)
	}
	if err := readFile(ctx, sobj, wsha, 1e5, 10); err != nil {
		t.Error(err)
	}

	var cur *Cursor
	for {
		objs, c, err := bucket.ListObjects(ctx, 100, cur)
		if err != nil {
			t.Fatal(err)
		}
		if len(objs) == 0 {
			break
		}
		for _, o := range objs {
			if err := o.Delete(ctx); err != nil {
				t.Error(err)
			}
		}
		cur = c
	}
}
