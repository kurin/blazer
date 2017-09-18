package consistent

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/kurin/blazer/b2"
)

const (
	apiID      = "B2_ACCOUNT_ID"
	apiKey     = "B2_SECRET_KEY"
	bucketName = "consistobucket"
)

func TestOperationLive(t *testing.T) {
	ctx := context.Background()
	bucket, done := startLiveTest(ctx, t)
	defer done()

	g := NewGroup(bucket, "tester")
	name := "some_kinda_name/thing.txt"

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		i := i
		go func() {
			var n int
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := g.Operate(ctx, name, func(b []byte) ([]byte, error) {
					if len(b) > 0 {
						i, err := strconv.Atoi(string(b))
						if err != nil {
							return nil, err
						}
						n = i
					}
					t.Logf("thread %d on cycle %d: %d++", i, j, n)
					return []byte(strconv.Itoa(n + 1)), nil
				}); err != nil {
					t.Error(err)
				}
				t.Logf("thread %d: successful %d++", i, n)
			}
		}()
	}
	wg.Wait()

	r, err := g.NewReader(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	n, err := strconv.Atoi(string(b))
	if err != nil {
		t.Fatal(err)
	}
	if n != 100 {
		t.Errorf("result: got %d, want 10", n)
	}
}

func startLiveTest(ctx context.Context, t *testing.T) (*b2.Bucket, func()) {
	id := os.Getenv(apiID)
	key := os.Getenv(apiKey)
	if id == "" || key == "" {
		t.Skipf("B2_ACCOUNT_ID or B2_SECRET_KEY unset; skipping integration tests")
		return nil, nil
	}
	client, err := b2.NewClient(ctx, id, key)
	if err != nil {
		t.Fatal(err)
		return nil, nil
	}
	bucket, err := client.NewBucket(ctx, id+"-"+bucketName, nil)
	if err != nil {
		t.Fatal(err)
		return nil, nil
	}
	f := func() {
		for c := range listObjects(ctx, bucket.ListObjects) {
			if c.err != nil {
				continue
			}
			if err := c.o.Delete(ctx); err != nil {
				t.Error(err)
			}
		}
		if err := bucket.Delete(ctx); err != nil && !b2.IsNotExist(err) {
			t.Error(err)
		}
	}
	return bucket, f
}

func listObjects(ctx context.Context, f func(context.Context, int, *b2.Cursor) ([]*b2.Object, *b2.Cursor, error)) <-chan object {
	ch := make(chan object)
	go func() {
		defer close(ch)
		var cur *b2.Cursor
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

type object struct {
	o   *b2.Object
	err error
}
