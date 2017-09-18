package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/kurin/blazer/b2"
)

const (
	apiID  = "B2_ACCOUNT_ID"
	apiKey = "B2_SECRET_KEY"
)

const (
	bucketName    = "base-tests"
	smallFileName = "TeenyTiny"
	largeFileName = "BigBytes"
)

func main() {
	id := os.Getenv(apiID)
	key := os.Getenv(apiKey)
	ctx := context.Background()
	client, err := b2.NewClient(ctx, id, key)
	if err != nil {
		fmt.Println(err)
		return
	}
	bucket, err := client.NewBucket(ctx, id+"-"+bucketName, nil)
	if b2.IsNotExist(err) {
		return
	}
	if err != nil {
		fmt.Println(err)
		return
	}
	defer bucket.Delete(ctx)
	cur := &b2.Cursor{}
	for {
		os, c, err := bucket.ListObjects(ctx, 1000, cur)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			return
		}
		for _, o := range os {
			o.Delete(ctx)
		}
		if err == io.EOF {
			return
		}
		cur = c
	}
}
