Blazer [![GoDoc](https://godoc.org/github.com/kurin/blazer/b2?status.svg)](https://godoc.org/github.com/kurin/blazer/b2)
====

Blazer is a Go library for Backblaze's B2.  It is designed for simple
integration, by exporting only a few standard Go types.

```go
import "github.com/kurin/blazer/b2"
```

## Examples

### Copying a file into B2

```go
func copyFile(ctx context.Context, bucket *b2.Bucket, src, dst string) error {
	f, err := file.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	obj := bucket.Object(dst)
	w := obj.NewWriter(ctx)
	if _, err := io.Copy(w, f); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}
```

If the file is less than 100MB, Blazer will simply buffer the file and use the
`b2_upload_file` API to send the file to Backblaze.  If the file is greater
than 100MB, Blazer will use B2's large file support to upload the file in 100MB
chunks.

### Copying a file into B2, with multiple concurrent uploads

Uploading a large file with multiple HTTP connections is simple:

```go
func copyFile(ctx context.Context, bucket *b2.Bucket, writers int, src, dst, string) error {
	f, err := file.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	w := bucket.Object(dst).NewWriter(ctx)
	w.ConcurrentUploads = writers
	if _, err := io.Copy(w, f); err != nil {
		w.Close()
		return err
	}
	return w.Close()
}
```

This will automatically split the file into `writers` chunks of 100MB uploads.
Note that 100MB is the smallest chunk size that B2 supports.

### Downloading a file from B2

Downloading is as simple as uploading:

```go
func downloadFile(ctx context.Context, bucket *b2.Bucket, downloads int, src, dst string) error {
	r, err := bucket.Object(src).NewReader(ctx)
	if err != nil {
		return err
	}
	defer r.Close()

	f, err := file.Create(dst)
	if err != nil {
		return err
	}
	r.ConcurrentDownloads = downloads
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
```

### Listing all objects in a bucket

```go
func printObjects(ctx context.Context, bucket *b2.Bucket) error {
	var cur *b2.Cursor
	for {
		objs, c, err := bucket.ListObjects(ctx, 1000, cur)
		if err != nil {
			return err
		}
		if len(objs) == 0 {
			return nil
		}
		for _, obj := range objs {
			fmt.Println(obj)
		}
		cur = c
	}
}
```
====
This is not an official Google product.
