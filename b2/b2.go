package b2

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"hash"
	"io"
	"log"
	"sync"

	"github.com/kurin/gozer/base"
)

// B2 is a Backblaze connection.
type B2 struct {
	b2 *base.B2
}

// New returns a new connection to Backblaze B2.
func New(account, key string) (*B2, error) {
	b2, err := base.B2AuthorizeAccount(account, key)
	if err != nil {
		return nil, err
	}
	return &B2{
		b2: b2,
	}, nil
}

// Bucket is a reference to a B2 bucket.
type Bucket struct {
	b *base.Bucket
}

// Bucket returns the named bucket, if it exists.
func (b *B2) Bucket(name string) (*Bucket, error) {
	buckets, err := b.b2.ListBuckets()
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if bucket.Name == name {
			return &Bucket{
				b: bucket,
			}, nil
		}
	}
	// TODO: create bucket
	return nil, fmt.Errorf("%s: no such bucket", name)
}

// NewWriter returns a new writer for the given file.
func (b *Bucket) NewWriter(name, contentType string, info map[string]string) *Writer {
	bw := &Writer{
		bucket: b.b,
		name:   name,
		ctype:  contentType,
		info:   info,
		chsh:   sha1.New(),
		cbuf:   &bytes.Buffer{},
	}
	bw.w = io.MultiWriter(bw.chsh, bw.cbuf)
	return bw
}

type chunk struct {
	id      int
	attempt int
	size    int
	sha1    string
	buf     *bytes.Buffer
}

// Writer writes data into Backblaze.  It automatically switches to the large
// file API if the file exceeds 100MB (that is, 1e8 bytes).  Due to that and
// other Backblaze API details, there is a large (100MB) buffer.
type Writer struct {
	// ConcurrentUploads is number of different threads sending data concurrently
	// to Backblaze for large files.  This can increase performance greatly, as
	// each thread will hit a different endpoint.  However, there is a 100MB
	// buffer for each thread.  Values less than 1 are equivalent to 1.
	ConcurrentUploads int

	// TotalRetries is the number of times a failed partial write will be retried
	// until the operation returns an error.
	TotalRetries int

	ready chan chunk
	wg    sync.WaitGroup
	once  sync.Once
	file  *base.LargeFile

	bucket *base.Bucket
	name   string
	ctype  string
	info   map[string]string

	cbuf *bytes.Buffer
	cidx int
	chsh hash.Hash
	w    io.Writer
}

func (bw *Writer) thread() {
	go func() {
		fc, err := bw.file.GetUploadPartURL()
		if err != nil {
			log.Print(err)
			return
		}
		bw.wg.Add(1)
		defer bw.wg.Done()
		for {
			chunk, ok := <-bw.ready
			if !ok {
				return
			}
			if _, err := fc.UploadPart(chunk.buf, chunk.sha1, chunk.size, chunk.id); err != nil {
				log.Print(err)
				chunk.attempt++
				bw.ready <- chunk
				continue
			}
		}
	}()
}

// Write satisfies the io.Writer interface.
func (bw *Writer) Write(p []byte) (int, error) {
	left := 1e8 - bw.cbuf.Len()
	if len(p) < left {
		return bw.w.Write(p)
	}
	i, err := bw.w.Write(p[:left])
	if err != nil {
		return i, err
	}
	if err := bw.sendChunk(); err != nil {
		return i, err
	}
	k, err := bw.Write(p[left:])
	return i + k, err
}

func (bw *Writer) simpleWriteFile() error {
	ue, err := bw.bucket.GetUploadURL()
	if err != nil {
		return err
	}
	sha1 := fmt.Sprintf("%x", bw.chsh.Sum(nil))
	if _, err := ue.UploadFile(bw.cbuf, bw.cbuf.Len(), bw.name, bw.ctype, sha1, bw.info); err != nil {
		return err
	}
	return nil
}

func (bw *Writer) sendChunk() error {
	var err error
	bw.once.Do(func() {
		lf, e := bw.bucket.StartLargeFile(bw.name, bw.ctype, bw.info)
		if e != nil {
			err = e
			return
		}
		bw.file = lf
		bw.ready = make(chan chunk)
		if bw.ConcurrentUploads < 1 {
			bw.ConcurrentUploads = 1
		}
		for i := 0; i < bw.ConcurrentUploads; i++ {
			bw.thread()
		}
	})
	if err != nil {
		return err
	}
	bw.ready <- chunk{
		id:   bw.cidx + 1,
		size: bw.cbuf.Len(),
		sha1: fmt.Sprintf("%x", bw.chsh.Sum(nil)),
		buf:  bw.cbuf,
	}
	bw.cidx++
	bw.chsh = sha1.New()
	bw.cbuf = &bytes.Buffer{}
	bw.w = io.MultiWriter(bw.chsh, bw.cbuf)
	return nil
}

// Close satisfies the io.Closer interface.  It should not be called more than
// once.
func (bw *Writer) Close() error {
	if bw.cidx == 0 {
		return bw.simpleWriteFile()
	}
	if bw.cbuf.Len() > 0 {
		if err := bw.sendChunk(); err != nil {
			return err
		}
	}
	close(bw.ready)
	bw.wg.Wait()
	if _, err := bw.file.FinishLargeFile(); err != nil {
		return err
	}
	return nil
}
