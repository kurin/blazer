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
	"golang.org/x/net/context"
)

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

	// ContentType sets the content type of the file to be uploaded.  If unset,
	// "application/octet-stream" is used.
	ContentType string

	// Info is a map of up to ten key/value pairs that are stored with the file.
	Info map[string]string

	ctx   context.Context
	ready chan chunk
	wg    sync.WaitGroup
	once  sync.Once
	done  sync.Once
	file  *base.LargeFile

	bucket *base.Bucket
	name   string

	cbuf *bytes.Buffer
	cidx int
	chsh hash.Hash
	w    io.Writer
}

func (bw *Writer) thread() {
	go func() {
		fc, err := bw.file.GetUploadPartURL(bw.ctx)
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
			if _, err := fc.UploadPart(bw.ctx, chunk.buf, chunk.sha1, chunk.size, chunk.id); err != nil {
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
	ue, err := bw.bucket.GetUploadURL(bw.ctx)
	if err != nil {
		return err
	}
	sha1 := fmt.Sprintf("%x", bw.chsh.Sum(nil))
	ctype := bw.ContentType
	if ctype == "" {
		ctype = "application/octet-stream"
	}
	if _, err := ue.UploadFile(bw.ctx, bw.cbuf, bw.cbuf.Len(), bw.name, ctype, sha1, bw.Info); err != nil {
		return err
	}
	return nil
}

func (bw *Writer) sendChunk() error {
	var err error
	bw.once.Do(func() {
		ctype := bw.ContentType
		if ctype == "" {
			ctype = "application/octet-stream"
		}
		lf, e := bw.bucket.StartLargeFile(bw.ctx, bw.name, ctype, bw.Info)
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

// Close satisfies the io.Closer interface.
func (bw *Writer) Close() error {
	var oerr error
	bw.done.Do(func() {
		if bw.cidx == 0 {
			oerr = bw.simpleWriteFile()
			return
		}
		if bw.cbuf.Len() > 0 {
			if err := bw.sendChunk(); err != nil {
				oerr = err
				return
			}
		}
		close(bw.ready)
		bw.wg.Wait()
		if _, err := bw.file.FinishLargeFile(bw.ctx); err != nil {
			oerr = err
			return
		}
	})
	return oerr
}
