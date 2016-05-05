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
	"hash"
	"io"
	"log"
	"sync"

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

	csize int
	ctx   context.Context
	ready chan chunk
	wg    sync.WaitGroup
	once  sync.Once
	done  sync.Once
	file  beLargeFileInterface

	o    *Object
	name string

	cbuf *bytes.Buffer
	cidx int
	chsh hash.Hash
	w    io.Writer

	emux sync.RWMutex
	err  error
}

func (w *Writer) setErr(err error) {
	w.emux.Lock()
	defer w.emux.Unlock()
	if w.err == nil {
		w.err = err
	}
}

func (w *Writer) getErr() error {
	w.emux.RLock()
	defer w.emux.RUnlock()
	return w.err
}

func (w *Writer) thread() {
	go func() {
		fc, err := w.file.getUploadPartURL(w.ctx)
		if err != nil {
			w.setErr(err)
			return
		}
		w.wg.Add(1)
		defer w.wg.Done()
		for {
			chunk, ok := <-w.ready
			if !ok {
				return
			}
			r := bytes.NewReader(chunk.buf.Bytes())
		redo:
			if _, err := fc.uploadPart(w.ctx, r, chunk.sha1, chunk.size, chunk.id); err != nil {
				if w.o.b.r.reupload(err) {
					log.Printf("b2 writer: %v; retrying", err)
					f, err := w.file.getUploadPartURL(w.ctx)
					if err != nil {
						w.setErr(err)
						return
					}
					fc = f
					uerr := err
					if _, err := r.Seek(0, 0); err != nil {
						log.Print(err)
						w.setErr(uerr)
						return
					}
					goto redo
				}
				w.setErr(err)
				return
			}
		}
	}()
}

// Write satisfies the io.Writer interface.
func (w *Writer) Write(p []byte) (int, error) {
	if err := w.getErr(); err != nil {
		return 0, err
	}
	if w.csize == 0 {
		w.csize = 1e8
	}
	left := w.csize - w.cbuf.Len()
	if len(p) < left {
		return w.w.Write(p)
	}
	i, err := w.w.Write(p[:left])
	if err != nil {
		w.setErr(err)
		return i, err
	}
	if err := w.sendChunk(); err != nil {
		w.setErr(err)
		return i, err
	}
	k, err := w.Write(p[left:])
	if err != nil {
		w.setErr(err)
	}
	return i + k, err
}

func (w *Writer) simpleWriteFile() error {
	ue, err := w.o.b.b.getUploadURL(w.ctx)
	if err != nil {
		return err
	}
	sha1 := fmt.Sprintf("%x", w.chsh.Sum(nil))
	ctype := w.ContentType
	if ctype == "" {
		ctype = "application/octet-stream"
	}
redo:
	f, err := ue.uploadFile(w.ctx, w.cbuf, w.cbuf.Len(), w.name, ctype, sha1, w.Info)
	if err != nil {
		if w.o.b.r.reupload(err) {
			log.Printf("b2 writer: %v; retrying", err)
			u, err := w.o.b.b.getUploadURL(w.ctx)
			if err != nil {
				return err
			}
			ue = u
			goto redo
		}
		return err
	}
	w.o.f = f
	return nil
}

func (w *Writer) sendChunk() error {
	var err error
	w.once.Do(func() {
		ctype := w.ContentType
		if ctype == "" {
			ctype = "application/octet-stream"
		}
		lf, e := w.o.b.b.startLargeFile(w.ctx, w.name, ctype, w.Info)
		if e != nil {
			err = e
			return
		}
		w.file = lf
		w.ready = make(chan chunk)
		if w.ConcurrentUploads < 1 {
			w.ConcurrentUploads = 1
		}
		for i := 0; i < w.ConcurrentUploads; i++ {
			w.thread()
		}
	})
	if err != nil {
		return err
	}
	w.ready <- chunk{
		id:   w.cidx + 1,
		size: w.cbuf.Len(),
		sha1: fmt.Sprintf("%x", w.chsh.Sum(nil)),
		buf:  w.cbuf,
	}
	w.cidx++
	w.chsh = sha1.New()
	w.cbuf = &bytes.Buffer{}
	w.w = io.MultiWriter(w.chsh, w.cbuf)
	return nil
}

// Close satisfies the io.Closer interface.  It is critical to check the return
// value of Close on all writers.
func (w *Writer) Close() error {
	var oerr error
	w.done.Do(func() {
		if w.cidx == 0 {
			oerr = w.simpleWriteFile()
			return
		}
		if w.cbuf.Len() > 0 {
			if err := w.sendChunk(); err != nil {
				oerr = err
				return
			}
		}
		close(w.ready)
		w.wg.Wait()
		f, err := w.file.finishLargeFile(w.ctx)
		if err != nil {
			oerr = err
			return
		}
		w.o.f = f
	})
	return oerr
}
