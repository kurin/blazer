// Copyright 2017, Google
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
)

type writeBuffer interface {
	io.Writer
	Len() int
	Reader() (io.ReadSeeker, error)
	Hash() string // sha1 or whatever it is
}

type memoryBuffer struct {
	buf *bytes.Buffer
	hsh hash.Hash
	w   io.Writer
}

func newMemoryBuffer() *memoryBuffer {
	mb := &memoryBuffer{
		buf: &bytes.Buffer{},
		hsh: sha1.New(),
	}
	mb.w = io.MultiWriter(mb.hsh, mb.buf)
	return mb
}

func (mb *memoryBuffer) Write(p []byte) (int, error)    { return mb.w.Write(p) }
func (mb *memoryBuffer) Len() int                       { return mb.buf.Len() }
func (mb *memoryBuffer) Reader() (io.ReadSeeker, error) { return bytes.NewReader(mb.buf.Bytes()), nil }
func (mb *memoryBuffer) Hash() string                   { return fmt.Sprintf("%x", mb.hsh.Sum(nil)) }
