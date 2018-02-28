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
	"fmt"
	"math"
	"sync"
	"time"
)

// StatusInfo reports information about a client.
type StatusInfo struct {
	Writers    map[string]*WriterStatus
	Readers    map[string]*ReaderStatus
	MethodInfo *MethodInfo
}

// numBuckets is the number of buckets per histogram, corresponding to 0-1ms,
// 1-3ms, 3-7ms, etc.  Each bucket index i is 2^i ms wide, except of course the
// last one.
const numBuckets = 30

func getBucket(d time.Duration) int {
	i := int(math.Log2(1 + float64(d/time.Millisecond)))
	if i > numBuckets {
		i = numBuckets
	}
	return i
}

type hist [numBuckets]int

func (h hist) add(o hist) hist {
	var r hist
	for i := range h {
		r[i] = h[i] + o[i]
	}
	return r
}

func (h hist) dup() hist {
	var r hist
	for i := range h {
		r[i] = h[i]
	}
	return r
}

func (h hist) inc(i int) hist { h[i]++; return h }

// MethodInfo reports aggregated information about specific method calls.
type MethodInfo struct {
	mu   sync.Mutex
	data map[string]map[int]hist
}

func (mi *MethodInfo) ensure(method string) {
	if mi.data == nil {
		mi.data = make(map[string]map[int]hist)
	}
	if mi.data[method] == nil {
		mi.data[method] = make(map[int]hist)
	}
}

func (mi *MethodInfo) dup() *MethodInfo {
	mi.mu.Lock()
	defer mi.mu.Unlock()

	new := &MethodInfo{}
	for method, codes := range mi.data {
		for code, h := range codes {
			new.ensure(method)
			new.data[method][code] = h.dup()
		}
	}
	return new
}

func (mi *MethodInfo) addCall(method string, d time.Duration, code int) {
	mi.mu.Lock()
	defer mi.mu.Unlock()

	mi.ensure(method)
	mi.data[method][code] = mi.data[method][code].inc(getBucket(d))
}

// Count returns the total number of method calls.
func (mi *MethodInfo) Count() int {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	var t int
	for _, codes := range mi.data {
		for _, h := range codes {
			for i := range h {
				t += h[i]
			}
		}
	}
	return t
}

// CountByMethod returns the number of method calls per method.
func (mi *MethodInfo) CountByMethod() map[string]int {
	mi.mu.Lock()
	defer mi.mu.Unlock()
	t := make(map[string]int)
	for method, codes := range mi.data {
		for _, h := range codes {
			for i := range h {
				t[method] += h[i]
			}
		}
	}
	return t
}

// WriterStatus reports the status for each writer.
type WriterStatus struct {
	// Progress is a slice of completion ratios.  The index of a ratio is its
	// chunk id less one.
	Progress []float64
}

// ReaderStatus reports the status for each reader.
type ReaderStatus struct {
	// Progress is a slice of completion ratios.  The index of a ratio is its
	// chunk id less one.
	Progress []float64
}

// Status returns information about the current state of the client.
func (c *Client) Status() *StatusInfo {
	c.slock.Lock()
	defer c.slock.Unlock()

	si := &StatusInfo{
		Writers:    make(map[string]*WriterStatus),
		Readers:    make(map[string]*ReaderStatus),
		MethodInfo: c.sMethods.dup(),
	}

	for name, w := range c.sWriters {
		si.Writers[name] = w.status()
	}

	for name, r := range c.sReaders {
		si.Readers[name] = r.status()
	}

	return si
}

func (c *Client) addWriter(w *Writer) {
	c.slock.Lock()
	defer c.slock.Unlock()

	if c.sWriters == nil {
		c.sWriters = make(map[string]*Writer)
	}

	c.sWriters[fmt.Sprintf("%s/%s", w.o.b.Name(), w.name)] = w
}

func (c *Client) removeWriter(w *Writer) {
	c.slock.Lock()
	defer c.slock.Unlock()

	if c.sWriters == nil {
		return
	}

	delete(c.sWriters, fmt.Sprintf("%s/%s", w.o.b.Name(), w.name))
}

func (c *Client) addReader(r *Reader) {
	c.slock.Lock()
	defer c.slock.Unlock()

	if c.sReaders == nil {
		c.sReaders = make(map[string]*Reader)
	}

	c.sReaders[fmt.Sprintf("%s/%s", r.o.b.Name(), r.name)] = r
}

func (c *Client) removeReader(r *Reader) {
	c.slock.Lock()
	defer c.slock.Unlock()

	if c.sReaders == nil {
		return
	}

	delete(c.sReaders, fmt.Sprintf("%s/%s", r.o.b.Name(), r.name))
}
