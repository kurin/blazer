// Copyright 2018, Google
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

// Package counter provides a type for efficiently computing the number of
// events seen in a given span of time, with a given resolution.
package counter

import (
	"sync"
	"time"
)

// A Counter efficiently counts the number of events that have occurred over a
// span of time extending from some fixed interval ago to now.  Events that
// pass beyond this horizon effectively "fall off" the back of the counter, and
// do not appear in the count.
type Counter struct {
	mu    sync.Mutex
	count []int
	res   time.Duration
	last  time.Time
}

// New returns an initialized counter for events over the given duration at the
// given resolution.  Counters with tight resolution (i.e., small values for
// that argument) will be more accurate, at the cost of some memory.
func New(duration, resolution time.Duration) *Counter {
	return &Counter{
		res:   resolution,
		count: make([]int, duration/resolution),
	}
}

func (c *Counter) bucket(now time.Time) int {
	nanos := now.UnixNano()
	abs := nanos / int64(c.res)
	return int(abs) % len(c.count)
}

// sweep keeps the counter valid.  It needs to be called from every method that
// views or updates the counter, and the caller needs to hold the mutex.
func (c *Counter) sweep(now time.Time) {
	defer func() {
		c.last = now
	}()

	b := c.bucket(now)
	p := c.bucket(c.last)

	if b == p && now.Sub(c.last) <= c.res {
		// We're in the same bucket as the previous sweep, so all buckets are
		// valid.
		return
	}

	if now.Sub(c.last) > c.res*time.Duration(len(c.count)) {
		// We've gone longer than this counter measures since the last sweep, just
		// zero the thing and have done.
		for i := range c.count {
			c.count[i] = 0
		}
		return
	}

	// Expire all invalid buckets.  This means buckets not seen since the
	// previous sweep and now, including the current bucket but not including the
	// previous bucket.
	old := int(c.last.UnixNano()) / int(c.res)
	new := int(now.UnixNano()) / int(c.res)
	for i := old + 1; i <= new; i++ {
		c.count[i%len(c.count)] = 0
	}
}

// Inc increases the counter by n.  Only positive n has an effect.
func (c *Counter) Inc(n int) {
	if n <= 0 {
		return
	}
	c.incAt(time.Now(), n)
}

func (c *Counter) incAt(t time.Time, n int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sweep(t)
	c.count[c.bucket(t)] += n
}

// Count returns the current value of the counter.  The current value is the
// cumulative value of all the calls to Inc over the period for which Counter
// retains data.
func (c *Counter) Count() int {
	return c.countAt(time.Now())
}

func (c *Counter) countAt(t time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sweep(t)
	var n int
	for i := range c.count {
		n += c.count[i]
	}
	return n
}
