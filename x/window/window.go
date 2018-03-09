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

// Package window provides a type for efficiently (in time) recording events
// seen in a given span of time, with a given resolution.
package window

import (
	"sync"
	"time"
)

// A Window efficiently records events that have occurred over a span of time
// extending from some fixed interval ago to now.  Events that pass beyond this
// horizon effectively "fall off" the back of the window.
type Window struct {
	mu     sync.Mutex
	events []interface{}
	res    time.Duration
	last   time.Time
	reduce ReduceFunc
}

// A ReduceFunc should take two values from the window and combine them into a
// third value that will be stored in the window.  The values i or j may be
// nil.
type ReduceFunc func(i, j interface{}) interface{}

// New returns an initialized window for events over the given duration at the
// given resolution.  Windows with tight resolution (i.e., small values for
// that argument) will be more accurate, at the cost of some memory.
func New(size, resolution time.Duration, reduce ReduceFunc) *Window {
	return &Window{
		res:    resolution,
		events: make([]interface{}, size/resolution),
		reduce: reduce,
	}
}

func (w *Window) bucket(now time.Time) int {
	nanos := now.UnixNano()
	abs := nanos / int64(w.res)
	return int(abs) % len(w.events)
}

// sweep keeps the counter valid.  It needs to be called from every method that
// views or updates the counter, and the caller needs to hold the mutex.
func (w *Window) sweep(now time.Time) {
	defer func() {
		w.last = now
	}()

	b := w.bucket(now)
	p := w.bucket(w.last)

	if b == p && now.Sub(w.last) <= w.res {
		// We're in the same bucket as the previous sweep, so all buckets are
		// valid.
		return
	}

	if now.Sub(w.last) > w.res*time.Duration(len(w.events)) {
		// We've gone longer than this counter measures since the last sweep, just
		// zero the thing and have done.
		for i := range w.events {
			w.events[i] = nil
		}
		return
	}

	// Expire all invalid buckets.  This means buckets not seen since the
	// previous sweep and now, including the current bucket but not including the
	// previous bucket.
	old := int(w.last.UnixNano()) / int(w.res)
	new := int(now.UnixNano()) / int(w.res)
	for i := old + 1; i <= new; i++ {
		b := i % len(w.events)
		w.events[b] = nil
	}
}

// Insert adds the given event.
func (w *Window) Insert(e interface{}) {
	w.addAt(time.Now(), e)
}

func (w *Window) addAt(t time.Time, e interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sweep(t)
	w.events[w.bucket(t)] = w.reduce(w.events[w.bucket(t)], e)
}

// Reduce runs the windows reducer over the valid values and returns the
// result.
func (w *Window) Reduce() interface{} {
	return w.reducedAt(time.Now())
}

func (w *Window) reducedAt(t time.Time) interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sweep(t)
	var n interface{}
	for i := range w.events {
		n = w.reduce(n, w.events[i])
	}
	return n
}
