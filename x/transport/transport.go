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

// Package transport provides http.RoundTrippers that may be useful to clients
// of Blazer.
package transport

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func WithFailures(rt http.RoundTripper, opts ...FailureOption) http.RoundTripper {
	o := &options{
		rt: rt,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

type options struct {
	urlSubstrings []string
	failureRate   float64
	status        int
	stall         time.Duration
	rt            http.RoundTripper
	msg           string
	hangAfter     int64
	trg           *triggerReaderGroup
}

func (o *options) doRequest(req *http.Request) (*http.Response, error) {
	if o.trg != nil {
		req.Body = o.trg.new(req.Body)
	}
	resp, err := o.rt.RoundTrip(req)
	if resp != nil && o.trg != nil {
		resp.Body = o.trg.new(resp.Body)
	}
	return resp, err
}

func (o *options) RoundTrip(req *http.Request) (*http.Response, error) {
	if rand.Float64() > o.failureRate {
		return o.doRequest(req)
	}

	var match bool
	for _, ss := range o.urlSubstrings {
		if strings.Contains(req.URL.Path, ss) {
			match = true
			break
		}
	}
	if !match {
		return o.doRequest(req)
	}

	if o.status > 0 {
		resp := &http.Response{
			Status:     fmt.Sprintf("%d %s", o.status, http.StatusText(o.status)),
			StatusCode: o.status,
			Body:       ioutil.NopCloser(strings.NewReader(o.msg)),
		}
		return resp, nil
	}

	if o.stall > 0 {
		ctx := req.Context()
		select {
		case <-time.After(o.stall):
		case <-ctx.Done():
		}
	}
	return o.doRequest(req)
}

type FailureOption func(*options)

func MatchURLSubstring(s string) FailureOption {
	return func(o *options) {
		o.urlSubstrings = append(o.urlSubstrings, s)
	}
}

func FailureRate(rate float64) FailureOption {
	return func(o *options) {
		o.failureRate = rate
	}
}

func Response(status int) FailureOption {
	return func(o *options) {
		o.status = status
	}
}

func Stall(dur time.Duration) FailureOption {
	return func(o *options) {
		o.stall = dur
	}
}

func Body(msg string) FailureOption {
	return func(o *options) {
		o.msg = msg
	}
}

func Trigger(ctx context.Context) FailureOption {
	return func(o *options) {
		go func() {
			<-ctx.Done()
			o.failureRate = 1
		}()
	}
}

func AfterNBytes(bytes int, effect func()) FailureOption {
	return func(o *options) {
		o.trg = &triggerReaderGroup{
			bytes:   int64(bytes),
			trigger: effect,
		}
	}
}

type triggerReaderGroup struct {
	bytes   int64
	trigger func()
	once    sync.Once
}

func (rg *triggerReaderGroup) new(rc io.ReadCloser) io.ReadCloser {
	return &triggerReader{
		ReadCloser: rc,
		bytes:      &rg.bytes,
		trigger:    rg.trigger,
		once:       &rg.once,
	}
}

type triggerReader struct {
	io.ReadCloser
	bytes   *int64
	trigger func()
	once    *sync.Once
}

func (r *triggerReader) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if atomic.AddInt64(r.bytes, -int64(n)) < 0 {
		r.once.Do(r.trigger)
	}
	return n, err
}
