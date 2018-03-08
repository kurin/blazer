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

package counter

import (
	"testing"
	"time"
)

func TestCounts(t *testing.T) {
	table := []struct {
		size, dur time.Duration
		incs      []time.Time
		look      time.Time
		want      int
	}{
		{
			size: time.Minute,
			dur:  time.Second,
			incs: []time.Time{
				// year, month, day, hour, min, sec, nano
				time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2000, 1, 1, 0, 0, 1, 0, time.UTC),
				time.Date(2000, 1, 1, 0, 0, 2, 0, time.UTC),
				time.Date(2000, 1, 1, 0, 0, 3, 0, time.UTC),
				time.Date(2000, 1, 1, 0, 0, 4, 0, time.UTC),
				time.Date(2000, 1, 1, 0, 0, 5, 0, time.UTC),
			},
			look: time.Date(2000, 1, 1, 0, 1, 0, 0, time.UTC),
			want: 5,
		},
	}

	for _, e := range table {
		c := New(e.size, e.dur)
		for _, inc := range e.incs {
			c.incAt(inc, 1)
		}
		ct := c.countAt(e.look)
		if ct != e.want {
			t.Errorf("countAt(%v) got %d, want %d", e.look, ct, e.want)
		}
	}
}
