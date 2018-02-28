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

package b2

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

func TestHistogram(t *testing.T) {
	table := []struct {
		ds   []time.Duration
		want []int
	}{
		{
			ds: []time.Duration{
				1 * time.Microsecond,
				1001 * time.Microsecond,
				3001 * time.Microsecond,
				7001 * time.Microsecond,
				15001 * time.Microsecond,
			},
			want: []int{
				1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
			},
		},
		{
			ds: []time.Duration{
				0,
				1 * time.Microsecond,
				999 * time.Microsecond,
				1000 * time.Microsecond,
				1001 * time.Microsecond,
				2999 * time.Microsecond,
				3000 * time.Microsecond,
				3001 * time.Microsecond,
				6999 * time.Microsecond,
				7000 * time.Microsecond,
				7001 * time.Microsecond,
				14999 * time.Microsecond,
				15000 * time.Microsecond,
				15001 * time.Microsecond,
				time.Hour,
			},
			want: []int{
				3, 3, 3, 3, 2, 0, 0, 0, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
			},
		},
	}

	for _, e := range table {
		mi := &MethodInfo{}
		for i, d := range e.ds {
			mi.addCall(fmt.Sprintf("%d", i%3), d, i%4)
		}
		got := mi.Histogram()
		if !reflect.DeepEqual(got, e.want) {
			t.Errorf("Histogram(%v): got %v, want %v", e.ds, got, e.want)
		}
	}
}
