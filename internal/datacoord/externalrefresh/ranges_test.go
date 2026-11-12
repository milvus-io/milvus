// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package externalrefresh

import (
	"math"
	"math/rand"
	"reflect"
	"testing"
)

func TestExternalRefreshBaseBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name          string
		rows          []int64
		files, budget int64
		want          []int
	}{
		{"legacy_balanced", make([]int64, 10), 6, 0, []int{0, 5, 10}},
		{"empty_snapshot", nil, 10000, 16, []int{0, 0}},
		{"rows", []int64{6, 6, 6, 6}, 10000, 12, []int{0, 2, 4}},
		{"skew", []int64{1, 1, 100, 1, 1}, 10000, 4, []int{0, 2, 3, 5}},
		{"file_boundaries_retained", []int64{1, 1, 1, 1, 1}, 2, 100, []int{0, 2, 4, 5}},
		{"zero_rows", []int64{0, 0, 0}, 10000, 16, []int{0, 3}},
		{"oversized_then_empty", []int64{100, 0}, 10000, 16, []int{0, 1, 2}},
		{"overflow", []int64{math.MaxInt64, math.MaxInt64, 1}, 10000, math.MaxInt64, []int{0, 1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := externalRefreshBaseBoundaries(tc.rows, tc.files, tc.budget)
			if !reflect.DeepEqual(tc.want, got) {
				t.Fatalf("want %v, got %v", tc.want, got)
			}
		})
	}
}

// Check coverage and the row bound independently of the split algorithm.
// One indivisible file may exceed the budget; it must not drag its neighbors
// into that range. Randomized skew also catches arithmetic/boundary mistakes.
func TestExternalRefreshBaseBoundariesProperties(t *testing.T) {
	rng := rand.New(rand.NewSource(55447))
	for iteration := 0; iteration < 2000; iteration++ {
		n := rng.Intn(300) + 1
		rows := make([]int64, n)
		for i := range rows {
			rows[i] = rng.Int63n(1000000) + 1
		}
		files, budget := int64(rng.Intn(50)+1), rng.Int63n(2000000)+1
		bounds := externalRefreshBaseBoundaries(rows, files, budget)
		if bounds[0] != 0 || bounds[len(bounds)-1] != n {
			t.Fatal("incomplete coverage", bounds)
		}
		for i := 1; i < len(bounds); i++ {
			lo, hi := bounds[i-1], bounds[i]
			if hi <= lo || int64(hi-lo) > files {
				t.Fatal("invalid file range", bounds)
			}
			var total int64
			for _, v := range rows[lo:hi] {
				total += v
			}
			if total > budget && hi-lo != 1 {
				t.Fatal("row budget exceeded by multiple files", rows[lo:hi], budget)
			}
		}
	}
}

func TestExternalRefreshLargeParquetWorkload(t *testing.T) {
	rows := make([]int64, 1024)
	for i := range rows {
		rows[i] = 1000000
	}
	legacy := externalRefreshBaseBoundaries(rows, 10000, 0)
	bounded := externalRefreshBaseBoundaries(rows, 10000, 16000000)
	if len(legacy) != 2 || len(bounded) != 65 {
		t.Fatalf("legacy=%d tasks, bounded=%d tasks", len(legacy)-1, len(bounded)-1)
	}
}
