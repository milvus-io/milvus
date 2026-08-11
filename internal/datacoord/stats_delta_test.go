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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestAddStatsDelta(t *testing.T) {
	cases := []struct {
		name   string
		base   *datapb.Statistics
		delta  *datapb.Statistics
		expect *datapb.Statistics
	}{
		{
			name:   "nil delta returns base unchanged",
			base:   &datapb.Statistics{InsertBinlogSize: 100, InsertBinlogCount: 2},
			delta:  nil,
			expect: &datapb.Statistics{InsertBinlogSize: 100, InsertBinlogCount: 2},
		},
		{
			name:   "nil base adopts delta values",
			base:   nil,
			delta:  &datapb.Statistics{InsertBinlogSize: 50, InsertBinlogCount: 1, StatsBinlogSize: 7},
			expect: &datapb.Statistics{InsertBinlogSize: 50, InsertBinlogCount: 1, StatsBinlogSize: 7},
		},
		{
			name: "additive scalars accumulate",
			base: &datapb.Statistics{
				InsertBinlogSize: 1000, InsertBinlogCount: 4, StatsBinlogSize: 20,
			},
			delta: &datapb.Statistics{
				InsertBinlogSize: 300, InsertBinlogCount: 2, StatsBinlogSize: 5,
			},
			expect: &datapb.Statistics{
				InsertBinlogSize: 1300, InsertBinlogCount: 6, StatsBinlogSize: 25,
			},
		},
		{
			name: "delta and timestamp fields are never touched",
			base: &datapb.Statistics{
				InsertBinlogSize:   1000,
				DeltaBinlogSize:    77,
				DeltaBinlogCount:   3,
				DeleteNumRows:      9,
				TimestampFrom:      10,
				TimestampTo:        50,
				DeltaTimestampFrom: 11,
				DeltaTimestampTo:   49,
				TimestampQuantiles: []int64{1, 2, 3, 4, 5},
			},
			// A malformed increment carrying these fields must be ignored.
			delta: &datapb.Statistics{
				InsertBinlogSize:   1,
				DeltaBinlogSize:    999,
				DeltaBinlogCount:   999,
				DeleteNumRows:      999,
				TimestampFrom:      999,
				TimestampTo:        999,
				DeltaTimestampFrom: 999,
				DeltaTimestampTo:   999,
				TimestampQuantiles: []int64{9, 9, 9, 9, 9},
			},
			expect: &datapb.Statistics{
				InsertBinlogSize:   1001,
				DeltaBinlogSize:    77,
				DeltaBinlogCount:   3,
				DeleteNumRows:      9,
				TimestampFrom:      10,
				TimestampTo:        50,
				DeltaTimestampFrom: 11,
				DeltaTimestampTo:   49,
				TimestampQuantiles: []int64{1, 2, 3, 4, 5},
			},
		},
		{
			name: "null counts union: new field added, existing field accumulates",
			base: &datapb.Statistics{
				NullCounts: map[int64]int64{100: 0, 101: 5},
			},
			delta: &datapb.Statistics{
				NullCounts: map[int64]int64{101: 2, 102: 0},
			},
			expect: &datapb.Statistics{
				NullCounts: map[int64]int64{100: 0, 101: 7, 102: 0},
			},
		},
		{
			name:   "negative increment clamps at zero",
			base:   &datapb.Statistics{InsertBinlogSize: 10, InsertBinlogCount: 1, StatsBinlogSize: 3},
			delta:  &datapb.Statistics{InsertBinlogSize: -50, InsertBinlogCount: -9, StatsBinlogSize: -8},
			expect: &datapb.Statistics{InsertBinlogSize: 0, InsertBinlogCount: 0, StatsBinlogSize: 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := addStatsDelta(context.TODO(), 1, tc.base, tc.delta)
			assert.Equal(t, tc.expect.GetInsertBinlogSize(), got.GetInsertBinlogSize())
			assert.Equal(t, tc.expect.GetInsertBinlogCount(), got.GetInsertBinlogCount())
			assert.Equal(t, tc.expect.GetStatsBinlogSize(), got.GetStatsBinlogSize())
			assert.Equal(t, tc.expect.GetDeltaBinlogSize(), got.GetDeltaBinlogSize())
			assert.Equal(t, tc.expect.GetDeltaBinlogCount(), got.GetDeltaBinlogCount())
			assert.Equal(t, tc.expect.GetDeleteNumRows(), got.GetDeleteNumRows())
			assert.Equal(t, tc.expect.GetTimestampFrom(), got.GetTimestampFrom())
			assert.Equal(t, tc.expect.GetTimestampTo(), got.GetTimestampTo())
			assert.Equal(t, tc.expect.GetDeltaTimestampFrom(), got.GetDeltaTimestampFrom())
			assert.Equal(t, tc.expect.GetDeltaTimestampTo(), got.GetDeltaTimestampTo())
			assert.Equal(t, tc.expect.GetTimestampQuantiles(), got.GetTimestampQuantiles())
			assert.Equal(t, tc.expect.GetNullCounts(), got.GetNullCounts())
		})
	}
}

// TestAddStatsDeltaDoesNotAliasBase pins that the returned Statistics is
// independent of the input, so a caller mutating the result cannot corrupt
// the segment's previous state.
func TestAddStatsDeltaDoesNotAliasBase(t *testing.T) {
	base := &datapb.Statistics{
		InsertBinlogSize: 100,
		NullCounts:       map[int64]int64{100: 1},
	}
	got := addStatsDelta(context.TODO(), 1, base, &datapb.Statistics{
		InsertBinlogSize: 10,
		NullCounts:       map[int64]int64{101: 0},
	})

	got.InsertBinlogSize = 999
	got.NullCounts[100] = 999

	assert.EqualValues(t, 100, base.GetInsertBinlogSize())
	assert.EqualValues(t, 1, base.GetNullCounts()[100])
}

// TestAddStatsDeltaDoesNotAliasBaseNilDelta pins that the nil-delta
// pass-through also returns an object independent of base, not base itself
// — a caller mutating the result on this path must not corrupt the
// segment's previous state either.
func TestAddStatsDeltaDoesNotAliasBaseNilDelta(t *testing.T) {
	base := &datapb.Statistics{
		InsertBinlogSize: 100,
		NullCounts:       map[int64]int64{100: 1},
	}
	got := addStatsDelta(context.TODO(), 1, base, nil)

	got.InsertBinlogSize = 999
	got.NullCounts[100] = 999

	assert.EqualValues(t, 100, base.GetInsertBinlogSize())
	assert.EqualValues(t, 1, base.GetNullCounts()[100])
}
