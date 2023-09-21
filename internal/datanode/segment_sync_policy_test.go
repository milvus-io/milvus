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

package datanode

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func TestSyncPeriodically(t *testing.T) {
	t0 := time.Now()

	tests := []struct {
		testName      string
		bufferTs      time.Time
		endPosTs      time.Time
		isBufferEmpty bool
		shouldSyncNum int
	}{
		{"test buffer empty", t0, t0.Add(Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)), true, 0},
		{"test buffer not empty and stale", t0, t0.Add(Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)), false, 1},
		{"test buffer not empty and not stale", t0, t0.Add(Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second) / 2), false, 0},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			policy := syncPeriodically()
			segment := &Segment{}
			segment.setInsertBuffer(&BufferData{
				startPos: &msgpb.MsgPosition{
					Timestamp: tsoutil.ComposeTSByTime(test.bufferTs, 0),
				},
			})
			if test.isBufferEmpty {
				segment.curInsertBuf = nil
			}
			res := policy([]*Segment{segment}, nil, tsoutil.ComposeTSByTime(test.endPosTs, 0))
			assert.Equal(t, test.shouldSyncNum, len(res))
		})
	}
}

func TestSyncMemoryTooHigh(t *testing.T) {
	tests := []struct {
		testName        string
		syncSegmentNum  int
		isHighMemory    bool
		memorySizesInMB []float64
		shouldSyncSegs  []UniqueID
	}{
		{
			"test normal 1", 3, true,
			[]float64{1, 2, 3, 4, 5},
			[]UniqueID{5, 4, 3},
		},
		{
			"test normal 2", 2, true,
			[]float64{1, 2, 3, 4, 5},
			[]UniqueID{5, 4},
		},
		{
			"test normal 3", 5, true,
			[]float64{1, 2, 3, 4, 5},
			[]UniqueID{5, 4, 3, 2, 1},
		},
		{
			"test isHighMemory false", 3, false,
			[]float64{1, 2, 3, 4, 5},
			[]UniqueID{},
		},
		{
			"test syncSegmentNum 1", 1, true,
			[]float64{1, 2, 3, 4, 5},
			[]UniqueID{5},
		},
		{
			"test with small segment", 3, true,
			[]float64{0.1, 0.1, 0.1, 4, 5},
			[]UniqueID{5, 4},
		},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			channel := newChannel("channel", 0, nil, nil, nil)
			channel.setIsHighMemory(test.isHighMemory)
			Params.Save(Params.DataNodeCfg.MemoryForceSyncSegmentNum.Key, fmt.Sprintf("%d", test.syncSegmentNum))
			policy := syncMemoryTooHigh()
			segments := make([]*Segment, len(test.memorySizesInMB))
			for i := range segments {
				segments[i] = &Segment{
					segmentID: UniqueID(i + 1), memorySize: int64(test.memorySizesInMB[i] * 1024 * 1024),
				}
			}
			segs := policy(segments, channel, 0)
			assert.ElementsMatch(t, segs, test.shouldSyncSegs)
		})
	}
}

func TestSyncSegmentsAtTs(t *testing.T) {
	tests := []struct {
		testName      string
		ts            Timestamp
		flushTs       Timestamp
		shouldSyncNum int
	}{
		{"test ts < flushTs", 100, 200, 0},
		{"test ts > flushTs", 300, 200, 1},
		{"test ts = flushTs", 100, 100, 1},
		{"test flushTs = 0", 100, 0, 0},
		{"test flushTs = maxUint64", 100, math.MaxUint64, 0},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			channel := newChannel("channel", 0, nil, nil, nil)
			channel.setFlushTs(test.flushTs)

			segment := &Segment{}
			segment.setInsertBuffer(&BufferData{
				startPos: &msgpb.MsgPosition{},
			})

			policy := syncSegmentsAtTs()
			res := policy([]*Segment{segment}, channel, test.ts)
			assert.Equal(t, test.shouldSyncNum, len(res))
		})
	}
}
