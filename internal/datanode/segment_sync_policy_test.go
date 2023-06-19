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
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestSyncPeriodically(t *testing.T) {
	t0 := time.Now()

	tests := []struct {
		testName      string
		lastTs        time.Time
		ts            time.Time
		isBufferEmpty bool
		shouldSyncNum int
	}{
		{"test buffer empty and stale", t0, t0.Add(Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)), true, 0},
		{"test buffer empty and not stale", t0, t0.Add(Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second) / 2), true, 0},
		{"test buffer not empty and stale", t0, t0.Add(Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)), false, 1},
		{"test buffer not empty and not stale", t0, t0.Add(Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second) / 2), false, 0},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			policy := syncPeriodically()
			segment := &Segment{}
			segment.lastSyncTs = tsoutil.ComposeTSByTime(test.lastTs, 0)
			if !test.isBufferEmpty {
				segment.curInsertBuf = &BufferData{}
			}
			res := policy([]*Segment{segment}, tsoutil.ComposeTSByTime(test.ts, 0), nil)
			assert.Equal(t, test.shouldSyncNum, len(res))
		})
	}
}

func TestSyncMemoryTooHigh(t *testing.T) {
	tests := []struct {
		testName        string
		syncSegmentNum  int
		needToSync      bool
		memorySizesInMB []float64
		shouldSyncSegs  []UniqueID
	}{
		{"test normal 1", 3, true,
			[]float64{1, 2, 3, 4, 5}, []UniqueID{5, 4, 3}},
		{"test normal 2", 2, true,
			[]float64{1, 2, 3, 4, 5}, []UniqueID{5, 4}},
		{"test normal 3", 5, true,
			[]float64{1, 2, 3, 4, 5}, []UniqueID{5, 4, 3, 2, 1}},
		{"test needToSync false", 3, false,
			[]float64{1, 2, 3, 4, 5}, []UniqueID{}},
		{"test syncSegmentNum 1", 1, true,
			[]float64{1, 2, 3, 4, 5}, []UniqueID{5}},
		{"test with small segment", 3, true,
			[]float64{0.1, 0.1, 0.1, 4, 5}, []UniqueID{5, 4}},
	}

	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			var baseParams = &Params.BaseTable
			baseParams.Save(Params.DataNodeCfg.MemoryForceSyncSegmentNum.Key, fmt.Sprintf("%d", test.syncSegmentNum))
			policy := syncMemoryTooHigh()
			segments := make([]*Segment, len(test.memorySizesInMB))
			for i := range segments {
				segments[i] = &Segment{
					segmentID: UniqueID(i + 1), memorySize: int64(test.memorySizesInMB[i] * 1024 * 1024),
				}
			}
			segs := policy(segments, 0, atomic.NewBool(test.needToSync))
			assert.ElementsMatch(t, segs, test.shouldSyncSegs)
		})
	}
}

func TestSyncCpLagBehindTooMuch(t *testing.T) {
	nowTs := tsoutil.ComposeTSByTime(time.Now(), 0)
	laggedTs := tsoutil.AddPhysicalDurationOnTs(nowTs, -2*Params.DataNodeCfg.CpLagPeriod.GetAsDuration(time.Second))
	tests := []struct {
		testName  string
		segments  []*Segment
		idsToSync []int64
	}{
		{"test_current_buf_lag_behind",
			[]*Segment{
				{
					segmentID: 1,
					curInsertBuf: &BufferData{
						startPos: &msgpb.MsgPosition{
							Timestamp: laggedTs,
						},
					},
				},
				{
					segmentID: 2,
					curDeleteBuf: &DelDataBuf{
						startPos: &msgpb.MsgPosition{
							Timestamp: laggedTs,
						},
					},
				},
			},
			[]int64{1, 2},
		},
		{"test_history_buf_lag_behind",
			[]*Segment{
				{
					segmentID: 1,
					historyInsertBuf: []*BufferData{
						{
							startPos: &msgpb.MsgPosition{
								Timestamp: laggedTs,
							},
						},
					},
				},
				{
					segmentID: 2,
					historyDeleteBuf: []*DelDataBuf{
						{
							startPos: &msgpb.MsgPosition{
								Timestamp: laggedTs,
							},
						},
					},
				},
				{
					segmentID: 3,
				},
			},
			[]int64{1, 2},
		},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			policy := syncCPLagTooBehind()
			ids := policy(test.segments, tsoutil.ComposeTSByTime(time.Now(), 0), nil)
			assert.ElementsMatch(t, test.idsToSync, ids)
		})
	}
}
