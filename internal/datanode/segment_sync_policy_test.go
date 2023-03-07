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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/tsoutil"
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
			res := policy([]*Segment{segment}, tsoutil.ComposeTSByTime(test.ts, 0))
			assert.Equal(t, test.shouldSyncNum, len(res))
		})
	}
}

func TestSyncMemoryTooHigh(t *testing.T) {
	s1 := &Segment{segmentID: 1, memorySize: 1}
	s2 := &Segment{segmentID: 2, memorySize: 2}
	s3 := &Segment{segmentID: 3, memorySize: 3}
	s4 := &Segment{segmentID: 4, memorySize: 4}
	s5 := &Segment{segmentID: 5, memorySize: 5}

	var baseParams = Params.BaseTable
	baseParams.Save(Params.DataNodeCfg.MemoryForceSyncEnable.Key, "true")
	baseParams.Save(Params.DataNodeCfg.MemoryForceSyncThreshold.Key, "0.0")
	baseParams.Save(Params.DataNodeCfg.MemoryForceSyncSegmentRatio.Key, "0.6")
	policy := syncMemoryTooHigh()
	segs := policy([]*Segment{s3, s4, s2, s1, s5}, 0)
	assert.Equal(t, 3, len(segs))
	assert.Equal(t, int64(5), segs[0])
	assert.Equal(t, int64(4), segs[1])
	assert.Equal(t, int64(3), segs[2])
}
