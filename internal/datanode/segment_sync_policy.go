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
	"math"
	"sort"
	"time"

	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

const minSyncSize = 0.5 * 1024 * 1024

// segmentsSyncPolicy sync policy applies to segments
type segmentSyncPolicy func(segments []*Segment, ts Timestamp, needToSync *atomic.Bool) []UniqueID

// syncPeriodically get segmentSyncPolicy with segments sync periodically.
func syncPeriodically() segmentSyncPolicy {
	return func(segments []*Segment, ts Timestamp, _ *atomic.Bool) []UniqueID {
		segsToSync := make([]UniqueID, 0)
		for _, seg := range segments {
			endTime := tsoutil.PhysicalTime(ts)
			lastSyncTime := tsoutil.PhysicalTime(seg.lastSyncTs)
			shouldSync := endTime.Sub(lastSyncTime) >= Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second) && !seg.isBufferEmpty()
			if shouldSync {
				segsToSync = append(segsToSync, seg.segmentID)
			}
		}
		if len(segsToSync) > 0 {
			log.Info("sync segment periodically", zap.Int64s("segmentID", segsToSync))
		}
		return segsToSync
	}
}

// syncMemoryTooHigh force sync the largest segment.
func syncMemoryTooHigh() segmentSyncPolicy {
	return func(segments []*Segment, ts Timestamp, needToSync *atomic.Bool) []UniqueID {
		if len(segments) == 0 || !needToSync.Load() {
			return nil
		}
		sort.Slice(segments, func(i, j int) bool {
			return segments[i].memorySize > segments[j].memorySize
		})
		syncSegments := make([]UniqueID, 0)
		syncSegmentsNum := math.Min(float64(Params.DataNodeCfg.MemoryForceSyncSegmentNum.GetAsInt()), float64(len(segments)))
		for i := 0; i < int(syncSegmentsNum); i++ {
			if segments[i].memorySize < minSyncSize { // prevent generating too many small binlogs
				break
			}
			syncSegments = append(syncSegments, segments[i].segmentID)
			log.Info("sync segment due to memory usage is too high",
				zap.Int64("segmentID", segments[i].segmentID),
				zap.Int64("memorySize", segments[i].memorySize))
		}
		return syncSegments
	}
}

// syncCPLagTooBehind force sync the segments lagging too behind the channel checkPoint
func syncCPLagTooBehind() segmentSyncPolicy {
	segmentMinTs := func(segment *Segment) uint64 {
		var minTs uint64 = math.MaxUint64
		if segment.curInsertBuf != nil && segment.curInsertBuf.startPos != nil && segment.curInsertBuf.startPos.Timestamp < minTs {
			minTs = segment.curInsertBuf.startPos.Timestamp
		}
		if segment.curDeleteBuf != nil && segment.curDeleteBuf.startPos != nil && segment.curDeleteBuf.startPos.Timestamp < minTs {
			minTs = segment.curDeleteBuf.startPos.Timestamp
		}
		for _, ib := range segment.historyInsertBuf {
			if ib != nil && ib.startPos != nil && ib.startPos.Timestamp < minTs {
				minTs = ib.startPos.Timestamp
			}
		}
		for _, db := range segment.historyDeleteBuf {
			if db != nil && db.startPos != nil && db.startPos.Timestamp < minTs {
				minTs = db.startPos.Timestamp
			}
		}
		return minTs
	}

	return func(segments []*Segment, ts Timestamp, _ *atomic.Bool) []UniqueID {
		segmentsToSync := make([]UniqueID, 0)
		for _, segment := range segments {
			segmentMinTs := segmentMinTs(segment)
			segmentStartTime := tsoutil.PhysicalTime(segmentMinTs)
			cpLagDuration := tsoutil.PhysicalTime(ts).Sub(segmentStartTime)
			shouldSync := cpLagDuration > Params.DataNodeCfg.CpLagPeriod.GetAsDuration(time.Second) && !segment.isBufferEmpty()
			if shouldSync {
				segmentsToSync = append(segmentsToSync, segment.segmentID)
			}
		}
		if len(segmentsToSync) > 0 {
			log.Info("sync segment for cp lag behind too much",
				zap.Int64s("segmentID", segmentsToSync))
		}
		return segmentsToSync
	}
}
