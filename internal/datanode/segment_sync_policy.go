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

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"go.uber.org/zap"
)

// segmentsSyncPolicy sync policy applies to segments
type segmentSyncPolicy func(segments []*Segment, ts Timestamp) []UniqueID

// syncPeriodically get segmentSyncPolicy with segments sync periodically.
func syncPeriodically() segmentSyncPolicy {
	return func(segments []*Segment, ts Timestamp) []UniqueID {
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
			log.Debug("sync segment periodically",
				zap.Int64s("segmentID", segsToSync))
		}
		return segsToSync
	}
}

// syncMemoryTooHigh force sync the largest segment.
func syncMemoryTooHigh() segmentSyncPolicy {
	return func(segments []*Segment, ts Timestamp) []UniqueID {
		if Params.DataNodeCfg.MemoryForceSyncEnable.GetAsBool() &&
			hardware.GetMemoryUseRatio() >= Params.DataNodeCfg.MemoryForceSyncThreshold.GetAsFloat() &&
			len(segments) >= 1 {
			toSyncSegmentNum := int(math.Max(float64(len(segments))*Params.DataNodeCfg.MemoryForceSyncSegmentRatio.GetAsFloat(), 1.0))
			toSyncSegmentIDs := make([]UniqueID, 0)
			sort.Slice(segments, func(i, j int) bool {
				return segments[i].memorySize > segments[j].memorySize
			})
			for i := 0; i < toSyncSegmentNum; i++ {
				toSyncSegmentIDs = append(toSyncSegmentIDs, segments[i].segmentID)
			}
			log.Debug("sync segment due to memory usage is too high",
				zap.Int64s("toSyncSegmentIDs", toSyncSegmentIDs),
				zap.Int("inputSegmentNum", len(segments)),
				zap.Int("toSyncSegmentNum", len(toSyncSegmentIDs)),
				zap.Float64("memoryUsageRatio", hardware.GetMemoryUseRatio()))
			return toSyncSegmentIDs
		}
		return []UniqueID{}
	}
}
