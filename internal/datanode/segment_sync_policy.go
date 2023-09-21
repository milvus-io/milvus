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

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

const minSyncSize = 0.5 * 1024 * 1024

// segmentsSyncPolicy sync policy applies to segments
type segmentSyncPolicy func(segments []*Segment, c Channel, ts Timestamp) []UniqueID

// syncPeriodically get segmentSyncPolicy with segments sync periodically.
func syncPeriodically() segmentSyncPolicy {
	return func(segments []*Segment, c Channel, ts Timestamp) []UniqueID {
		segmentsToSync := make([]UniqueID, 0)
		for _, seg := range segments {
			endPosTime := tsoutil.PhysicalTime(ts)
			minBufferTime := tsoutil.PhysicalTime(seg.minBufferTs())
			shouldSync := endPosTime.Sub(minBufferTime) >= Params.DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)
			if shouldSync {
				segmentsToSync = append(segmentsToSync, seg.segmentID)
			}
		}
		if len(segmentsToSync) > 0 {
			log.Info("sync segment periodically", zap.Int64s("segmentIDs", segmentsToSync))
		}
		return segmentsToSync
	}
}

// syncMemoryTooHigh force sync the largest segment.
func syncMemoryTooHigh() segmentSyncPolicy {
	return func(segments []*Segment, c Channel, _ Timestamp) []UniqueID {
		if len(segments) == 0 || !c.getIsHighMemory() {
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

// syncSegmentsAtTs returns a new segmentSyncPolicy, sync segments when ts exceeds ChannelMeta.flushTs
func syncSegmentsAtTs() segmentSyncPolicy {
	return func(segments []*Segment, c Channel, ts Timestamp) []UniqueID {
		flushTs := c.getFlushTs()
		if flushTs != 0 && ts >= flushTs {
			segmentsWithBuffer := lo.Filter(segments, func(segment *Segment, _ int) bool {
				return !segment.isBufferEmpty()
			})
			segmentIDs := lo.Map(segmentsWithBuffer, func(segment *Segment, _ int) UniqueID {
				return segment.segmentID
			})
			log.Info("sync segment at ts", zap.Int64s("segmentIDs", segmentIDs),
				zap.Time("ts", tsoutil.PhysicalTime(ts)), zap.Time("flushTs", tsoutil.PhysicalTime(flushTs)))
			return segmentIDs
		}
		return nil
	}
}
