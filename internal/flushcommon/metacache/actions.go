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

package metacache

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type segmentCriterion struct {
	ids    typeutil.Set[int64]
	states typeutil.Set[commonpb.SegmentState]
	others []SegmentFilter
}

func (sc *segmentCriterion) Match(segment *SegmentInfo) bool {
	for _, filter := range sc.others {
		if !filter.Filter(segment) {
			return false
		}
	}
	return true
}

type SegmentFilter interface {
	Filter(info *SegmentInfo) bool
	AddFilter(*segmentCriterion)
}

// SegmentIDFilter segment filter with segment ids.
type SegmentIDFilter struct {
	ids typeutil.Set[int64]
}

func (f *SegmentIDFilter) Filter(info *SegmentInfo) bool {
	return f.ids.Contain(info.segmentID)
}

func (f *SegmentIDFilter) AddFilter(criterion *segmentCriterion) {
	criterion.ids = f.ids
}

func WithSegmentIDs(segmentIDs ...int64) SegmentFilter {
	set := typeutil.NewSet(segmentIDs...)
	return &SegmentIDFilter{
		ids: set,
	}
}

// SegmentStateFilter segment filter with segment states.
type SegmentStateFilter struct {
	states typeutil.Set[commonpb.SegmentState]
}

func (f *SegmentStateFilter) Filter(info *SegmentInfo) bool {
	return f.states.Contain(info.State())
}

func (f *SegmentStateFilter) AddFilter(criterion *segmentCriterion) {
	criterion.states = f.states
}

func WithSegmentState(states ...commonpb.SegmentState) SegmentFilter {
	set := typeutil.NewSet(states...)
	return &SegmentStateFilter{
		states: set,
	}
}

// SegmentFilterFunc implements segment filter with other filters logic.
type SegmentFilterFunc func(info *SegmentInfo) bool

func (f SegmentFilterFunc) Filter(info *SegmentInfo) bool {
	return f(info)
}

func (f SegmentFilterFunc) AddFilter(criterion *segmentCriterion) {
	criterion.others = append(criterion.others, f)
}

func WithPartitionID(partitionID int64) SegmentFilter {
	return SegmentFilterFunc(func(info *SegmentInfo) bool {
		return partitionID == common.AllPartitionsID || info.partitionID == partitionID
	})
}

func WithPartitionIDs(partitionIDs []int64) SegmentFilter {
	return SegmentFilterFunc(func(info *SegmentInfo) bool {
		idSet := typeutil.NewSet(partitionIDs...)
		return idSet.Contain(info.partitionID)
	})
}

func WithStartPosNotRecorded() SegmentFilter {
	return SegmentFilterFunc(func(info *SegmentInfo) bool {
		return !info.startPosRecorded && info.startPosition != nil
	})
}

func WithLevel(level datapb.SegmentLevel) SegmentFilter {
	return SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.level == level
	})
}

func WithNoSyncingTask() SegmentFilter {
	return SegmentFilterFunc(func(info *SegmentInfo) bool {
		return info.syncingTasks == 0
	})
}

type SegmentAction func(info *SegmentInfo)

func UpdateBinlogs(binlogs []*datapb.FieldBinlog) SegmentAction {
	return func(info *SegmentInfo) {
		info.binlogs = binlogs
	}
}

func UpdateStatslogs(statslogs []*datapb.FieldBinlog) SegmentAction {
	return func(info *SegmentInfo) {
		info.statslogs = statslogs
	}
}

func UpdateDeltalogs(deltalogs []*datapb.FieldBinlog) SegmentAction {
	return func(info *SegmentInfo) {
		info.deltalogs = deltalogs
	}
}

func UpdateBm25logs(bm25logs []*datapb.FieldBinlog) SegmentAction {
	return func(info *SegmentInfo) {
		info.bm25logs = bm25logs
	}
}

func UpdateState(state commonpb.SegmentState) SegmentAction {
	return func(info *SegmentInfo) {
		info.state = state
	}
}

func UpdateCheckpoint(checkpoint *msgpb.MsgPosition) SegmentAction {
	return func(info *SegmentInfo) {
		info.checkpoint = checkpoint
	}
}

func UpdateNumOfRows(numOfRows int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.flushedRows = numOfRows
	}
}

func SetStartPositionIfNil(startPos *msgpb.MsgPosition) SegmentAction {
	return func(info *SegmentInfo) {
		if info.startPosition == nil {
			info.startPosition = startPos
		}
	}
}

func SetStorageVersion(version int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.storageVersion = version
	}
}

func UpdateBufferedRows(bufferedRows int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.bufferRows = bufferedRows
	}
}

func RollStats(newStats ...*storage.PrimaryKeyStats) SegmentAction {
	return func(info *SegmentInfo) {
		info.bfs.Roll(newStats...)
	}
}

func MergeBm25Stats(newStats map[int64]*storage.BM25Stats) SegmentAction {
	return func(info *SegmentInfo) {
		if info.bm25stats == nil {
			info.bm25stats = NewEmptySegmentBM25Stats()
		}
		info.bm25stats.Merge(newStats)
	}
}

func SetStatistics(stats *SegmentStats) SegmentAction {
	return func(info *SegmentInfo) {
		if stats != nil {
			info.stats = stats
		}
	}
}

func StartSyncing(batchSize int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.syncingRows += batchSize
		info.bufferRows -= batchSize
		info.syncingTasks++
	}
}

func AbortSyncing(batchSize int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.syncingRows -= batchSize
		info.bufferRows += batchSize
		info.syncingTasks--
	}
}

// DiscardSyncing removes a task's syncing ownership without pretending that
// its payload was restored to the write buffer. Use AbortSyncing only when the
// exact batch is actually put back into a buffer.
func DiscardSyncing(batchSize int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.syncingRows -= batchSize
		info.syncingTasks--
	}
}

func FinishSyncing(batchSize int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.flushedRows += batchSize
		info.syncingRows -= batchSize
		info.syncingTasks--
	}
}

// SetLastFlushPosition records how far this segment has been flushed, as a WAL
// position. Applied in the same commit transaction that publishes the flush, so
// the fence advances only once the data it names is durable.
func SetLastFlushPosition(position *msgpb.MsgPosition) SegmentAction {
	return func(info *SegmentInfo) {
		if position == nil {
			return
		}
		if info.lastFlushPosition == nil || position.GetTimestamp() > info.lastFlushPosition.GetTimestamp() {
			info.lastFlushPosition = position
		}
	}
}

// SetPendingFlushCheckpointIfNil records the WAL position a replay must resume
// from to regenerate this segment's outstanding flush obligation. Applied when
// the segment is sealed, so the channel checkpoint cannot advance past the fence
// that sealed it before the resulting flush is committed.
//
// Set-if-nil, NOT max: a re-seal of an already sealed segment must never push
// the pin later. The earliest un-committed fence is the one recovery needs.
func SetPendingFlushCheckpointIfNil(position *msgpb.MsgPosition) SegmentAction {
	return func(info *SegmentInfo) {
		if position == nil {
			return
		}
		if info.pendingFlushCheckpoint == nil {
			info.pendingFlushCheckpoint = position
		}
	}
}

func UpdateCurrentSplit(split []storagecommon.ColumnGroup) SegmentAction {
	return func(info *SegmentInfo) {
		info.currentSplit = split
	}
}

// SetCurrentSplitIfNil fixes a segment's physical column layout before
// concurrent storage prepares start. The split is derived state: installing it
// early does not advance the segment checkpoint, and a WAL replay can derive it
// again if the process exits before metadata commit.
func SetCurrentSplitIfNil(split []storagecommon.ColumnGroup) SegmentAction {
	return func(info *SegmentInfo) {
		if info.currentSplit == nil {
			info.currentSplit = split
		}
	}
}

func SetStartPosRecorded(flag bool) SegmentAction {
	return func(info *SegmentInfo) {
		info.startPosRecorded = flag
	}
}

func UpdateManifestPath(manifestPath string) SegmentAction {
	return func(info *SegmentInfo) {
		info.manifestPath = manifestPath
	}
}

// SetFlushSourceMode records which subsystem owns the segment's payload at
// flush time. The decision is sticky: once a non-Unknown mode is set, later
// calls with a different mode are no-ops, so the source for a given segment
// stays consistent across its lifetime.
func SetFlushSourceMode(mode FlushSourceMode) SegmentAction {
	return func(info *SegmentInfo) {
		if info.flushSourceMode == FlushSourceUnknown {
			info.flushSourceMode = mode
		}
	}
}

// MergeSegmentAction is the util function to merge multiple SegmentActions into one.
func MergeSegmentAction(actions ...SegmentAction) SegmentAction {
	return func(info *SegmentInfo) {
		for _, action := range actions {
			action(info)
		}
	}
}
