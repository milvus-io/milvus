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
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
		return !info.startPosRecorded
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

func SegmentActions(actions ...SegmentAction) SegmentAction {
	return func(info *SegmentInfo) {
		for _, act := range actions {
			act(info)
		}
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

func StartSyncing(batchSize int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.syncingRows += batchSize
		info.bufferRows -= batchSize
		info.syncingTasks++
	}
}

func FinishSyncing(batchSize int64) SegmentAction {
	return func(info *SegmentInfo) {
		info.flushedRows += batchSize
		info.syncingRows -= batchSize
		info.syncingTasks--
	}
}

func SetStartPosRecorded(flag bool) SegmentAction {
	return func(info *SegmentInfo) {
		info.startPosRecorded = flag
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
