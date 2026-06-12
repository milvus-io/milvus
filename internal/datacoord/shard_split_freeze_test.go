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
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/task"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newFreezeTestInspector(t *testing.T) (*compactionInspector, *MockCompactionMeta) {
	paramtable.Init()
	mockMeta := NewMockCompactionMeta(t)
	mockAlloc := allocator.NewMockAllocator(t)
	mockScheduler := task.NewMockGlobalScheduler(t)
	mockScheduler.EXPECT().Enqueue(mock.Anything).Return().Maybe()
	return newCompactionInspector(mockMeta, mockAlloc, nil, mockScheduler, mockScheduler, newMockVersionManager()), mockMeta
}

func TestEnqueueCompactionRejectedOnSplittingChannel(t *testing.T) {
	inspector, _ := newFreezeTestInspector(t)
	inspector.setChannelSplittingChecker(func(channel string) bool { return channel == "v0" })

	// a compaction on the splitting channel is rejected before any task is created.
	err := inspector.enqueueCompaction(&datapb.CompactionTask{
		TriggerID: 1,
		PlanID:    1,
		Channel:   "v0",
		Type:      datapb.CompactionType_MixCompaction,
	})
	assert.ErrorIs(t, err, merr.ErrCompactionPlanConflict)
	assert.Nil(t, inspector.getCompactionTask(1))
}

func TestPreemptTasksByChannel(t *testing.T) {
	inspector, mockMeta := newFreezeTestInspector(t)
	// enqueue two tasks: one on the splitting channel, one on another channel.
	mockMeta.EXPECT().CheckAndSetSegmentsCompacting(mock.Anything, mock.Anything).Return(true, true).Times(2)
	mockMeta.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	assert.NoError(t, inspector.enqueueCompaction(&datapb.CompactionTask{
		TriggerID: 1, PlanID: 1, Channel: "v0", Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{100},
	}))
	assert.NoError(t, inspector.enqueueCompaction(&datapb.CompactionTask{
		TriggerID: 1, PlanID: 2, Channel: "v9", Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{200},
	}))

	// the preemption cleans the v0 task: the cleaned state is persisted and
	// the compacting flags of its input segments are released.
	mockMeta.EXPECT().SetSegmentsCompacting(mock.Anything, []int64{100}, false).Return().Once()
	inspector.preemptTasksByChannel("v0")

	assert.Nil(t, inspector.getCompactionTask(1))
	assert.NotNil(t, inspector.getCompactionTask(2))

	// idempotent: a second preemption finds nothing.
	inspector.preemptTasksByChannel("v0")
}

func TestGetRealSegmentsForSplitFamily(t *testing.T) {
	paramtable.Init()
	// segments live on the source and on one target after a partial relabel.
	m := newSplitTestMeta(true, "v0", map[int64]int64{10: 10, 11: 20})
	m.segments.SetSegment(2000, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID: 2000, CollectionID: 1, PartitionID: 12, InsertChannel: "v1",
			State: commonpb.SegmentState_Flushed,
		},
	})

	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListSplitShardTask(mock.Anything).Return([]*datapb.SplitShardTask{{
		TaskId:         100,
		CollectionId:   1,
		SourceVchannel: "v0",
		State:          datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: "v1"},
			{Vchannel: "v2"},
		},
	}}, nil).Once()
	manager, err := newShardSplitManager(context.Background(), m, catalog, allocator.NewMockAllocator(t), nil, nil, nil)
	assert.NoError(t, err)

	handler := &ServerHandler{s: &Server{meta: m, shardSplitManager: manager}}

	// the source channel reports the union of its remaining segments and the
	// segments already relabeled to the targets.
	segments := handler.getRealSegmentsForSplitFamily("v0")
	assert.Len(t, segments, 3)
	// a non-splitting channel reports only its own segments.
	segments = handler.getRealSegmentsForSplitFamily("v1")
	assert.Len(t, segments, 1)

	// without the split manager the behavior is unchanged.
	handler = &ServerHandler{s: &Server{meta: m}}
	segments = handler.getRealSegmentsForSplitFamily("v0")
	assert.Len(t, segments, 2)
}
