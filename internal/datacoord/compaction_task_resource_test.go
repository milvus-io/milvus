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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCompactionTaskResource_Mix(t *testing.T) {
	paramtable.Init()
	task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10}},
		nil, NewMockCompactionMeta(t), newMockVersionManager())
	assert.Equal(t, mixCompactionTaskResource(), task.GetTaskResource())
}

func TestCompactionTaskResource_Sort(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	calls := 0
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).RunAndReturn(func(ctx context.Context, id int64) *SegmentInfo {
		calls++
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: 10, NumOfRows: 100,
			Stats: &datapb.Statistics{InsertBinlogSize: 3 * testGiB},
		}}
	})
	task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}},
		nil, meta, newMockVersionManager())
	assert.Equal(t, statsTaskResource(3*testGiB), task.GetTaskResource())
	// Cached: the second call does not walk meta again.
	assert.Equal(t, statsTaskResource(3*testGiB), task.GetTaskResource())
	assert.Equal(t, 1, calls)
}

func TestCompactionTaskResource_SortSegmentMissing(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(nil).Twice()
	task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}},
		nil, meta, newMockVersionManager())
	// Not resolvable: floor, and NOT cached so the next round retries.
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
}

// TestCompactionTaskResource_SortUnsizedSegment covers a sort compaction whose
// input segment is present but carries no size at all: still the floor, still
// not cached.
func TestCompactionTaskResource_SortUnsizedSegment(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).
		Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 10}}).Twice()
	task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}},
		nil, meta, newMockVersionManager())
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
}

// TestCompactionTaskResource_SortNoInput covers the malformed-task shape.
func TestCompactionTaskResource_SortNoInput(t *testing.T) {
	paramtable.Init()
	task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction},
		nil, NewMockCompactionMeta(t), newMockVersionManager())
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
}

func TestCompactionTaskResource_L0(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 10, Stats: &datapb.Statistics{DeltaBinlogSize: 300 * testMiB},
	}}).Once()
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(11)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID: 11, Stats: &datapb.Statistics{DeltaBinlogSize: 200 * testMiB},
	}}).Once()
	task := newL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10, 11}}, nil, meta)
	assert.Equal(t, l0CompactionTaskResource(500*testMiB), task.GetTaskResource())
	assert.Equal(t, l0CompactionTaskResource(500*testMiB), task.GetTaskResource()) // cached
}

func TestCompactionTaskResource_L0SegmentMissing(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(nil)
	task := newL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10}}, nil, meta)
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
}

func TestCompactionTaskResource_ClusteringAndBump(t *testing.T) {
	paramtable.Init()
	clustering := newClusteringCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_ClusteringCompaction},
		nil, NewMockCompactionMeta(t), nil, nil, newMockVersionManager())
	assert.Equal(t, clusteringCompactionTaskResource(), clustering.GetTaskResource())

	bump := newBumpSchemaVersionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_BumpSchemaVersionCompaction},
		nil, NewMockCompactionMeta(t), newMockVersionManager())
	assert.Equal(t, mixCompactionTaskResource(), bump.GetTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 1024 * testMiB}, bump.GetTaskResource())
}

// TestCompactionTaskResource_RequestCarriesEstimate proves the plan the worker
// receives ships exactly what GetTaskResource priced, for every compaction
// family whose plan is buildable without a full meta.
func TestCompactionTaskResource_RequestCarriesEstimate(t *testing.T) {
	paramtable.Init()

	newSegment := func(segID int64) *SegmentInfo {
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
			ID: segID, NumOfRows: 10, InsertChannel: "ch-1",
			State: commonpb.SegmentState_Flushed,
			Stats: &datapb.Statistics{InsertBinlogSize: 1024, DeltaBinlogSize: 1024},
		}}
	}
	newAlloc := func(t *testing.T) allocator.Allocator {
		alloc := allocator.NewMockAllocator(t)
		alloc.EXPECT().AllocN(mock.Anything).Return(100, 200, nil).Maybe()
		return alloc
	}

	t.Run("mix", func(t *testing.T) {
		meta := NewMockCompactionMeta(t)
		meta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *SegmentInfo { return newSegment(segID) })
		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10},
			Schema: &schemapb.CollectionSchema{Version: 1},
		}, newAlloc(t), meta, newMockVersionManager())
		plan, err := task.BuildCompactionRequest()
		assert.NoError(t, err)
		assert.Equal(t, task.GetTaskResource(), taskcommon.Resource{CPU: plan.GetCpu(), Memory: plan.GetMemory()})
	})

	t.Run("sort", func(t *testing.T) {
		meta := NewMockCompactionMeta(t)
		meta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *SegmentInfo { return newSegment(segID) })
		task := newMixCompactionTask(&datapb.CompactionTask{
			PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10},
			Schema: &schemapb.CollectionSchema{Version: 1},
		}, newAlloc(t), meta, newMockVersionManager())
		plan, err := task.BuildCompactionRequest()
		assert.NoError(t, err)
		assert.Equal(t, statsTaskResource(2048), task.GetTaskResource())
		assert.Equal(t, task.GetTaskResource(), taskcommon.Resource{CPU: plan.GetCpu(), Memory: plan.GetMemory()})
	})

	t.Run("l0", func(t *testing.T) {
		meta := NewMockCompactionMeta(t)
		meta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *SegmentInfo { return newSegment(segID) })
		meta.EXPECT().SelectSegments(mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
		task := newL0CompactionTask(&datapb.CompactionTask{
			PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10},
			Channel: "ch-1", Schema: &schemapb.CollectionSchema{Version: 1},
		}, newAlloc(t), meta)
		plan, err := task.BuildCompactionRequest()
		assert.NoError(t, err)
		assert.Equal(t, task.GetTaskResource(), taskcommon.Resource{CPU: plan.GetCpu(), Memory: plan.GetMemory()})
	})

	t.Run("bumpSchemaVersion", func(t *testing.T) {
		meta := NewMockCompactionMeta(t)
		meta.EXPECT().GetHealthySegment(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, segID int64) *SegmentInfo { return newSegment(segID) })
		task := newBumpSchemaVersionTask(&datapb.CompactionTask{
			PlanID: 1, Type: datapb.CompactionType_BumpSchemaVersionCompaction, InputSegments: []int64{10},
			Schema: &schemapb.CollectionSchema{Version: 1},
		}, newAlloc(t), meta, newMockVersionManager())
		plan, err := task.BuildCompactionRequest()
		assert.NoError(t, err)
		assert.Equal(t, task.GetTaskResource(), taskcommon.Resource{CPU: plan.GetCpu(), Memory: plan.GetMemory()})
	})
}
