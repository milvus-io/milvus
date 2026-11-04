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

package task

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/gc"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRestoreL0CommitAndTaskSaveCrash(t *testing.T) {
	mockey.PatchConvey("input retirement is an atomic materialization proof", t, func() {
		ctx := t.Context()
		store := metacache.NewMetaStore(nil)
		m := &meta.Meta{MetaStore: store, Ctx: ctx, Segments: meta.NewSegmentsInfo(store)}
		m.Segments.SetSegment(1, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Dropped, Compacted: true, IsImporting: true}))
		plan := &datapb.CompactionTask{PlanID: 5, RestoreJobId: 7, InputSegments: []int64{1}, RestoreTargetSegments: []int64{2}}
		task := NewL0CompactionTask(plan, nil, m, &CompactionTaskMeta{})
		mockey.Mock((*CompactionTaskMeta).SaveCompactionTask).Return(nil).Build()
		require.True(t, task.restoreMutationApplied())
		task.CreateTaskOnWorker(8, nil) // must not read or resend the retired L0
		require.Equal(t, datapb.CompactionTaskState_meta_saved, task.GetTaskProto().State)
		require.True(t, task.Process())
		require.True(t, task.GetTaskProto().RestoreCompleted)
		require.True(t, task.Clean())
		require.True(t, task.GetTaskProto().RestoreCompleted, "cleanup retains successful completion")
		require.NoError(t, task.SaveSegmentMeta(nil), "replaying an applied result does not append twice")
		gc := &gc.GarbageCollector{}
		require.False(t, gc.CheckDroppedSegmentGCForTest(m.GetSegment(ctx, 1), nil, nil, 0))
	})
}

func TestRestoreL0RejectsMissingAndForeignTargets(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	m := &meta.Meta{MetaStore: store, Ctx: context.Background(), Segments: meta.NewSegmentsInfo(store)}
	task := NewL0CompactionTask(&datapb.CompactionTask{RestoreJobId: 7, CollectionID: 10, Channel: "v1", PartitionID: -1, RestoreTargetSegments: []int64{2}}, nil, m, &CompactionTaskMeta{})
	_, err := task.restoreTargets()
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	m.Segments.SetSegment(2, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 2, CollectionID: 10, InsertChannel: "v1", Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, IsImporting: true}))
	require.ErrorIs(t, task.SaveSegmentMeta([]*datapb.CompactionSegment{{SegmentID: 3}}), merr.ErrDataIntegrity)
	require.False(t, task.restoreMutationApplied())
}
