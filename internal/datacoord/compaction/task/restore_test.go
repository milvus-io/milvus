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
		plan := &datapb.CompactionTask{PlanID: 5, TriggerID: 7, Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, InputSegments: []int64{1}}
		task := NewL0CompactionTask(plan, nil, m, &CompactionTaskMeta{})
		mockey.Mock((*CompactionTaskMeta).SaveCompactionTask).Return(nil).Build()
		require.True(t, task.restoreMutationApplied())
		task.CreateTaskOnWorker(8, nil) // must not read or resend the retired L0
		require.Equal(t, datapb.CompactionTaskState_meta_saved, task.GetTaskProto().State)
		require.True(t, task.Process())
		require.Equal(t, datapb.CompactionTaskState_completed, task.GetTaskProto().GetTerminalState())
		require.True(t, task.Clean())
		require.Equal(t, datapb.CompactionTaskState_completed, task.GetTaskProto().GetTerminalState(), "cleanup retains successful completion")
		require.NoError(t, task.SaveSegmentMeta(nil), "replaying an applied result does not append twice")
		gc := &gc.GarbageCollector{}
		require.False(t, gc.CheckDroppedSegmentGCForTest(m.GetSegment(ctx, 1), nil, nil, 0))
	})
}

func TestRestoreL0RejectsMissingAndForeignTargets(t *testing.T) {
	ctx := t.Context()
	store := metacache.NewMetaStore(nil)
	m := &meta.Meta{MetaStore: store, Ctx: ctx, Segments: meta.NewSegmentsInfo(store)}
	task := NewL0CompactionTask(&datapb.CompactionTask{TriggerID: 7, Type: datapb.CompactionType_RestoreLevel0DeleteCompaction, CollectionID: 10, Channel: "v1", PartitionID: -1}, nil, m, &CompactionTaskMeta{})
	var job *datapb.CopySegmentJob
	task.LookupRestoreJob = func(_ context.Context, id int64) *datapb.CopySegmentJob {
		require.EqualValues(t, 7, id)
		return job
	}
	_, err := task.restoreTargets()
	require.ErrorIs(t, err, merr.ErrDataIntegrity, "a missing job cannot fall back to ordinary L0 selection")
	job = &datapb.CopySegmentJob{JobId: 7, CollectionId: 10, CreatesCollection: true, State: datapb.CopySegmentJobState_CopySegmentJobExecuting, IdMappings: []*datapb.CopySegmentIDMapping{{TargetSegmentId: 2}}}
	_, err = task.restoreTargets()
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	m.Segments.SetSegment(2, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 2, CollectionID: 10, InsertChannel: "v1", Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, IsImporting: true}))
	require.ErrorIs(t, task.SaveSegmentMeta([]*datapb.CompactionSegment{{SegmentID: 3}}), merr.ErrDataIntegrity)
	require.False(t, task.restoreMutationApplied())
	m.Segments.SetSegment(2, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 2, CollectionID: 11, InsertChannel: "v1", Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed}))
	_, err = task.restoreTargets()
	require.ErrorIs(t, err, merr.ErrDataIntegrity, "foreign metadata cannot become a restore target")
	task.SetTask(task.ShadowClone(func(plan *datapb.CompactionTask) { plan.CollectionID = 11 }))
	_, err = task.restoreTargets()
	require.ErrorIs(t, err, merr.ErrDataIntegrity, "the trigger job must own this collection")
}
