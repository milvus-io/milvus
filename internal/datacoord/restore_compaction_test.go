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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRestoreL0MaterializationResumesFixedPlans(t *testing.T) {
	mockey.PatchConvey("durable per-partition coverage gates publication", t, func() {
		paramtable.Init()
		ctx := t.Context()
		catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
		store := metacache.NewMetaStore(nil)
		m := &meta{metaStore: store, ctx: ctx, catalog: catalog, segments: NewSegmentsInfo(store), snapshotMeta: &snapshotMeta{}}
		var err error
		m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		copies, err := NewCopySegmentMeta(ctx, catalog, m, m.snapshotMeta, nil)
		require.NoError(t, err)
		job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
		job.CreatesCollection = true
		job.snapshotCache = &copySegmentSnapshotCache{}
		job.CollectionId = 200
		job.IdMappings = nil
		for _, s := range []*datapb.SegmentInfo{
			{ID: 1, Level: datapb.SegmentLevel_L0, PartitionID: -1, InsertChannel: "v1"},
			{ID: 2, Level: datapb.SegmentLevel_L1, PartitionID: 10, InsertChannel: "v1", CommitTimestamp: 300},
			{ID: 3, Level: datapb.SegmentLevel_L1, PartitionID: 20, InsertChannel: "v1"},
			{ID: 4, Level: datapb.SegmentLevel_L1, PartitionID: 10, InsertChannel: "v2"},
			{ID: 5, Level: datapb.SegmentLevel_L0, PartitionID: 10, InsertChannel: "v1"},
		} {
			s.CollectionID = 200
			s.State = commonpb.SegmentState_Flushed
			s.IsImporting = true
			m.segments.SetSegment(s.ID, NewSegmentInfo(s))
			job.IdMappings = append(job.IdMappings, &datapb.CopySegmentIDMapping{SourceSegmentId: s.ID + 100, TargetSegmentId: s.ID, PartitionId: s.PartitionID})
		}
		require.NoError(t, copies.AddJob(ctx, job))
		snapshot := &snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "snapshot"}}}
		mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(snapshot, nil).Build()
		allocations := mockey.Mock((*restoreAllocatorTarget).AllocN).Return(int64(1000), int64(1002), nil).Build()
		mockey.Mock((*compactionInspector).getCompactionTask).Return(&l0CompactionTask{}).Build()
		submissions := 0
		mockey.Mock((*compactionInspector).enqueueCompaction).To(func(_ *compactionInspector, plan *datapb.CompactionTask) error {
			submissions++
			require.NotZero(t, copies.GetJob(ctx, 100).(*copySegmentJob).GetRestoreCompactionPlans()["v1/-1"])
			return m.SaveCompactionTask(ctx, proto.Clone(plan).(*datapb.CompactionTask))
		}).Build()
		c := &copySegmentChecker{ctx: ctx, meta: m, copyMeta: copies, alloc: &restoreAllocatorTarget{}, compactions: &compactionInspector{}}
		ready, err := c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 2, submissions)
		plans := m.GetCompactionTasksByTriggerID(ctx, 100)
		require.Len(t, plans, 2)
		for _, plan := range plans {
			if plan.PartitionID == -1 {
				require.Equal(t, []int64{2, 3}, plan.RestoreTargetSegments)
			} else {
				require.Equal(t, []int64{2}, plan.RestoreTargetSegments)
			}
			task := newL0CompactionTask(plan, nil, m)
			targets, binlogs, err := task.selectFlushedSegment()
			require.NoError(t, err)
			require.Len(t, targets, len(plan.RestoreTargetSegments))
			require.EqualValues(t, 300, binlogs[0].CommitTimestamp)
		}
		// Restart between submission and completion: no duplicate plan allocation.
		copies, err = NewCopySegmentMeta(ctx, catalog, m, m.snapshotMeta, nil)
		require.NoError(t, err)
		c.copyMeta = copies
		m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 2, submissions)
		require.Equal(t, 1, allocations.Times())
		first := proto.Clone(plans[0]).(*datapb.CompactionTask)
		first.State = datapb.CompactionTaskState_cleaned
		first.RestoreCompleted = true
		require.NoError(t, m.SaveCompactionTask(ctx, first))
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		last := proto.Clone(plans[1]).(*datapb.CompactionTask)
		last.State = datapb.CompactionTaskState_cleaned
		last.RestoreCompleted = false
		require.NoError(t, m.SaveCompactionTask(ctx, last))
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 3, submissions, "failed cleaned attempt retries its stable plan")
		last.RestoreCompleted = true
		require.NoError(t, m.SaveCompactionTask(ctx, last))
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.True(t, ready)
		require.True(t, m.GetSegment(ctx, 2).GetIsImporting(), "materialization does not itself publish data")
	})
}

func TestRestoreL0CommitAndTaskSaveCrash(t *testing.T) {
	mockey.PatchConvey("input retirement is an atomic materialization proof", t, func() {
		ctx := t.Context()
		store := metacache.NewMetaStore(nil)
		m := &meta{metaStore: store, ctx: ctx, segments: NewSegmentsInfo(store)}
		m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Dropped, Compacted: true, IsImporting: true}))
		plan := &datapb.CompactionTask{PlanID: 5, RestoreJobId: 7, InputSegments: []int64{1}, RestoreTargetSegments: []int64{2}}
		task := newL0CompactionTask(plan, nil, m)
		mockey.Mock((*meta).SaveCompactionTask).Return(nil).Build()
		require.True(t, task.restoreMutationApplied())
		task.CreateTaskOnWorker(8, nil) // must not read or resend the retired L0
		require.Equal(t, datapb.CompactionTaskState_meta_saved, task.GetTaskProto().State)
		require.True(t, task.Process())
		require.True(t, task.GetTaskProto().RestoreCompleted)
		require.True(t, task.Clean())
		require.True(t, task.GetTaskProto().RestoreCompleted, "cleanup retains successful completion")
		require.NoError(t, task.saveSegmentMeta(nil), "replaying an applied result does not append twice")
		gc := &garbageCollector{}
		require.False(t, gc.checkDroppedSegmentGC(m.GetSegment(ctx, 1), nil, nil, 0))
	})
}

func TestRestoreSegmentRetainsEffectiveInsertTime(t *testing.T) {
	source := &datapb.SegmentDescription{
		SegmentId: 1, SegmentLevel: datapb.SegmentLevel_L1, CommitTimestamp: 300,
		StartPosition: &msgpb.MsgPosition{Timestamp: 100, ChannelName: "source"}, DmlPosition: &msgpb.MsgPosition{Timestamp: 100},
	}
	data, err := proto.Marshal(source)
	require.NoError(t, err)
	decoded := &datapb.SegmentDescription{}
	require.NoError(t, proto.Unmarshal(data, decoded))
	target := restoredSegmentInfo(decoded, 20, 30, 40, "target", 500)
	require.EqualValues(t, 300, segmentEffectiveTs(target.SegmentInfo))
	require.EqualValues(t, 300, target.GetCommitTimestamp())
	require.EqualValues(t, 100, target.GetStartPosition().GetTimestamp())
	require.EqualValues(t, 500, target.GetTransformStartAfterTimetick())
	require.Equal(t, "source", source.GetStartPosition().GetChannelName())
}

func TestRestoreL0RejectsMissingAndForeignTargets(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	m := &meta{metaStore: store, ctx: context.Background(), segments: NewSegmentsInfo(store)}
	task := newL0CompactionTask(&datapb.CompactionTask{RestoreJobId: 7, CollectionID: 10, Channel: "v1", PartitionID: -1, RestoreTargetSegments: []int64{2}}, nil, m)
	_, err := task.restoreTargets()
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	m.segments.SetSegment(2, NewSegmentInfo(&datapb.SegmentInfo{ID: 2, CollectionID: 10, InsertChannel: "v1", Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, IsImporting: true}))
	require.ErrorIs(t, task.saveSegmentMeta([]*datapb.CompactionSegment{{SegmentID: 3}}), merr.ErrDataIntegrity)
	require.False(t, task.restoreMutationApplied())
}

func TestRestoreL0AdmissionRetry(t *testing.T) {
	mockey.PatchConvey("rejected admission reuses its durable plan ID", t, func() {
		ctx := t.Context()
		catalog := kvdatacoord.NewCatalog(NewMetaMemoryKV(), "", "")
		store := metacache.NewMetaStore(nil)
		m := &meta{metaStore: store, ctx: ctx, catalog: catalog, segments: NewSegmentsInfo(store)}
		var err error
		m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil)
		require.NoError(t, err)
		job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
		job.IdMappings = []*datapb.CopySegmentIDMapping{{TargetSegmentId: 1}}
		require.NoError(t, copies.AddJob(ctx, job))
		c := &copySegmentChecker{ctx: ctx, meta: m, copyMeta: copies, alloc: &restoreAllocatorTarget{}}
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L1}))
		ready, err := c.materializeRestoreDeletes(job)
		require.NoError(t, err)
		require.True(t, ready, "a snapshot without L0 needs no materialization")
		m.segments.SetSegment(1, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L0, InsertChannel: "v1"}))
		snapshotRead := mockey.Mock(loadCopySnapshot).Return(nil, merr.ErrServiceUnavailable).Build()
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		snapshotRead.UnPatch()
		mockey.Mock(loadCopySnapshot).Return(&snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{}}, nil).Build()
		allocation := mockey.Mock((*restoreAllocatorTarget).AllocN).Return(int64(0), int64(0), merr.ErrServiceUnavailable).Build()
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		allocation.UnPatch()
		allocation = mockey.Mock((*restoreAllocatorTarget).AllocN).Return(int64(1000), int64(1001), nil).Build()
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable, "plan identity persists before scheduler becomes available")
		c.compactions = &compactionInspector{}
		tracked := mockey.Mock((*compactionInspector).getCompactionTask).Return(nil).Build()
		rejected := true
		submissions := 0
		mockey.Mock((*compactionInspector).enqueueCompaction).To(func(_ *compactionInspector, plan *datapb.CompactionTask) error {
			submissions++
			require.EqualValues(t, 1000, plan.PlanID)
			require.NoError(t, m.SaveCompactionTask(ctx, proto.Clone(plan).(*datapb.CompactionTask)))
			if rejected {
				return merr.ErrServiceUnavailable
			}
			return nil
		}).Build()
		_, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		rejected = false
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 2, submissions)
		require.Equal(t, 1, allocation.Times())
		require.Equal(t, 1, tracked.Times())
	})
}
