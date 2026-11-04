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

package copysegment

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/compaction"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
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
		catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
		store := metacache.NewMetaStore(nil)
		source := &restoreSnapshotSource{}
		m := &meta.Meta{MetaStore: store, Ctx: ctx, Catalog: catalog, Segments: meta.NewSegmentsInfo(store)}
		taskMeta, err := ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, source, nil)
		require.NoError(t, err)
		job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
		job.CreatesCollection = true
		job.SnapshotCache = &CopySegmentSnapshotCache{}
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
			m.Segments.SetSegment(s.ID, meta.NewSegmentInfo(s))
			job.IdMappings = append(job.IdMappings, &datapb.CopySegmentIDMapping{SourceSegmentId: s.ID + 100, TargetSegmentId: s.ID, PartitionId: s.PartitionID})
		}
		require.NoError(t, copies.AddJob(ctx, job))
		snapshot := &snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Name: "snapshot"}}}
		mockey.Mock((*restoreSnapshotSource).ReadSnapshotData).Return(snapshot, nil).Build()
		allocations := mockey.Mock((*restoreAllocatorTarget).AllocN).Return(int64(1000), int64(1002), nil).Build()
		mockey.Mock((*compaction.Inspector).HasCompactionTask).Return(true).Build()
		submissions := 0
		mockey.Mock((*compaction.Inspector).EnqueueCompaction).To(func(_ *compaction.Inspector, plan *datapb.CompactionTask) error {
			submissions++
			require.NotZero(t, copies.GetJob(ctx, 100).(*Job).GetRestoreCompactionPlans()["v1/-1"])
			return taskMeta.SaveCompactionTask(ctx, proto.Clone(plan).(*datapb.CompactionTask))
		}).Build()
		c := &Checker{CompactionTasks: taskMeta, SnapshotSource: source, Ctx: ctx, Meta: m, CopyMeta: copies, alloc: &restoreAllocatorTarget{}, Compactions: &compaction.Inspector{}}
		ready, err := c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 2, submissions)
		plans := taskMeta.GetCompactionTasksByTriggerID(100)
		require.Len(t, plans, 2)
		for _, plan := range plans {
			if plan.PartitionID == -1 {
				require.Equal(t, []int64{2, 3}, plan.RestoreTargetSegments)
			} else {
				require.Equal(t, []int64{2}, plan.RestoreTargetSegments)
			}
			task := ctask.NewL0CompactionTask(plan, nil, m, taskMeta)
			targets, binlogs, err := task.SelectFlushedSegment()
			require.NoError(t, err)
			require.Len(t, targets, len(plan.RestoreTargetSegments))
			require.EqualValues(t, 300, binlogs[0].CommitTimestamp)
		}
		// Restart between submission and completion: no duplicate plan allocation.
		copies, err = NewCopySegmentMeta(ctx, catalog, m, nil, source, nil)
		require.NoError(t, err)
		c.CopyMeta = copies
		taskMeta, err = ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		c.CompactionTasks = taskMeta
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 2, submissions)
		require.Equal(t, 1, allocations.Times())
		first := proto.Clone(plans[0]).(*datapb.CompactionTask)
		first.State = datapb.CompactionTaskState_cleaned
		first.RestoreCompleted = true
		require.NoError(t, taskMeta.SaveCompactionTask(ctx, first))
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		last := proto.Clone(plans[1]).(*datapb.CompactionTask)
		last.State = datapb.CompactionTaskState_cleaned
		last.RestoreCompleted = false
		require.NoError(t, taskMeta.SaveCompactionTask(ctx, last))
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.False(t, ready)
		require.Equal(t, 3, submissions, "failed cleaned attempt retries its stable plan")
		last.RestoreCompleted = true
		require.NoError(t, taskMeta.SaveCompactionTask(ctx, last))
		ready, err = c.materializeRestoreDeletes(copies.GetJob(ctx, 100))
		require.NoError(t, err)
		require.True(t, ready)
		require.True(t, m.GetSegment(ctx, 2).GetIsImporting(), "materialization does not itself publish data")
	})
}

func TestRestoreL0AdmissionRetry(t *testing.T) {
	mockey.PatchConvey("rejected admission reuses its durable plan ID", t, func() {
		ctx := t.Context()
		catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
		store := metacache.NewMetaStore(nil)
		source := &restoreSnapshotSource{}
		m := &meta.Meta{MetaStore: store, Ctx: ctx, Catalog: catalog, Segments: meta.NewSegmentsInfo(store)}
		taskMeta, err := ctask.NewCompactionTaskMeta(ctx, catalog)
		require.NoError(t, err)
		copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, nil, nil)
		require.NoError(t, err)
		job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
		job.IdMappings = []*datapb.CopySegmentIDMapping{{TargetSegmentId: 1}}
		require.NoError(t, copies.AddJob(ctx, job))
		c := &Checker{CompactionTasks: taskMeta, SnapshotSource: source, Ctx: ctx, Meta: m, CopyMeta: copies, alloc: &restoreAllocatorTarget{}}
		_, err = c.materializeRestoreDeletes(job)
		require.ErrorIs(t, err, merr.ErrDataIntegrity)
		m.Segments.SetSegment(1, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L1}))
		ready, err := c.materializeRestoreDeletes(job)
		require.NoError(t, err)
		require.True(t, ready, "a snapshot without L0 needs no materialization")
		m.Segments.SetSegment(1, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 1, Level: datapb.SegmentLevel_L0, InsertChannel: "v1"}))
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
		c.Compactions = &compaction.Inspector{}
		tracked := mockey.Mock((*compaction.Inspector).HasCompactionTask).Return(false).Build()
		rejected := true
		submissions := 0
		mockey.Mock((*compaction.Inspector).EnqueueCompaction).To(func(_ *compaction.Inspector, plan *datapb.CompactionTask) error {
			submissions++
			require.EqualValues(t, 1000, plan.PlanID)
			require.NoError(t, taskMeta.SaveCompactionTask(ctx, proto.Clone(plan).(*datapb.CompactionTask)))
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

func TestRestoreSegmentRetainsEffectiveInsertTime(t *testing.T) {
	source := &datapb.SegmentDescription{
		SegmentId: 1, SegmentLevel: datapb.SegmentLevel_L1, CommitTimestamp: 300,
		StartPosition: &msgpb.MsgPosition{Timestamp: 100, ChannelName: "source"}, DmlPosition: &msgpb.MsgPosition{Timestamp: 100},
	}
	data, err := proto.Marshal(source)
	require.NoError(t, err)
	decoded := &datapb.SegmentDescription{}
	require.NoError(t, proto.Unmarshal(data, decoded))
	target := RestoredSegmentInfo(decoded, 20, 30, 40, "target", 500)
	require.EqualValues(t, 300, meta.SegmentEffectiveTs(target.SegmentInfo))
	require.EqualValues(t, 300, target.GetCommitTimestamp())
	require.EqualValues(t, 100, target.GetStartPosition().GetTimestamp())
	require.EqualValues(t, 500, target.GetTransformStartAfterTimetick())
	require.Equal(t, "source", source.GetStartPosition().GetChannelName())
}
