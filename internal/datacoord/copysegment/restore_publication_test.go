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
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	kvdatacoord "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRestorePublicationRetriesWithoutNewState(t *testing.T) {
	lookup := mockey.Mock((*restoreCollectionTarget).IsCollectionDropped).Return(false, nil).Build()
	defer lookup.UnPatch()
	ctx := t.Context()
	task := CreateTestCopyTask(100, 2001).(*Task)
	copies, meta := NewCopySegmentTaskTestMeta(t, task)
	job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
	job.CreatesCollection = true
	job.IdMappings = task.GetIdMappings()
	job.TimeoutTs = CopyJobTimeoutTs(-time.Hour)
	require.NoError(t, copies.AddJob(ctx, job))
	require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted)))
	// A restart reconstructs the publication prerequisite from existing records.
	recovered, err := NewCopySegmentMeta(ctx, meta.Catalog, meta, nil, nil, nil)
	require.NoError(t, err)
	checker := &Checker{Ctx: ctx, CopyMeta: recovered, Meta: meta, CollectionRestorer: &restoreCollectionTarget{}}
	checker.tryTimeoutJob(job)
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, recovered.GetJob(ctx, 100).GetState())
	patch := mockey.Mock((*restoreCollectionTarget).CompleteRestoreCollection).Return(merr.ErrServiceUnavailable).Build()
	checker.publishRestoredCollection(job, 10)
	patch.UnPatch()
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, recovered.GetJob(ctx, 100).GetState())
	// Stale task failure and timeout cannot revoke copied data while publication retries.
	require.NoError(t, recovered.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskFailed)))
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, recovered.GetTask(ctx, task.GetTaskId()).GetState())
	require.NoError(t, recovered.UpdateJobStateAndReleaseRef(ctx, 100, UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobFailed)))
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, recovered.GetJob(ctx, 100).GetState())
	patch = mockey.Mock((*restoreCollectionTarget).CompleteRestoreCollection).Return(nil).Build()
	checker.publishRestoredCollection(job, 10)
	patch.UnPatch()
	checker.tryTimeoutJob(job) // Snapshot from before completion.
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobCompleted, recovered.GetJob(ctx, 100).GetState())
}

func TestRestoreCopyCoverageRequiresEveryExpectedSegment(t *testing.T) {
	job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*Job)
	job.IdMappings = []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 2001}, {SourceSegmentId: 2, TargetSegmentId: 2002}}
	task := CreateTestCopyTask(100, 2001).(*Task)
	UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted)(task)
	require.False(t, allRestoreSegmentsCopied(job, []CopySegmentTask{task}))
	task.Task.Load().IdMappings = job.IdMappings
	require.True(t, allRestoreSegmentsCopied(job, []CopySegmentTask{task}))
	job.IdMappings = nil
	require.True(t, allRestoreSegmentsCopied(job, nil))
}

func TestRestoreSegmentRegistrationResumesFromJob(t *testing.T) {
	ctx := t.Context()
	catalog := kvdatacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	store := metacache.NewMetaStore(nil)
	source := &restoreSnapshotSource{}
	m := &meta.Meta{MetaStore: store, Catalog: catalog, Segments: meta.NewSegmentsInfo(store)}
	copies, err := NewCopySegmentMeta(ctx, catalog, m, nil, source, nil)
	require.NoError(t, err)
	job := NewTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobPending).(*Job)
	job.CreatesCollection = true
	job.IdMappings = []*datapb.CopySegmentIDMapping{{SourceSegmentId: 1, TargetSegmentId: 2001, PartitionId: 10}, {SourceSegmentId: 2, TargetSegmentId: 2002, PartitionId: 10}}
	job.ChannelMapping = map[string]string{"source": "target"}
	job.TransformStartAfterTimeticks = map[string]uint64{"target": 123}
	require.NoError(t, copies.AddJob(ctx, job))
	snapshot := &snapshotstorage.SnapshotData{Segments: []*datapb.SegmentDescription{
		{SegmentId: 1, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L1},
		{SegmentId: 2, ChannelName: "source", SegmentLevel: datapb.SegmentLevel_L1},
	}}
	patch := mockey.Mock((*restoreSnapshotSource).ReadSnapshotData).Return(snapshot, nil).Build()
	defer patch.UnPatch()
	// Crash after Job persistence: no target segment has been registered yet.
	copies, err = NewCopySegmentMeta(ctx, catalog, m, nil, source, nil)
	require.NoError(t, err)
	checker := &Checker{SnapshotSource: source, Ctx: ctx, Meta: m, CopyMeta: copies}
	var original func(*meta.Meta, context.Context, *meta.SegmentInfo) error
	fail := mockey.Mock((*meta.Meta).AddSegment).Origin(&original).To(func(meta *meta.Meta, ctx context.Context, segment *meta.SegmentInfo) error {
		if segment.GetID() == 2002 {
			return merr.ErrServiceUnavailable
		}
		return original(meta, ctx, segment)
	}).Build()
	require.ErrorIs(t, checker.registerRestoreSegments(copies.GetJob(ctx, 100)), merr.ErrServiceUnavailable)
	fail.UnPatch()
	require.NotNil(t, m.GetSegment(ctx, 2001))
	require.Nil(t, m.GetSegment(ctx, 2002))
	// Another restart after partial registration reuses both original IDs.
	copies, err = NewCopySegmentMeta(ctx, catalog, m, nil, source, nil)
	require.NoError(t, err)
	checker.CopyMeta = copies
	require.NoError(t, checker.registerRestoreSegments(copies.GetJob(ctx, 100)))
	require.NoError(t, checker.registerRestoreSegments(copies.GetJob(ctx, 100)))
	for _, id := range []int64{2001, 2002} {
		segment := m.GetSegment(ctx, id)
		require.Equal(t, "target", segment.GetInsertChannel())
		require.EqualValues(t, 123, segment.GetTransformStartAfterTimetick())
	}
}
