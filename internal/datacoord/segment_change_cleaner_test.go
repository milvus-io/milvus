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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type failNthSegmentChangeDeleteCatalog struct {
	metastore.DataCoordCatalog
	failAt    int
	deletedID []int64
}

func (c *failNthSegmentChangeDeleteCatalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	for _, action := range actions {
		if action.Type == metastore.ActionDelete {
			if entry, ok := action.Entry.(metastore.SegmentChangeGroupEntry); ok {
				c.deletedID = append(c.deletedID, entry.GroupID)
				if len(c.deletedID) == c.failAt {
					return merr.WrapErrServiceInternalMsg("injected segment change group deletion failure")
				}
			}
		}
	}
	return c.DataCoordCatalog.Update(ctx, actions...)
}

func TestSegmentChangeCleanerWaitsForProducerAndRetirement(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	task := &datapb.CompactionTask{
		PlanID: ready.SourceJobID, CollectionID: ready.CollectionID,
		State: datapb.CompactionTaskState_executing,
	}
	require.NoError(t, m.SaveCompactionTask(t.Context(), task))
	require.NoError(t, m.publishGroup(t.Context(), ready, newMockAllocator(t)))
	cleaner := &segmentChangeCleaner{mt: m}
	clean := func() {
		cleaner.clean(t.Context(), m.GetSegmentChangeGroupsByCollection(t.Context(), 0))
	}
	clean()
	require.NotNil(t, m.GetSegmentChangeGroup(t.Context(), ready.CollectionID, ready.GroupID),
		"an executing producer still needs its retry identity")
	task.State = datapb.CompactionTaskState_completed
	require.NoError(t, m.SaveCompactionTask(t.Context(), task))
	clean()
	require.NotNil(t, m.GetSegmentChangeGroup(t.Context(), ready.CollectionID, ready.GroupID),
		"a superseded segment may still be needed by a retained snapshot")
	require.NoError(t, m.DropSegment(t.Context(), 2001))
	clean()
	require.Nil(t, m.GetSegmentChangeGroup(t.Context(), ready.CollectionID, ready.GroupID))
	require.Nil(t, reloadSegmentChangeMeta(t, m).GetSegmentChangeGroup(t.Context(), ready.CollectionID, ready.GroupID),
		"the terminal group key must also be removed from the catalog")
}

func TestSegmentChangeCleanerWaitsForFailedMemberReclamation(t *testing.T) {
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	ready := seedPublishMeta(t, m, model.SegmentChangeSourceMixCompaction)
	require.NoError(t, m.SaveCompactionTask(t.Context(), &datapb.CompactionTask{
		PlanID: ready.SourceJobID, CollectionID: ready.CollectionID,
		State: datapb.CompactionTaskState_failed,
	}))
	failed := ready.Clone()
	failed.State = model.SegmentChangeStateFailed
	require.NoError(t, m.UpdateSegmentChangeGroup(t.Context(), failed))
	cleaner := &segmentChangeCleaner{mt: m}
	cleaner.clean(t.Context(), m.GetSegmentChangeGroupsByCollection(t.Context(), 0))
	require.NotNil(t, m.GetSegmentChangeGroup(t.Context(), failed.CollectionID, failed.GroupID))
	require.NoError(t, m.reclaimSegmentChangeGroup(t.Context(), failed))
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState(),
		"reclamation must leave the original parent available")
	cleaner.clean(t.Context(), m.GetSegmentChangeGroupsByCollection(t.Context(), 0))
	require.Nil(t, m.GetSegmentChangeGroup(t.Context(), failed.CollectionID, failed.GroupID))
}

func TestSegmentChangeCleanerUsesCopyTaskIdentity(t *testing.T) {
	copyMeta := &copySegmentMeta{
		jobs:  make(map[int64]CopySegmentJob),
		tasks: newCopySegmentTasks(),
	}
	job := &copySegmentJob{CopySegmentJob: &datapb.CopySegmentJob{
		JobId: 700, State: datapb.CopySegmentJobState_CopySegmentJobExecuting,
	}}
	copyMeta.jobs[700] = job
	task := &copySegmentTask{}
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 900, JobId: 700, State: datapb.CopySegmentTaskState_CopySegmentTaskInProgress,
	})
	copyMeta.tasks.add(task)
	g := &model.SegmentChangeGroup{Source: model.SegmentChangeSourceCopySegment, SourceJobID: 900}
	cleaner := &segmentChangeCleaner{copyMeta: copyMeta}
	require.False(t, cleaner.producerFinished(t.Context(), g, nil))
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 900, JobId: 700, State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
	})
	require.False(t, cleaner.producerFinished(t.Context(), g, nil), "the owning job can still retry")
	job.State = datapb.CopySegmentJobState_CopySegmentJobCompleted
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 900, JobId: 700, State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
		CleanupRequired: true,
	})
	require.False(t, cleaner.producerFinished(t.Context(), g, nil), "copy output cleanup is still pending")
	task.task.Store(&datapb.CopySegmentTask{
		TaskId: 900, JobId: 700, State: datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
	})
	require.True(t, cleaner.producerFinished(t.Context(), g, nil))
}

func TestSegmentChangeInspectorCleansOnlyFinishedProducers(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	compaction := &model.SegmentChangeGroup{
		GroupID: 5000, Source: model.SegmentChangeSourceMixCompaction, SourceJobID: 5000,
		CollectionID: 10, State: model.SegmentChangeStateCommitted,
		InitialSegmentIDs: []int64{1001}, NewSegmentIDs: []int64{1002},
		SortJobs: map[int64]model.SegmentChangeSortJob{6000: {InputSegmentID: 1001, OutputSegmentID: 1002}},
	}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, compaction))
	require.NoError(t, m.SaveCompactionTask(ctx, &datapb.CompactionTask{
		PlanID: 5000, CollectionID: 10, State: datapb.CompactionTaskState_completed,
	}))
	require.NoError(t, m.SaveCompactionTask(ctx, &datapb.CompactionTask{
		PlanID: 6000, CollectionID: 10, State: datapb.CompactionTaskState_executing,
	}))
	importGroup := &model.SegmentChangeGroup{
		GroupID: 7000, Source: model.SegmentChangeSourceImportJob, SourceJobID: 8000,
		CollectionID: 10, State: model.SegmentChangeStateCommitted,
	}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, importGroup))
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 8000, State: internalpb.ImportJobState_Importing}}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, int64(8000)).Return(job).Maybe()
	inspector := newSegmentChangeInspector(ctx, m, newMockAllocator(t))
	server := &Server{meta: m, importMeta: importMeta, segmentChangeInspector: inspector}
	server.initSegmentChangeCleaner()
	require.Same(t, m, inspector.cleaner.mt)
	require.Same(t, importMeta, inspector.cleaner.importMeta)
	require.Same(t, m.externalCollectionRefreshMeta, inspector.cleaner.refreshMeta)
	inspector.checkAllGroups(ctx)
	require.NotNil(t, m.GetSegmentChangeGroup(ctx, 10, 5000), "unfinished sort task retains the compaction group")
	require.NotNil(t, m.GetSegmentChangeGroup(ctx, 10, 7000), "unfinished import job retains the import group")

	require.NoError(t, m.SaveCompactionTask(ctx, &datapb.CompactionTask{
		PlanID: 6000, CollectionID: 10, State: datapb.CompactionTaskState_completed,
	}))
	job.State = internalpb.ImportJobState_Completed
	inspector.checkAllGroups(ctx)
	require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 5000))
	require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 7000))
}

func TestSegmentChangeCleanerPreservesImportPredecessorsAcrossRestart(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	const jobID int64 = 8000
	for ordinal, id := range []int64{1001, 1002, 1003} {
		group := &model.SegmentChangeGroup{
			GroupID: id, Source: model.SegmentChangeSourceImportJob, SourceJobID: jobID,
			CollectionID: 10, VChannel: "ch-1", SubgroupOrdinal: int64(ordinal),
			State: model.SegmentChangeStateCommitted,
		}
		if ordinal > 0 {
			group.PreviousGroupID = id - 1
		}
		if ordinal == 2 {
			group.State = model.SegmentChangeStateStaged
		}
		require.NoError(t, m.AddSegmentChangeGroup(ctx, group))
	}
	require.ErrorIs(t, m.DeleteSegmentChangeGroup(ctx, 10, 1001), merr.ErrDataIntegrity)
	require.ErrorIs(t, m.DeleteSegmentChangeGroup(ctx, 10, 1002), merr.ErrDataIntegrity)
	require.ErrorIs(t, m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{metastore.DeleteSegmentChangeGroup(10, 1001)}), merr.ErrDataIntegrity)

	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, jobID).Return(&importJob{ImportJob: &datapb.ImportJob{
		JobID: jobID, State: internalpb.ImportJobState_Completed,
	}}).Maybe()
	cleaner := &segmentChangeCleaner{mt: m, importMeta: importMeta}
	cleaner.clean(ctx, m.GetSegmentChangeGroupsByCollection(ctx, 10))
	require.Len(t, m.GetSegmentChangeGroupsByCollection(ctx, 10), 3,
		"an ineligible successor must retain the entire predecessor chain")

	last := m.GetSegmentChangeGroup(ctx, 10, 1003)
	last.State = model.SegmentChangeStateAborted
	require.NoError(t, m.UpdateSegmentChangeGroup(ctx, last))
	failedCatalog := &failNthSegmentChangeDeleteCatalog{DataCoordCatalog: m.catalog, failAt: 2}
	m.catalog = failedCatalog
	cleaner.clean(ctx, m.GetSegmentChangeGroupsByCollection(ctx, 10))
	require.Equal(t, []int64{1003, 1002}, failedCatalog.deletedID,
		"the cleaner must delete the successor first and stop at a failed delete")
	require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 1003))
	require.NotNil(t, m.GetSegmentChangeGroup(ctx, 10, 1002))

	recovered := reloadSegmentChangeMeta(t, m)
	require.Len(t, recovered.GetSegmentChangeGroupsByCollection(ctx, 10), 2,
		"the persisted intermediate state must survive a coordinator restart")
	require.ErrorIs(t, recovered.DeleteSegmentChangeGroup(ctx, 10, 1001), merr.ErrDataIntegrity,
		"recovery must rebuild the successor index before cleanup resumes")
	cleaner.mt = recovered
	cleaner.clean(ctx, recovered.GetSegmentChangeGroupsByCollection(ctx, 10))
	require.Equal(t, []int64{1003, 1002, 1002, 1001}, failedCatalog.deletedID)
	require.Empty(t, reloadSegmentChangeMeta(t, recovered).GetSegmentChangeGroupsByCollection(ctx, 10))
}

func TestSegmentChangeGroupCompositeWriteCannotOrphanNewSuccessor(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	first := &model.SegmentChangeGroup{
		GroupID: 1001, Source: model.SegmentChangeSourceImportJob, SourceJobID: 8000,
		CollectionID: 10, VChannel: "ch-1", State: model.SegmentChangeStateCommitted,
	}
	require.NoError(t, m.AddSegmentChangeGroup(ctx, first))
	successor := &model.SegmentChangeGroup{
		GroupID: 1002, Source: model.SegmentChangeSourceImportJob, SourceJobID: 8000,
		CollectionID: 10, VChannel: "ch-1", State: model.SegmentChangeStateStaged,
		PreviousGroupID: 1001, SubgroupOrdinal: 1,
	}
	for _, actions := range [][]metastore.UpdateAction{
		{metastore.SaveSegmentChangeGroup(successor), metastore.DeleteSegmentChangeGroup(10, 1001)},
		{metastore.DeleteSegmentChangeGroup(10, 1001), metastore.SaveSegmentChangeGroup(successor)},
	} {
		require.ErrorIs(t, m.UpdateSegmentsInfoAndChangeGroups(ctx, actions), merr.ErrDataIntegrity)
		require.NotNil(t, m.GetSegmentChangeGroup(ctx, 10, 1001))
		require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 1002))
	}
}

func TestSegmentChangeGroupCreationRequiresPersistedPredecessor(t *testing.T) {
	ctx := t.Context()
	m, err := newMemoryMeta(t)
	require.NoError(t, err)
	first := &model.SegmentChangeGroup{
		GroupID: 1001, Source: model.SegmentChangeSourceImportJob, SourceJobID: 8000,
		CollectionID: 10, VChannel: "ch-1", State: model.SegmentChangeStateCommitted,
	}
	successor := &model.SegmentChangeGroup{
		GroupID: 1002, Source: model.SegmentChangeSourceImportJob, SourceJobID: 8000,
		CollectionID: 10, VChannel: "ch-1", State: model.SegmentChangeStateStaged,
		PreviousGroupID: 1001, SubgroupOrdinal: 1,
	}
	require.ErrorIs(t, m.AddSegmentChangeGroup(ctx, successor), merr.ErrDataIntegrity)
	require.ErrorIs(t, m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(successor)}), merr.ErrDataIntegrity)
	require.Nil(t, m.GetSegmentChangeGroup(ctx, 10, 1002))

	require.NoError(t, m.AddSegmentChangeGroup(ctx, first))
	wrongJob := successor.Clone()
	wrongJob.SourceJobID++
	require.ErrorIs(t, m.AddSegmentChangeGroup(ctx, wrongJob), merr.ErrDataIntegrity)
	require.NoError(t, m.UpdateSegmentsInfoAndChangeGroups(ctx,
		[]metastore.UpdateAction{metastore.SaveSegmentChangeGroup(successor)}))
	require.NotNil(t, reloadSegmentChangeMeta(t, m).GetSegmentChangeGroup(ctx, 10, 1002))
}
