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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

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
