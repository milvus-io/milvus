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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRestorePublishesCompleteDataViewBeforeCollectionAndJob(t *testing.T) {
	mockey.PatchConvey("data visibility and durable completion follow delete installation", t, func() {
		ctx := t.Context()
		task := createTestCopyTask(100, 2001).(*copySegmentTask)
		task.task.Load().IdMappings = append(task.GetIdMappings(), &datapb.CopySegmentIDMapping{SourceSegmentId: 2, TargetSegmentId: 2002})
		copies, m := newCopySegmentTaskTestMeta(t, task)
		data := newTestCopySegment(2001)
		data.State = commonpb.SegmentState_Flushed
		data.Level = datapb.SegmentLevel_L1
		data.IsImporting = true
		data.Binlogs = makeTestCopySegmentBinlogs()
		require.NoError(t, m.AddSegment(ctx, data))
		require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: 2002, CollectionID: 100, InsertChannel: "ch1", Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Dropped, IsImporting: true, Compacted: true})))
		var err error
		m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, m.catalog)
		require.NoError(t, err)
		require.NoError(t, m.SaveCompactionTask(ctx, &datapb.CompactionTask{PlanID: 77, TriggerID: 100, RestoreJobId: 100, TerminalState: datapb.CompactionTaskState_completed, State: datapb.CompactionTaskState_cleaned}))
		job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
		job.CreatesCollection = true
		job.IdMappings = task.GetIdMappings()
		job.RestoreCompactionPlans = map[string]int64{"ch1/-1": 77}
		require.NoError(t, copies.AddJob(ctx, job))
		require.NoError(t, copies.UpdateTask(ctx, task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskCompleted)))
		m.dataViewManager = dataview.NewManager(m.catalog.(dataview.Catalog), nil)
		_, err = m.dataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 100, VChannels: []string{"ch1"}})
		require.NoError(t, err)
		complete := mockey.Mock((*rootcoord.Core).CompleteRestoreCollection).To(func(_ *rootcoord.Core, _ context.Context, id int64) error {
			require.EqualValues(t, 100, id)
			require.False(t, m.GetSegment(ctx, 2001).GetIsImporting())
			require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 2002).GetState())
			ref, err := m.dataViewManager.Latest(ctx, id)
			require.NoError(t, err)
			require.NotNil(t, ref)
			defer ref.Deref()
			require.Equal(t, []int64{2001}, ref.DataView().GetShards()[0].GetPartitions()[0].GetSegmentIds())
			require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(ctx, 100).GetState())
			return nil
		}).Build()
		checker := &copySegmentChecker{ctx: ctx, copyMeta: copies, meta: m, collectionRestorer: &rootcoord.Core{}}
		checker.finishJob(job, 100)
		require.Equal(t, 1, complete.Times())
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobCompleted, copies.GetJob(ctx, 100).GetState())
		require.True(t, m.GetSegment(ctx, 2002).GetIsImporting(), "retirement proof remains owned until terminal cleanup")
		checker.checkGC(copies.GetJob(ctx, 100))
		require.False(t, m.GetSegment(ctx, 2002).GetIsImporting())
	})
}

func TestRestorePublicationKeepsCopiedDataOnMetadataFailure(t *testing.T) {
	for _, failure := range []string{"materialization", "missing target", "metadata write"} {
		t.Run(failure, func(t *testing.T) {
			mockey.PatchConvey("retry publication without failing durable copies", t, func() {
				task := createTestCopyTask(100, 2001).(*copySegmentTask)
				copies, m := newCopySegmentTaskTestMeta(t, task)
				job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
				job.CreatesCollection = true
				job.IdMappings = task.GetIdMappings()
				require.NoError(t, copies.AddJob(t.Context(), job))
				mockey.Mock((*copySegmentChecker).materializeRestoreDeletes).Return(failure != "materialization", nil).Build()
				if failure == "metadata write" {
					require.NoError(t, m.AddSegment(t.Context(), newTestCopySegment(2001)))
					mockey.Mock((*meta).UpdateSegmentsInfo).Return(merr.ErrServiceUnavailable).Build()
				}
				complete := mockey.Mock((*rootcoord.Core).CompleteRestoreCollection).Return(nil).Build()
				checker := &copySegmentChecker{ctx: t.Context(), copyMeta: copies, meta: m, collectionRestorer: &rootcoord.Core{}}
				checker.finishJob(job, 100)
				require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(t.Context(), 100).GetState())
				require.Zero(t, complete.Times())
			})
		})
	}
}

func TestRestoreAckCallbackPublishesIndexNotification(t *testing.T) {
	mockey.PatchConvey("new recipes are applied before notifying index work", t, func() {
		msg := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10}).WithBody(&message.RestoreSnapshotMessageBody{Collection: &message.CreateCollectionRequest{}}).WithBroadcast([]string{"target"}).MustBuildBroadcast()
		result := message.BroadcastResultRestoreSnapshotMessageV2{Message: message.MustAsBroadcastRestoreSnapshotMessageV2(msg)}
		callbacks := &DDLCallbacks{Server: &Server{snapshotManager: &snapshotManager{}, notifyIndexChan: make(chan int64, 1)}}
		apply := mockey.Mock((*snapshotManager).applyRestoreCollection).Return(merr.ErrServiceUnavailable).Build()
		require.ErrorIs(t, callbacks.restoreSnapshotV2AckCallback(t.Context(), result), merr.ErrServiceUnavailable)
		require.Empty(t, callbacks.notifyIndexChan)
		apply.Return(nil)
		require.NoError(t, callbacks.restoreSnapshotV2AckCallback(t.Context(), result))
		require.NoError(t, callbacks.restoreSnapshotV2AckCallback(t.Context(), result), "a full notification queue cannot hold the durable callback")
		require.EqualValues(t, 10, <-callbacks.notifyIndexChan)
	})
}

func TestRestoreCompactionProofSurvivesTaskGC(t *testing.T) {
	ctx := t.Context()
	copies, m := newCopySegmentTaskTestMeta(t, createTestCopyTask(100, 2001).(*copySegmentTask))
	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting)
	require.NoError(t, copies.AddJob(ctx, job))
	var err error
	m.compactionTaskMeta, err = newCompactionTaskMeta(ctx, m.catalog)
	require.NoError(t, err)
	task := &datapb.CompactionTask{PlanID: 77, TriggerID: 100, RestoreJobId: 100, TerminalState: datapb.CompactionTaskState_completed, State: datapb.CompactionTaskState_cleaned, EndTime: 1}
	require.NoError(t, m.SaveCompactionTask(ctx, task))
	inspector := &compactionInspector{meta: m, restoreJobs: copies}
	inspector.cleanCompactionTaskMeta()
	require.Len(t, m.GetCompactionTasksByTriggerID(ctx, 100), 1)
	require.NoError(t, copies.UpdateJobStateAndReleaseRef(ctx, 100, UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted)))
	inspector.cleanCompactionTaskMeta()
	require.Empty(t, m.GetCompactionTasksByTriggerID(ctx, 100))
}
