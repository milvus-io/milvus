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
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func newForwardRetryTestJob(t *testing.T) (*copySegmentTask, CopySegmentMeta, *meta, *copySegmentJob) {
	t.Helper()
	task := createTestCopyTask(100, 2001).(*copySegmentTask)
	copies, m := newCopySegmentTaskTestMeta(t, task)
	task.copyMeta, task.meta = copies, m
	job := newTestCopyJob(100, datapb.CopySegmentJobState_CopySegmentJobExecuting).(*copySegmentJob)
	job.CreatesCollection = true
	job.IdMappings = task.GetIdMappings()
	job.TotalSegments = int64(len(job.IdMappings))
	job.TimeoutTs = CopyJobTimeoutTs(-time.Hour)
	require.NoError(t, copies.AddJob(t.Context(), job))
	require.NoError(t, m.AddSegment(t.Context(), newTestCopySegment(2001)))
	return task, copies, m, job
}

func TestRestoreRetryWorkerFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		permanent bool
	}{
		{"legacy reason only", nil, false},
		{"storage unavailable", merr.ErrServiceUnavailable, false},
		{"unclassified system error", merr.ErrIoFailed, false},
		{"worker upgrade", merr.ErrServiceUnimplemented, false},
		{"corrupt source", merr.ErrDataIntegrity, true},
		{"missing source object", merr.ErrIoKeyNotFound, true},
		{"missing snapshot", merr.ErrSnapshotNotFound, true},
		{"invalid credentials", merr.ErrIoInvalidCredentials, true},
		{"source access denied", merr.ErrIoPermissionDenied, true},
		{"internal system error", merr.ErrServiceInternal, false},
		{"invalid source", merr.WrapErrParameterInvalidMsg("bad source"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey(tc.name, t, func() {
				task, copies, _, job := newForwardRetryTestJob(t)
				response := &datapb.QueryCopySegmentResponse{State: datapb.CopySegmentTaskState_CopySegmentTaskFailed, Reason: tc.name}
				if tc.err != nil {
					response.FailureStatus = merr.Status(tc.err)
				}
				encoded, err := proto.Marshal(response)
				require.NoError(t, err)
				response = &datapb.QueryCopySegmentResponse{}
				require.NoError(t, proto.Unmarshal(encoded, response))
				mockey.Mock((*copySegmentClusterMock).QueryCopySegment).Return(response, nil).Build()
				drops := 0
				mockey.Mock((*copySegmentClusterMock).DropCopySegment).To(func(_ *copySegmentClusterMock, _, _ int64) error { drops++; return nil }).Build()
				task.QueryTaskOnWorker(&copySegmentClusterMock{})
				if tc.permanent {
					require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, copies.GetJob(t.Context(), job.JobId).GetState())
					require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
					require.Zero(t, drops)
				} else {
					require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(t.Context(), job.JobId).GetState())
					require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
					require.EqualValues(t, NullNodeID, task.GetNodeId())
					require.Equal(t, 1, drops)
				}
			})
		})
	}
}

func TestRestoreRetryWaitsForWorkerRemoval(t *testing.T) {
	mockey.PatchConvey("failed attempt must be removed before reset", t, func() {
		task, copies, _, job := newForwardRetryTestJob(t)
		mockey.Mock((*copySegmentClusterMock).QueryCopySegment).Return(&datapb.QueryCopySegmentResponse{State: datapb.CopySegmentTaskState_CopySegmentTaskFailed}, nil).Build()
		patch := mockey.Mock((*copySegmentClusterMock).DropCopySegment).Return(merr.ErrServiceUnavailable).Build()
		task.QueryTaskOnWorker(&copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
		patch.UnPatch()
		mockey.Mock((*copySegmentClusterMock).DropCopySegment).Return(nil).Build()
		task.QueryTaskOnWorker(&copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(t.Context(), job.JobId).GetState())
	})
}

func TestRestoreRetryCompletedMetadata(t *testing.T) {
	mockey.PatchConvey("same completed result survives transient catalog failure and recovery", t, func() {
		task, copies, m, job := newForwardRetryTestJob(t)
		response := &datapb.QueryCopySegmentResponse{
			State:          datapb.CopySegmentTaskState_CopySegmentTaskCompleted,
			SegmentResults: []*datapb.CopySegmentResult{{SegmentId: 2001, Binlogs: makeTestCopySegmentBinlogs()}},
		}
		mockey.Mock((*copySegmentClusterMock).QueryCopySegment).Return(response, nil).Build()
		patch := mockey.Mock((*meta).UpdateSegmentsInfo).Return(merr.ErrServiceUnavailable).Build()
		task.QueryTaskOnWorker(&copySegmentClusterMock{})
		require.True(t, task.GetCleanupRequired())
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(t.Context(), job.JobId).GetState())
		patch.UnPatch()
		recovered, err := NewCopySegmentMeta(t.Context(), m.catalog, m, nil, nil)
		require.NoError(t, err)
		task = recovered.GetTask(t.Context(), task.GetTaskId()).(*copySegmentTask)
		task.QueryTaskOnWorker(&copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskCompleted, task.GetState())
		require.False(t, task.GetCleanupRequired())
		require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(t.Context(), 2001).GetState())
	})
}

func TestRestoreRetryIgnoresElapsedTimeout(t *testing.T) {
	task, copies, m, job := newForwardRetryTestJob(t)
	checker := &copySegmentChecker{ctx: t.Context(), copyMeta: copies, meta: m}
	checker.tryTimeoutJob(job)
	require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(t.Context(), job.JobId).GetState())
}

func TestRestoreRecipePreflight(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*snapshotstorage.SnapshotData)
		valid  bool
	}{
		{"normal", func(*snapshotstorage.SnapshotData) {}, true},
		{"unknown partition", func(s *snapshotstorage.SnapshotData) { s.Segments[0].PartitionId = 99 }, false},
		{"L0 without partition", func(s *snapshotstorage.SnapshotData) {
			s.Segments[0].PartitionId = -1
			s.Segments[0].SegmentLevel = datapb.SegmentLevel_L0
		}, true},
		{"unknown channel", func(s *snapshotstorage.SnapshotData) { s.Segments[0].ChannelName = "absent" }, false},
		{"duplicate channel", func(s *snapshotstorage.SnapshotData) { s.Collection.VirtualChannelNames = []string{"v1", "v1"} }, false},
		{"duplicate partition", func(s *snapshotstorage.SnapshotData) { s.Collection.Partitions["another"] = 1 }, false},
		{"shard mismatch", func(s *snapshotstorage.SnapshotData) { s.Collection.NumShards = 2 }, false},
		{"missing data", func(s *snapshotstorage.SnapshotData) { s.Segments[0].Binlogs = nil }, false},
		{"manifest only", func(s *snapshotstorage.SnapshotData) {
			s.Segments[0].Binlogs = nil
			s.Segments[0].StorageVersion = storage.StorageV3
			s.Segments[0].ManifestPath = "manifest"
		}, true},
		{"empty", func(s *snapshotstorage.SnapshotData) { s.Segments = nil }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{Partitions: map[string]int64{"_default": 1}, VirtualChannelNames: []string{"v1"}, NumShards: 1}, Segments: []*datapb.SegmentDescription{{SegmentId: 10, PartitionId: 1, ChannelName: "v1", Binlogs: makeTestCopySegmentBinlogs()}}}
			tc.mutate(s)
			if tc.valid {
				require.NoError(t, validateRestoreSnapshotRecipe(s))
			} else {
				// A nil manager cannot reach the broadcaster or reserve collection resources.
				_, err := (*snapshotManager)(nil).broadcastRestoreCollection(context.Background(), s, "snap", 1, "target", "default", 0, false, "", "")
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			}
		})
	}
}

func TestRestorePermanentSourceFailureIsAtomic(t *testing.T) {
	_, copies, _, _ := newForwardRetryTestJob(t)
	sm := &snapshotManager{copySegmentMeta: copies}
	_, err := sm.handleRestoreSourceError(t.Context(), 1, "snap", "", 200, 101, "read source", merr.ErrDataIntegrity, false, 0, &restorePublication{createsCollection: true})
	require.NoError(t, err)
	job := copies.GetJob(t.Context(), 101).(*copySegmentJob)
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, job.GetState())
	require.True(t, job.GetCreatesCollection())
	require.False(t, job.GetExternal())
	require.Contains(t, job.GetReason(), "read source")
	_, err = sm.restoreData(t.Context(), 1, "snap", "", 200, 101, 0, false, "", "", &restorePublication{createsCollection: true})
	require.NoError(t, err)
	require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, copies.GetJob(t.Context(), 101).GetState())
}

func TestRestoreRetrySourcePinDoesNotExpire(t *testing.T) {
	mockey.PatchConvey("indefinite forward retry retains source pin", t, func() {
		var ttl int64 = -1
		mockey.Mock((*snapshotMeta).PinSnapshot).To(func(_ *snapshotMeta, _ context.Context, _ int64, _ string, seconds int64) (int64, int, error) {
			ttl = seconds
			return 42, 1, nil
		}).Build()
		release := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, nil).Build()
		mockey.Mock((*snapshotManager).validateRestoreTargetAbsent).Return(nil).Build()
		mockey.Mock((*snapshotMeta).ReadSnapshotData).Return(nil, merr.ErrServiceUnavailable).Build()
		sm := &snapshotManager{collectionRestorer: &rootcoord.Core{}, snapshotMeta: &snapshotMeta{}}
		lock := func(context.Context, int64, string, string, string) (broadcaster.BroadcastAPI, error) {
			return &mockBroadcastAPI{closeFn: func() {}}, nil
		}
		_, err := sm.RestoreSnapshot(t.Context(), 1, "snapshot", "target", "default", lock, nil, nil, nil)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Zero(t, ttl)
		require.EqualValues(t, 1, release.Times(), "pre-submission errors still release their pin")
	})
}

func TestRestoreRetryUnsupportedWorkerDispatch(t *testing.T) {
	mockey.PatchConvey("rolling upgrade retries an unsupported worker", t, func() {
		task, copies, _, job := newForwardRetryTestJob(t)
		require.NoError(t, copies.UpdateJob(t.Context(), job.JobId, func(j CopySegmentJob) { j.(*copySegmentJob).External = true }))
		require.NoError(t, copies.UpdateTask(t.Context(), task.GetTaskId(), UpdateCopyTaskState(datapb.CopySegmentTaskState_CopySegmentTaskPending)))
		mockey.Mock(AssembleCopySegmentRequest).Return(&datapb.CopySegmentRequest{TaskID: task.GetTaskId()}, nil).Build()
		patch := mockey.Mock((*copySegmentClusterMock).CreateCopySegment).Return(merr.ErrServiceUnimplemented).Build()
		task.CreateTaskOnWorker(42, &copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(t.Context(), job.JobId).GetState())
		patch.UnPatch()
		mockey.Mock((*copySegmentClusterMock).CreateCopySegment).Return(nil).Build()
		task.CreateTaskOnWorker(42, &copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
		require.EqualValues(t, 42, task.GetNodeId())
	})
}

func TestRestoreRetryUnsafeLostResultRequiresManualIntervention(t *testing.T) {
	mockey.PatchConvey("lost worker cannot recreate a partially installed result", t, func() {
		task, copies, _, job := newForwardRetryTestJob(t)
		require.NoError(t, copies.UpdateTask(t.Context(), task.GetTaskId(), updateCopyTaskCleanup(true)))
		mockey.Mock((*copySegmentClusterMock).QueryCopySegment).Return(nil, merr.ErrNodeNotFound).Build()
		task.QueryTaskOnWorker(&copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, copies.GetJob(t.Context(), job.JobId).GetState())
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskFailed, task.GetState())
		require.True(t, task.GetCleanupRequired())
	})
}

func TestRestoreRetryTerminalPinRelease(t *testing.T) {
	mockey.PatchConvey("terminal cleanup resumes a failed unpin before GC", t, func() {
		_, copies, m, job := newForwardRetryTestJob(t)
		require.NoError(t, copies.UpdateJob(t.Context(), job.JobId, UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobCompleted), func(j CopySegmentJob) { j.(*copySegmentJob).PinId = 42 }))
		job = copies.GetJob(t.Context(), job.JobId).(*copySegmentJob)
		m.snapshotMeta = &snapshotMeta{}
		checker := &copySegmentChecker{ctx: t.Context(), copyMeta: copies, meta: m}
		release := mockey.Mock((*snapshotMeta).UnpinSnapshot).Return(int64(0), "", 0, merr.ErrServiceUnavailable).Build()
		checker.checkGC(job)
		require.EqualValues(t, 1, release.Times())
		require.NotNil(t, copies.GetJob(t.Context(), job.JobId))
		release.Return(int64(1), "snapshot", 0, nil)
		checker.checkGC(job)
		require.EqualValues(t, 1, release.Times())
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobCompleted, copies.GetJob(t.Context(), job.JobId).GetState())
	})
}

func TestRestoreRetryCannotRedispatchAdmittedFailure(t *testing.T) {
	mockey.PatchConvey("failed worker cannot overwrite an admitted result", t, func() {
		task, copies, _, job := newForwardRetryTestJob(t)
		require.NoError(t, copies.UpdateTask(t.Context(), task.GetTaskId(), updateCopyTaskCleanup(true)))
		mockey.Mock((*copySegmentClusterMock).QueryCopySegment).Return(&datapb.QueryCopySegmentResponse{State: datapb.CopySegmentTaskState_CopySegmentTaskFailed}, nil).Build()
		task.QueryTaskOnWorker(&copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, copies.GetJob(t.Context(), job.JobId).GetState())
		require.Contains(t, task.GetReason(), "manual intervention")
	})
}

func TestRestoreRetryRescheduleSaveFailure(t *testing.T) {
	mockey.PatchConvey("failed reset is recovered from a confirmed missing worker", t, func() {
		task, copies, _, job := newForwardRetryTestJob(t)
		query := mockey.Mock((*copySegmentClusterMock).QueryCopySegment).Return(&datapb.QueryCopySegmentResponse{State: datapb.CopySegmentTaskState_CopySegmentTaskFailed}, nil).Build()
		mockey.Mock((*copySegmentClusterMock).DropCopySegment).Return(nil).Build()
		save := mockey.Mock((*copySegmentMeta).UpdateTask).Return(merr.ErrServiceUnavailable).Build()
		task.QueryTaskOnWorker(&copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskInProgress, task.GetState())
		save.UnPatch()
		query.Return(nil, merr.ErrNodeNotFound)
		task.QueryTaskOnWorker(&copySegmentClusterMock{})
		require.Equal(t, datapb.CopySegmentTaskState_CopySegmentTaskPending, task.GetState())
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobExecuting, copies.GetJob(t.Context(), job.JobId).GetState())
	})
}

func TestRestoreRegistrationFailureClassification(t *testing.T) {
	for _, sourceErr := range []error{merr.ErrServiceUnavailable, merr.ErrDataIntegrity} {
		mockey.PatchConvey(sourceErr.Error(), t, func() {
			_, copies, m, job := newForwardRetryTestJob(t)
			require.NoError(t, copies.UpdateJob(t.Context(), job.JobId, UpdateCopyJobState(datapb.CopySegmentJobState_CopySegmentJobPending)))
			checker := &copySegmentChecker{ctx: t.Context(), copyMeta: copies, meta: m}
			mockey.Mock((*copySegmentChecker).checkRestoreTarget).Return(true).Build()
			mockey.Mock((*copySegmentChecker).registerRestoreSegments).Return(sourceErr).Build()
			checker.checkPendingJob(copies.GetJob(t.Context(), job.JobId))
			expected := datapb.CopySegmentJobState_CopySegmentJobPending
			if sourceErr == merr.ErrDataIntegrity {
				expected = datapb.CopySegmentJobState_CopySegmentJobFailed
			}
			require.Equal(t, expected, copies.GetJob(t.Context(), job.JobId).GetState())
		})
	}
}

func TestRestorePermanentSourceFailureRetriesUnpin(t *testing.T) {
	mockey.PatchConvey("failed source job remains Failed while callback retries unpin", t, func() {
		_, copies, _, _ := newForwardRetryTestJob(t)
		sm := &snapshotManager{copySegmentMeta: copies}
		release := mockey.Mock((*snapshotManager).UnpinSnapshotData).Return(merr.ErrServiceUnavailable).Build()
		_, err := sm.handleRestoreSourceError(t.Context(), 1, "snap", "", 200, 101, "read source", merr.ErrDataIntegrity, false, 42, &restorePublication{createsCollection: true})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, copies.GetJob(t.Context(), 101).GetState())
		release.Return(nil)
		_, err = sm.restoreData(t.Context(), 1, "snap", "", 200, 101, 42, false, "", "", &restorePublication{createsCollection: true})
		require.NoError(t, err)
		require.EqualValues(t, 1, release.Times())
		require.Equal(t, datapb.CopySegmentJobState_CopySegmentJobFailed, copies.GetJob(t.Context(), 101).GetState())
	})
}
