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
	"path"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	mocks2 "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

// TestReshardTaskWaitsForPreReshardingJobState pins the fix for the
// AssigningIDRange race: a ReshardTask is persisted just before the job
// advances to Resharding (createReshardTasks), so the async
// dispatch loop can observe it while the job is still PreImporting or
// AssigningIDRange. The task must wait (stay Pending) for a later tick instead
// of failing the whole job.
func TestReshardTaskWaitsForPreReshardingJobState(t *testing.T) {
	for _, state := range []internalpb.ImportJobState{
		internalpb.ImportJobState_PreImporting,
		internalpb.ImportJobState_AssigningIDRange,
	} {
		t.Run(state.String(), func(t *testing.T) {
			importMeta := NewMockImportMeta(t)
			job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: state}}
			importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
			task := newReshardTask(&datapb.ReshardTask{
				JobId: 1, TaskId: 10, CollectionId: 2, RunId: 1,
				State: datapb.ImportTaskStateV2_Pending,
			}, importMeta, nil, nil)

			// Must not call fail(): that would hit UpdateTask/UpdateJob, which the
			// mock is not set up for, and the mock would fail the test.
			task.CreateTaskOnWorker(1, nil)
			require.Equal(t, datapb.ImportTaskStateV2_Pending, task.GetState())
		})
	}
}

// writeReshardManifest writes a serialized reshard manifest at the task's result
// path so acceptResult can read it back.
func writeReshardManifest(t *testing.T, cm storage.ChunkManager, jobID, taskID, runID int64, manifest *importv3pb.ReshardManifest) {
	t.Helper()
	data, err := proto.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, cm.Write(context.Background(),
		path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(jobID, taskID, runID)), data))
}

// TestReshardTaskQueryAcceptsManifestAndWritesCompleted pins the happy path: a
// Completed reply whose manifest validates is accepted and the task records the
// Completed marker with a completion time.
func TestReshardTaskQueryAcceptsManifestAndWritesCompleted(t *testing.T) {
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	writeReshardManifest(t, cm, 1, 10, 1, &importv3pb.ReshardManifest{
		Fragments: []*importv3pb.FragmentDescriptor{{Path: "frag-0-0-0.parquet", Rows: 5, LogicalBytes: 100, VchannelIndex: 0, PartitionId: 3}},
	})

	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Resharding}}
	task := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress, RunId: 1, NodeId: 5,
	}, importMeta, &meta{chunkManager: cm}, nil)

	cluster.EXPECT().QueryReshard(int64(5), mock.Anything).Return(&datapb.QueryReshardTaskResponse{
		Status: merr.Success(), State: datapb.ImportTaskStateV2_Completed,
	}, nil).Once()
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ int64, actions ...UpdateAction) {
			clone := task.Clone().(*reshardTask)
			for _, action := range actions {
				action(clone)
			}
			require.Equal(t, datapb.ImportTaskStateV2_Completed, clone.GetState())
			require.NotEmpty(t, clone.GetCompleteTime())
		}).Return(nil).Once()

	task.QueryTaskOnWorker(cluster)
}

// TestReshardTaskQueryCompletedAfterJobLeftReshardingIsNoOp pins the late-result
// guard: once the job has moved out of Resharding, a Completed reply is a no-op
// (the strict mock has no UpdateTask/accept expectation).
func TestReshardTaskQueryCompletedAfterJobLeftReshardingIsNoOp(t *testing.T) {
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Planning}}
	task := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress, RunId: 1, NodeId: 5,
	}, importMeta, &meta{chunkManager: mocks2.NewChunkManager(t)}, nil)

	cluster.EXPECT().QueryReshard(int64(5), mock.Anything).Return(&datapb.QueryReshardTaskResponse{
		Status: merr.Success(), State: datapb.ImportTaskStateV2_Completed,
	}, nil).Once()
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()

	require.NotPanics(t, func() { task.QueryTaskOnWorker(cluster) })
	require.Equal(t, datapb.ImportTaskStateV2_InProgress, task.GetState())
}

// TestReshardTaskQueryRetryAndFailed pins the Retry/Failed dispatch: a Retry
// reply re-enters prepareRetry, a Failed reply marks the task failed with the
// worker reason.
func TestReshardTaskQueryRetryAndFailed(t *testing.T) {
	t.Run("retry prepares a fresh run", func(t *testing.T) {
		importMeta := NewMockImportMeta(t)
		cluster := session.NewMockCluster(t)
		job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Resharding}}
		task := newReshardTask(&datapb.ReshardTask{
			JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress, RunId: 1, NodeId: 5,
		}, importMeta, nil, nil)

		cluster.EXPECT().QueryReshard(int64(5), mock.Anything).Return(&datapb.QueryReshardTaskResponse{
			Status: merr.Success(), State: datapb.ImportTaskStateV2_Retry,
		}, nil).Once()
		importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
		cluster.EXPECT().DropReshard(int64(5), mock.Anything).Return(nil).Once()
		importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, _ int64, actions ...UpdateAction) {
				clone := task.Clone().(*reshardTask)
				for _, action := range actions {
					action(clone)
				}
				require.Equal(t, int64(2), clone.task.Load().GetRunId())
				require.Equal(t, int64(NullNodeID), clone.GetNodeID())
				require.Equal(t, datapb.ImportTaskStateV2_Pending, clone.GetState())
			}).Return(nil).Once()

		task.QueryTaskOnWorker(cluster)
	})

	t.Run("failed marks the task failed", func(t *testing.T) {
		importMeta := NewMockImportMeta(t)
		cluster := session.NewMockCluster(t)
		task := newReshardTask(&datapb.ReshardTask{
			JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress, RunId: 1, NodeId: 5,
		}, importMeta, nil, nil)

		cluster.EXPECT().QueryReshard(int64(5), mock.Anything).Return(&datapb.QueryReshardTaskResponse{
			Status: merr.Success(), State: datapb.ImportTaskStateV2_Failed, Reason: "reshard blew up",
		}, nil).Once()
		importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything, mock.Anything).
			Run(func(_ context.Context, _ int64, actions ...UpdateAction) {
				clone := task.Clone().(*reshardTask)
				for _, action := range actions {
					action(clone)
				}
				require.Equal(t, datapb.ImportTaskStateV2_Failed, clone.GetState())
				require.Contains(t, clone.GetReason(), "reshard blew up")
			}).Return(nil).Once()

		task.QueryTaskOnWorker(cluster)
	})
}

// TestReshardTaskAcceptResultClassification pins acceptResult's error classes: a
// missing storage and an unparseable manifest are terminal, an object-store read
// failure is transient, and a validating manifest is accepted.
func TestReshardTaskAcceptResultClassification(t *testing.T) {
	t.Run("storage unavailable is terminal", func(t *testing.T) {
		task := newReshardTask(&datapb.ReshardTask{JobId: 1, TaskId: 10, RunId: 1}, NewMockImportMeta(t), nil, nil)
		err := task.acceptResult()
		require.Error(t, err)
		require.True(t, importcommon.IsTerminalImportV3Err(err))
	})

	t.Run("read failure is transient", func(t *testing.T) {
		cm := mocks2.NewChunkManager(t)
		cm.EXPECT().RootPath().Return("root").Maybe()
		cm.EXPECT().Read(mock.Anything, mock.Anything).Return(nil, context.DeadlineExceeded).Once()
		task := newReshardTask(&datapb.ReshardTask{JobId: 1, TaskId: 10, RunId: 1}, NewMockImportMeta(t), &meta{chunkManager: cm}, nil)
		err := task.acceptResult()
		require.Error(t, err)
		require.False(t, importcommon.IsTerminalImportV3Err(err))
	})

	t.Run("unparseable manifest is terminal", func(t *testing.T) {
		cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		require.NoError(t, cm.Write(context.Background(),
			path.Join(cm.RootPath(), metautil.BuildImportReshardResultPath(1, 10, 1)), []byte("\xff\xff\xff")))
		task := newReshardTask(&datapb.ReshardTask{JobId: 1, TaskId: 10, RunId: 1}, NewMockImportMeta(t), &meta{chunkManager: cm}, nil)
		err := task.acceptResult()
		require.Error(t, err)
		require.True(t, importcommon.IsTerminalImportV3Err(err))
	})

	t.Run("valid manifest accepted", func(t *testing.T) {
		cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
		writeReshardManifest(t, cm, 1, 10, 1, &importv3pb.ReshardManifest{
			Fragments: []*importv3pb.FragmentDescriptor{{Path: "frag.parquet", Rows: 5, LogicalBytes: 100, VchannelIndex: 0, PartitionId: 3}},
		})
		task := newReshardTask(&datapb.ReshardTask{JobId: 1, TaskId: 10, RunId: 1}, NewMockImportMeta(t), &meta{chunkManager: cm}, nil)
		require.NoError(t, task.acceptResult())
	})
}

// TestReshardTaskPrepareRetryResetsProgress pins prepareRetry: it increments the
// run, clears the node binding, returns the task to Pending, and resets the
// transient per-source progress.
func TestReshardTaskPrepareRetryResetsProgress(t *testing.T) {
	importMeta := NewMockImportMeta(t)
	cluster := session.NewMockCluster(t)
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, CollectionID: 2, State: internalpb.ImportJobState_Resharding}}
	task := newReshardTask(&datapb.ReshardTask{
		JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress, RunId: 1, NodeId: 5,
		Reason: "stale",
	}, importMeta, nil, nil)
	task.setSourceProgress([]*datapb.ReshardSourceProgress{{FileId: 1, HashedRows: 7}})

	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job).Once()
	cluster.EXPECT().DropReshard(int64(5), mock.Anything).Return(nil).Once()
	importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, _ int64, actions ...UpdateAction) {
			clone := task.Clone().(*reshardTask)
			for _, action := range actions {
				action(clone)
			}
			require.Equal(t, int64(2), clone.task.Load().GetRunId())
			require.Equal(t, int64(NullNodeID), clone.GetNodeID())
			require.Equal(t, datapb.ImportTaskStateV2_Pending, clone.GetState())
			require.Empty(t, clone.GetReason())
		}).Return(nil).Once()

	task.prepareRetry(cluster)
	require.Empty(t, task.getSourceProgress(), "the retry must drop the superseded run's progress")
}

// TestReshardTaskDropTaskOnWorker pins the drop contract: a missing node is
// ownership loss and still unbinds the task, an unexpected drop error keeps the
// binding, and an already-unbound task just re-clears the node.
func TestReshardTaskDropTaskOnWorker(t *testing.T) {
	newTask := func(importMeta ImportMeta, nodeID int64) *reshardTask {
		return newReshardTask(&datapb.ReshardTask{
			JobId: 1, TaskId: 10, CollectionId: 2, State: datapb.ImportTaskStateV2_InProgress, NodeId: nodeID,
		}, importMeta, nil, nil)
	}

	t.Run("node gone unbinds", func(t *testing.T) {
		importMeta := NewMockImportMeta(t)
		cluster := session.NewMockCluster(t)
		task := newTask(importMeta, 5)
		cluster.EXPECT().DropReshard(int64(5), mock.Anything).Return(merr.ErrNodeNotFound).Once()
		importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything).Return(nil).Once()

		task.DropTaskOnWorker(cluster)
	})

	t.Run("drop error keeps binding", func(t *testing.T) {
		importMeta := NewMockImportMeta(t)
		cluster := session.NewMockCluster(t)
		task := newTask(importMeta, 5)
		cluster.EXPECT().DropReshard(int64(5), mock.Anything).Return(merr.WrapErrServiceUnavailable("transient")).Once()
		// No UpdateTask expectation: a transient drop error must not unbind.

		task.DropTaskOnWorker(cluster)
	})

	t.Run("unbound re-clears node", func(t *testing.T) {
		importMeta := NewMockImportMeta(t)
		task := newTask(importMeta, NullNodeID)
		importMeta.EXPECT().UpdateTask(mock.Anything, int64(10), mock.Anything).Return(nil).Once()

		task.DropTaskOnWorker(session.NewMockCluster(t))
	})
}
