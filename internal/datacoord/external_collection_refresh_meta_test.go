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
	"crypto/sha256"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ==================== Helper Functions for Meta Tests ====================

func createMetaTestRefreshMeta(t *testing.T, jobs []*datapb.ExternalCollectionRefreshJob, tasks []*datapb.ExternalCollectionRefreshTask) *externalCollectionRefreshMeta {
	catalog := &stubCatalog{
		jobs:  jobs,
		tasks: tasks,
	}
	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)
	return meta
}

func createMetaTestRefreshResultStore(t *testing.T) (*externalCollectionRefreshResultStore, *storage.LocalChunkManager) {
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	return newExternalCollectionRefreshResultStore(chunkManager), chunkManager
}

type controlledRefreshCatalog struct {
	*stubCatalog
	onUpdate   func()
	onTaskSave func()
}

func (c *controlledRefreshCatalog) Update(ctx context.Context, actions ...metastore.UpdateAction) error {
	if c.onUpdate != nil {
		c.onUpdate()
	}
	return c.stubCatalog.Update(ctx, actions...)
}

func (c *controlledRefreshCatalog) SaveExternalCollectionRefreshTask(
	ctx context.Context,
	task *datapb.ExternalCollectionRefreshTask,
) error {
	if c.onTaskSave != nil {
		c.onTaskSave()
	}
	return nil
}

// ==================== Test Functions ====================

func TestExternalCollectionRefreshResultStore(t *testing.T) {
	ctx := context.Background()
	assert.Nil(t, newExternalCollectionRefreshResultStore(nil))
	resultStore, chunkManager := createMetaTestRefreshResultStore(t)
	task := &datapb.ExternalCollectionRefreshTask{
		CollectionId: 100,
		JobId:        1,
		TaskId:       1001,
		Version:      3,
	}
	updatedSegment := &datapb.SegmentInfo{ID: 10, CollectionID: 100, NumOfRows: 7}

	resultRef, err := resultStore.Save(ctx, task, []int64{1, 2}, []*datapb.SegmentInfo{updatedSegment})
	assert.NoError(t, err)
	assert.Len(t, resultRef.checksum, sha256.Size)
	assert.Contains(t, resultRef.path, path.Join(externalRefreshTaskResultRoot, "100", "1", "1001", "3"))

	updatedSegment.NumOfRows = 99
	storedTask := &datapb.ExternalCollectionRefreshTask{
		CollectionId:   task.GetCollectionId(),
		JobId:          task.GetJobId(),
		TaskId:         task.GetTaskId(),
		Version:        task.GetVersion(),
		ResultPath:     resultRef.path,
		ResultChecksum: resultRef.checksum,
	}
	// Version is retained only for compatibility. A different persisted value
	// must not reject a result whose task identity is otherwise exact.
	storedTask.Version = 9
	result, err := resultStore.Load(ctx, storedTask)
	assert.NoError(t, err)
	assert.Equal(t, int64(3), result.GetTaskVersion())
	assert.Equal(t, []int64{1, 2}, result.GetKeptSegments())
	assert.Equal(t, int64(7), result.GetUpdatedSegments()[0].GetNumOfRows())
	cloneStoredTask := func() *datapb.ExternalCollectionRefreshTask {
		return proto.Clone(storedTask).(*datapb.ExternalCollectionRefreshTask)
	}

	t.Run("checksum_mismatch", func(t *testing.T) {
		checksum := append([]byte(nil), resultRef.checksum...)
		checksum[0] ^= 0xff
		corruptTask := cloneStoredTask()
		corruptTask.ResultChecksum = checksum

		_, err := resultStore.Load(ctx, corruptTask)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("identity_mismatch", func(t *testing.T) {
		foreignTask := cloneStoredTask()
		foreignTask.JobId = 2

		_, err := resultStore.Load(ctx, foreignTask)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("invalid_reference", func(t *testing.T) {
		emptyPathTask := cloneStoredTask()
		emptyPathTask.ResultPath = ""
		_, err := resultStore.Load(ctx, emptyPathTask)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)

		shortChecksumTask := cloneStoredTask()
		shortChecksumTask.ResultChecksum = []byte{1}
		_, err = resultStore.Load(ctx, shortChecksumTask)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("read_failed", func(t *testing.T) {
		missingTask := cloneStoredTask()
		missingTask.ResultPath = path.Join(chunkManager.RootPath(), "missing.pb")
		_, err := resultStore.Load(ctx, missingTask)
		assert.Error(t, err)
	})

	t.Run("invalid_proto", func(t *testing.T) {
		payload := []byte{0xff}
		checksum := sha256.Sum256(payload)
		invalidPath := path.Join(chunkManager.RootPath(), "invalid.pb")
		assert.NoError(t, chunkManager.Write(ctx, invalidPath, payload))
		invalidTask := cloneStoredTask()
		invalidTask.ResultPath = invalidPath
		invalidTask.ResultChecksum = checksum[:]

		_, err := resultStore.Load(ctx, invalidTask)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("marshal_failed", func(t *testing.T) {
		_, err := resultStore.Save(ctx, task, nil, []*datapb.SegmentInfo{{
			ManifestPath: string([]byte{0xff}),
		}})
		assert.ErrorIs(t, err, merr.ErrSerializationFailed)
	})

	t.Run("remove", func(t *testing.T) {
		assert.NoError(t, resultStore.Remove(ctx, ""))
		removeTask := &datapb.ExternalCollectionRefreshTask{
			CollectionId: 100,
			JobId:        3,
			TaskId:       3001,
			Version:      1,
		}
		removeRef, err := resultStore.Save(ctx, removeTask, nil, nil)
		assert.NoError(t, err)
		assert.NoError(t, resultStore.Remove(ctx, removeRef.path))
		exists, err := chunkManager.Exist(ctx, removeRef.path)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("remove_failed", func(t *testing.T) {
		mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).
			Return(merr.WrapErrIoFailed("result", errors.New("remove failed"))).
			Build()
		defer mockRemove.UnPatch()

		err := resultStore.Remove(ctx, resultRef.path)
		assert.Error(t, err)
	})

	t.Run("remove_job_prefix_failed", func(t *testing.T) {
		mockRemovePrefix := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).
			Return(merr.WrapErrIoFailed("result", errors.New("remove prefix failed"))).
			Build()
		defer mockRemovePrefix.UnPatch()

		err := resultStore.RemoveJob(ctx, 100, 10)
		assert.Error(t, err)
	})

	t.Run("remove_job_root_failed", func(t *testing.T) {
		rootTask := &datapb.ExternalCollectionRefreshTask{
			CollectionId: 100,
			JobId:        10,
			TaskId:       10001,
			Version:      1,
		}
		rootRef, err := resultStore.Save(ctx, rootTask, nil, nil)
		assert.NoError(t, err)
		assert.NoError(t, resultStore.Remove(ctx, rootRef.path))

		mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).
			Return(merr.WrapErrIoFailed("result", errors.New("remove root failed"))).
			Build()
		defer mockRemove.UnPatch()

		err = resultStore.RemoveJob(ctx, 100, 10)
		assert.Error(t, err)
	})

	t.Run("remove_job_prefix", func(t *testing.T) {
		secondTask := &datapb.ExternalCollectionRefreshTask{
			CollectionId: 100,
			JobId:        1,
			TaskId:       1002,
			Version:      1,
		}
		secondRef, err := resultStore.Save(ctx, secondTask, nil, nil)
		assert.NoError(t, err)
		otherJobTask := &datapb.ExternalCollectionRefreshTask{
			CollectionId: 100,
			JobId:        2,
			TaskId:       2001,
			Version:      1,
		}
		otherJobRef, err := resultStore.Save(ctx, otherJobTask, nil, nil)
		assert.NoError(t, err)

		assert.NoError(t, resultStore.RemoveJob(ctx, 100, 1))
		firstExists, err := chunkManager.Exist(ctx, resultRef.path)
		assert.NoError(t, err)
		assert.False(t, firstExists)
		secondExists, err := chunkManager.Exist(ctx, secondRef.path)
		assert.NoError(t, err)
		assert.False(t, secondExists)
		otherJobExists, err := chunkManager.Exist(ctx, otherJobRef.path)
		assert.NoError(t, err)
		assert.True(t, otherJobExists)
	})
}

func TestExternalCollectionRefreshMeta_NewMeta(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, CollectionName: "test_collection"},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100},
		}
		catalog := &stubCatalog{jobs: jobs, tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)
		assert.NotNil(t, meta)

		// Verify job loaded
		job := meta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, int64(1), job.GetJobId())
		assert.Equal(t, int64(100), job.GetCollectionId())

		// Verify task loaded
		task := meta.GetTask(1001)
		assert.NotNil(t, task)
		assert.Equal(t, int64(1001), task.GetTaskId())
	})

	t.Run("list_jobs_failed", func(t *testing.T) {
		catalog := &stubCatalog{}

		// Mock ListExternalCollectionRefreshJobs to return error
		mockList := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, errors.New("list jobs error")).Build()
		defer mockList.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.Error(t, err)
		assert.Nil(t, meta)
	})

	t.Run("list_tasks_failed", func(t *testing.T) {
		catalog := &stubCatalog{}

		// Mock ListExternalCollectionRefreshTasks to return error
		mockList := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, errors.New("list tasks error")).Build()
		defer mockList.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.Error(t, err)
		assert.Nil(t, meta)
	})
}

func TestExternalCollectionRefreshMeta_AddJob(t *testing.T) {
	ctx := context.Background()

	t.Run("success", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		job := &datapb.ExternalCollectionRefreshJob{
			JobId:          1,
			CollectionId:   100,
			CollectionName: "test_collection",
			State:          indexpb.JobState_JobStateInit,
			StartTime:      time.Now().UnixMilli(),
		}

		err := meta.AddJob(job)
		assert.NoError(t, err)

		// Verify job added
		got := meta.GetJob(1)
		assert.NotNil(t, got)
		assert.Equal(t, int64(1), got.GetJobId())
		assert.Equal(t, int64(100), got.GetCollectionId())
	})

	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		// Mock SaveExternalCollectionRefreshJob to return error
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		job := &datapb.ExternalCollectionRefreshJob{
			JobId:        1,
			CollectionId: 100,
		}

		err = meta.AddJob(job)
		assert.Error(t, err)

		// Verify job not added
		got := meta.GetJob(1)
		assert.Nil(t, got)
	})
}

func TestExternalCollectionRefreshMeta_GetJob(t *testing.T) {
	ctx := context.Background()

	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, CollectionName: "test_collection"},
	}
	catalog := &stubCatalog{jobs: jobs}
	meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
	assert.NoError(t, err)

	t.Run("job_exists", func(t *testing.T) {
		job := meta.GetJob(1)
		assert.NotNil(t, job)
		assert.Equal(t, int64(1), job.GetJobId())
	})

	t.Run("job_not_exists", func(t *testing.T) {
		job := meta.GetJob(999)
		assert.Nil(t, job)
	})
}

func TestExternalCollectionRefreshMeta_GetActiveJobByCollectionID(t *testing.T) {
	now := time.Now().UnixMilli()
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, StartTime: now - 1000},
		{JobId: 2, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, StartTime: now},
		{JobId: 3, CollectionId: 100, State: indexpb.JobState_JobStateFinished, StartTime: now + 1000},
		{JobId: 4, CollectionId: 200, State: indexpb.JobState_JobStateInit, StartTime: now},
	}
	meta := createMetaTestRefreshMeta(t, jobs, nil)

	t.Run("has_active_job", func(t *testing.T) {
		job := meta.GetActiveJobByCollectionID(100)
		assert.NotNil(t, job)
		// Should return the newest active job (jobId=2 with InProgress state)
		assert.Equal(t, int64(2), job.GetJobId())
	})

	t.Run("no_active_job", func(t *testing.T) {
		// Collection 300 has no jobs
		job := meta.GetActiveJobByCollectionID(300)
		assert.Nil(t, job)
	})
}

func TestExternalCollectionRefreshMeta_ListJobsByCollectionID(t *testing.T) {
	now := time.Now().UnixMilli()
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, StartTime: now - 2000},
		{JobId: 2, CollectionId: 100, StartTime: now - 1000},
		{JobId: 3, CollectionId: 100, StartTime: now},
		{JobId: 4, CollectionId: 200, StartTime: now},
	}
	meta := createMetaTestRefreshMeta(t, jobs, nil)

	t.Run("all_jobs", func(t *testing.T) {
		jobs := meta.ListJobsByCollectionID(100)
		assert.Len(t, jobs, 3)
		// Should be sorted by StartTime descending
		assert.Equal(t, int64(3), jobs[0].GetJobId())
		assert.Equal(t, int64(2), jobs[1].GetJobId())
		assert.Equal(t, int64(1), jobs[2].GetJobId())
	})

	t.Run("no_jobs", func(t *testing.T) {
		jobs := meta.ListJobsByCollectionID(300)
		assert.Nil(t, jobs)
	})
}

func TestExternalCollectionRefreshMeta_GetAllJobs(t *testing.T) {
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, StartTime: 100},
		{JobId: 2, CollectionId: 200, StartTime: 200},
	}
	meta := createMetaTestRefreshMeta(t, jobs, nil)

	allJobs := meta.GetAllJobs()
	assert.Len(t, allJobs, 2)
	assert.NotNil(t, allJobs[1])
	assert.NotNil(t, allJobs[2])

	listedJobs := meta.ListAllJobs()
	assert.Len(t, listedJobs, 2)
	assert.Equal(t, int64(2), listedJobs[0].GetJobId())
	assert.Equal(t, int64(1), listedJobs[1].GetJobId())
}

func TestExternalCollectionRefreshMeta_UpdateJobState(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Mock save to fail
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
		assert.False(t, applied)

		// State should remain Init
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetJob(1).GetState())
	})

	t.Run("terminal_save_failed_fails_stop", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		writeErr := errors.New("ambiguous catalog response")
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(writeErr).Build()
		defer mockSave.UnPatch()
		var fatalCalled atomic.Bool
		var fatalHeldJobLock atomic.Bool
		mockFatal := mockey.Mock(mlog.Fatal).
			To(func(context.Context, string, ...mlog.Field) {
				fatalCalled.Store(true)
				if meta.jobLock.TryLock(100) {
					meta.jobLock.Unlock(100)
					return
				}
				fatalHeldJobLock.Store(true)
			}).
			Build()
		defer mockFatal.UnPatch()

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateFailed, "timeout")
		assert.ErrorIs(t, err, writeErr)
		assert.False(t, applied)
		assert.True(t, fatalCalled.Load())
		assert.True(t, fatalHeldJobLock.Load())
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())
	})

	t.Run("terminal_save_failed_during_shutdown_does_not_fail_stop", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		componentCtx, cancel := context.WithCancel(context.Background())
		cancel()
		meta, err := newExternalCollectionRefreshMeta(componentCtx, catalog)
		assert.NoError(t, err)

		writeErr := errors.New("catalog error during shutdown")
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(writeErr).Build()
		defer mockSave.UnPatch()
		var fatalCalled atomic.Bool
		mockFatal := mockey.Mock(mlog.Fatal).
			To(func(context.Context, string, ...mlog.Field) { fatalCalled.Store(true) }).
			Build()
		defer mockFatal.UnPatch()

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateFinished, "")
		assert.ErrorIs(t, err, writeErr)
		assert.False(t, applied)
		assert.False(t, fatalCalled.Load())
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetJob(1).GetState())
	})

	t.Run("success", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateInProgress, "")
		assert.NoError(t, err)
		assert.True(t, applied)

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, job.GetState())
	})

	t.Run("finished_sets_end_time_and_progress", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateFinished, "")
		assert.NoError(t, err)
		assert.True(t, applied)

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFinished, job.GetState())
		assert.Equal(t, int64(100), job.GetProgress())
		assert.Greater(t, job.GetEndTime(), int64(0))
	})

	t.Run("failed_sets_end_time_and_reason", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateFailed, "timeout")
		assert.NoError(t, err)
		assert.True(t, applied)

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFailed, job.GetState())
		assert.Equal(t, "timeout", job.GetFailReason())
		assert.Greater(t, job.GetEndTime(), int64(0))
	})

	t.Run("terminal_state_guard_skips_write", func(t *testing.T) {
		// Once a job is Finished, a follow-up UpdateJobState(Failed) must
		// NOT persist the transition and MUST return applied=false so the
		// caller can distinguish "silently skipped" from "persisted".
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFinished, Progress: 100},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		applied, err := meta.UpdateJobState(1, indexpb.JobState_JobStateFailed, "timeout")
		assert.NoError(t, err)
		assert.False(t, applied, "terminal-state guard must report applied=false")

		job := meta.GetJob(1)
		assert.Equal(t, indexpb.JobState_JobStateFinished, job.GetState(), "state must remain Finished")
	})

	t.Run("job_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		applied, err := meta.UpdateJobState(999, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
		assert.False(t, applied)
	})
}

func TestExternalCollectionRefreshMeta_UpdateJobProgress(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, Progress: 0},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(nil, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshJob).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateJobProgress(1, 50)
		assert.Error(t, err)
		assert.Equal(t, int64(0), meta.GetJob(1).GetProgress())
	})

	t.Run("success", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100, Progress: 0},
		}
		meta := createMetaTestRefreshMeta(t, jobs, nil)

		err := meta.UpdateJobProgress(1, 50)
		assert.NoError(t, err)

		job := meta.GetJob(1)
		assert.Equal(t, int64(50), job.GetProgress())
	})

	t.Run("job_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateJobProgress(999, 50)
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_AddTasksToJob(t *testing.T) {
	newTask := func(taskID int64) *datapb.ExternalCollectionRefreshTask {
		return &datapb.ExternalCollectionRefreshTask{TaskId: taskID, JobId: 1, CollectionId: 100}
	}

	t.Run("success", func(t *testing.T) {
		catalog := &stubCatalog{jobs: []*datapb.ExternalCollectionRefreshJob{{
			JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit,
		}}}
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		err = meta.AddTasksToJob(1, []*datapb.ExternalCollectionRefreshTask{newTask(1001), newTask(1002)})
		assert.NoError(t, err)
		assert.Equal(t, []int64{1001, 1002}, meta.GetJob(1).GetTaskIds())
		assert.NotNil(t, meta.GetTask(1001))
		assert.NotNil(t, meta.GetTask(1002))
		assert.Len(t, catalog.updateActions, 1)
		assert.Len(t, catalog.updateActions[0], 3)
	})

	t.Run("catalog_update_failed", func(t *testing.T) {
		catalog := &stubCatalog{
			jobs: []*datapb.ExternalCollectionRefreshJob{{
				JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit,
			}},
			updateErr: errors.New("save task plan failed"),
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		err = meta.AddTasksToJob(1, []*datapb.ExternalCollectionRefreshTask{newTask(1001)})
		assert.ErrorContains(t, err, "save task plan failed")
		assert.False(t, errors.Is(err, errExternalRefreshTaskPlanNotPublishable))
		assert.Empty(t, meta.GetJob(1).GetTaskIds())
		assert.Nil(t, meta.GetTask(1001))
	})

	t.Run("terminal_job", func(t *testing.T) {
		catalog := &stubCatalog{jobs: []*datapb.ExternalCollectionRefreshJob{{
			JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed,
		}}}
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		err = meta.AddTasksToJob(1, []*datapb.ExternalCollectionRefreshTask{newTask(1001)})
		assert.ErrorContains(t, err, "state JobStateFailed")
		assert.True(t, errors.Is(err, errExternalRefreshTaskPlanNotPublishable))
		assert.Empty(t, catalog.updateActions)
		assert.Empty(t, meta.GetJob(1).GetTaskIds())
		assert.Nil(t, meta.GetTask(1001))
	})

	t.Run("empty_plan", func(t *testing.T) {
		catalog := &stubCatalog{jobs: []*datapb.ExternalCollectionRefreshJob{{
			JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit,
		}}}
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		err = meta.AddTasksToJob(1, nil)
		assert.ErrorContains(t, err, "empty task plan")
		assert.True(t, errors.Is(err, errExternalRefreshTaskPlanNotPublishable))
		assert.Empty(t, catalog.updateActions)
	})

	t.Run("already_published", func(t *testing.T) {
		existingTask := newTask(1001)
		catalog := &stubCatalog{
			jobs: []*datapb.ExternalCollectionRefreshJob{{
				JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInit, TaskIds: []int64{1001},
			}},
			tasks: []*datapb.ExternalCollectionRefreshTask{existingTask},
		}
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		err = meta.AddTasksToJob(1, []*datapb.ExternalCollectionRefreshTask{newTask(1002)})
		assert.ErrorContains(t, err, "already has a published task plan")
		assert.True(t, errors.Is(err, errExternalRefreshTaskPlanNotPublishable))
		assert.Empty(t, catalog.updateActions)
		assert.Equal(t, []int64{1001}, meta.GetJob(1).GetTaskIds())
		assert.Nil(t, meta.GetTask(1002))
	})

	t.Run("job_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.AddTasksToJob(999, []*datapb.ExternalCollectionRefreshTask{newTask(1001)})
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errExternalRefreshTaskPlanNotPublishable))
	})
}

func TestExternalCollectionRefreshMeta_ReplaceRetryTask(t *testing.T) {
	oldTask := func() *datapb.ExternalCollectionRefreshTask {
		return &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			CollectionId:         100,
			Version:              3,
			NodeId:               10,
			State:                indexpb.JobState_JobStateRetry,
			FailReason:           "worker failure 1/10",
			ExternalSource:       "s3://bucket/path",
			ExternalSpec:         "iceberg",
			Progress:             40,
			ExploreManifestPath:  "manifests/1.pb",
			FileIndexBegin:       3,
			FileIndexEnd:         8,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
			OwnedSegmentIds:      []int64{10, 20},
		}
	}
	replacement := func(task *datapb.ExternalCollectionRefreshTask) *datapb.ExternalCollectionRefreshTask {
		cloned := proto.Clone(task).(*datapb.ExternalCollectionRefreshTask)
		cloned.TaskId = 2001
		cloned.Version = 0
		cloned.NodeId = 0
		cloned.State = indexpb.JobState_JobStateInit
		cloned.FailReason = ""
		cloned.Progress = 0
		cloned.KeptSegments = nil
		cloned.UpdatedSegments = nil
		cloned.ResultReady = false
		cloned.ResultStorageVersion = 0
		cloned.ResultPath = ""
		cloned.ResultChecksum = nil
		return cloned
	}
	newMeta := func(t *testing.T, catalog *stubCatalog) *externalCollectionRefreshMeta {
		t.Helper()
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)
		return meta
	}

	t.Run("success_is_one_composite_publication", func(t *testing.T) {
		old := oldTask()
		catalog := &stubCatalog{
			jobs: []*datapb.ExternalCollectionRefreshJob{{
				JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress, TaskIds: []int64{1001},
			}},
			tasks: []*datapb.ExternalCollectionRefreshTask{old},
		}
		meta := newMeta(t, catalog)
		counter := &atomic.Int64{}
		counter.Store(1)
		meta.workerFailureCounts.Insert(1001, counter)

		replaced, err := meta.ReplaceRetryTask(1001, replacement(old))
		assert.NoError(t, err)
		assert.True(t, replaced)
		assert.Nil(t, meta.GetTask(1001), "the old task must not remain in coordinator metadata")
		newTask := meta.GetTask(2001)
		assert.NotNil(t, newTask)
		assert.Equal(t, "manifests/1.pb", newTask.GetExploreManifestPath())
		assert.Equal(t, int64(3), newTask.GetFileIndexBegin())
		assert.Equal(t, int64(8), newTask.GetFileIndexEnd())
		assert.Equal(t, []int64{10, 20}, newTask.GetOwnedSegmentIds())
		assert.Equal(t, []int64{2001}, meta.GetJob(1).GetTaskIds())
		committed, err := meta.GetCommittedTasksByJobID(1)
		assert.NoError(t, err)
		if assert.Len(t, committed, 1) {
			assert.Equal(t, int64(2001), committed[0].GetTaskId(),
				"task lookup must resolve the same replacement published by the job")
		}
		movedCounter, ok := meta.workerFailureCounts.Get(2001)
		assert.True(t, ok)
		assert.Equal(t, int64(1), movedCounter.Load())
		assert.Equal(t, int64(2), movedCounter.Add(1))

		if assert.Len(t, catalog.updateActions, 1) && assert.Len(t, catalog.updateActions[0], 3) {
			actions := catalog.updateActions[0]
			assert.Equal(t, metastore.ActionDelete, actions[0].Type)
			assert.Equal(t, int64(1001), actions[0].Entry.(metastore.RefreshTaskEntry).TaskID)
			assert.Equal(t, metastore.ActionAdd, actions[1].Type)
			assert.Equal(t, int64(2001), actions[1].Entry.(metastore.RefreshTaskEntry).Task.GetTaskId())
			assert.Equal(t, metastore.ActionUpdate, actions[2].Type)
			assert.Equal(t, []int64{2001}, actions[2].Entry.(metastore.RefreshJobEntry).Job.GetTaskIds())
		}

		assert.NoError(t, meta.DropJob(context.Background(), 1))
		assert.Nil(t, meta.GetTask(1001))
		assert.Nil(t, meta.GetTask(2001))
		if assert.Len(t, catalog.updateActions, 2) {
			assert.Len(t, catalog.updateActions[1], 2, "job GC removes the sole committed task and the job")
		}
	})

	t.Run("terminal_job_is_noop", func(t *testing.T) {
		old := oldTask()
		catalog := &stubCatalog{
			jobs: []*datapb.ExternalCollectionRefreshJob{{
				JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateFailed, TaskIds: []int64{1001},
			}},
			tasks: []*datapb.ExternalCollectionRefreshTask{old},
		}
		meta := newMeta(t, catalog)

		replaced, err := meta.ReplaceRetryTask(1001, replacement(old))
		assert.NoError(t, err)
		assert.False(t, replaced)
		assert.Empty(t, catalog.updateActions)
		assert.NotNil(t, meta.GetTask(1001))
	})
}

func TestExternalCollectionRefreshMeta_GetCommittedTasksByJobIDMissingTask(t *testing.T) {
	meta := createMetaTestRefreshMeta(t, []*datapb.ExternalCollectionRefreshJob{{
		JobId:        1,
		CollectionId: 100,
		TaskIds:      []int64{1001},
	}}, nil)

	_, err := meta.GetCommittedTasksByJobID(1)
	assert.ErrorContains(t, err, "references missing task 1001")
}

func TestExternalCollectionRefreshMeta_GetCommittedTaskResultsByJobID(t *testing.T) {
	newJob := func() []*datapb.ExternalCollectionRefreshJob {
		return []*datapb.ExternalCollectionRefreshJob{{
			JobId:        1,
			CollectionId: 100,
			TaskIds:      []int64{1001},
		}}
	}

	t.Run("job_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)
		_, err := meta.GetCommittedTaskResultsByJobID(1)
		assert.Error(t, err)
	})

	t.Run("reference_without_storage_version", func(t *testing.T) {
		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:     1001,
			JobId:      1,
			ResultPath: "result.pb",
		}
		meta := createMetaTestRefreshMeta(t, newJob(), []*datapb.ExternalCollectionRefreshTask{task})

		_, err := meta.GetCommittedTaskResultsByJobID(1)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("version_two_inline_result", func(t *testing.T) {
		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			ResultReady:          true,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
			KeptSegments:         []int64{1},
		}
		meta := createMetaTestRefreshMeta(t, newJob(), []*datapb.ExternalCollectionRefreshTask{task})

		_, err := meta.GetCommittedTaskResultsByJobID(1)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("consumed_result", func(t *testing.T) {
		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			State:                indexpb.JobState_JobStateFinished,
			ResultReady:          true,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
		}
		meta := createMetaTestRefreshMeta(t, newJob(), []*datapb.ExternalCollectionRefreshTask{task})

		tasks, err := meta.GetCommittedTaskResultsByJobID(1)
		assert.NoError(t, err)
		if assert.Len(t, tasks, 1) {
			assert.True(t, isExternalRefreshTaskResultConsumed(tasks[0]))
		}
	})

	t.Run("unpublished_external_result", func(t *testing.T) {
		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			ResultStorageVersion: externalRefreshTaskResultStorageVersion,
		}
		meta := createMetaTestRefreshMeta(t, newJob(), []*datapb.ExternalCollectionRefreshTask{task})

		_, err := meta.GetCommittedTaskResultsByJobID(1)
		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	})

	t.Run("missing_result_store", func(t *testing.T) {
		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			ResultReady:          true,
			ResultStorageVersion: externalRefreshTaskResultStorageVersion,
			ResultPath:           "result.pb",
			ResultChecksum:       make([]byte, sha256.Size),
		}
		meta := createMetaTestRefreshMeta(t, newJob(), []*datapb.ExternalCollectionRefreshTask{task})

		_, err := meta.GetCommittedTaskResultsByJobID(1)
		assert.ErrorContains(t, err, "unconfigured result store")
	})

	t.Run("load_failed", func(t *testing.T) {
		resultStore, chunkManager := createMetaTestRefreshResultStore(t)
		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			ResultReady:          true,
			ResultStorageVersion: externalRefreshTaskResultStorageVersion,
			ResultPath:           path.Join(chunkManager.RootPath(), "missing.pb"),
			ResultChecksum:       make([]byte, sha256.Size),
		}
		catalog := &stubCatalog{jobs: newJob(), tasks: []*datapb.ExternalCollectionRefreshTask{task}}
		meta, err := newExternalCollectionRefreshMeta(
			context.Background(),
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		assert.NoError(t, err)

		_, err = meta.GetCommittedTaskResultsByJobID(1)
		assert.Error(t, err)
	})

	t.Run("unsupported_storage_version", func(t *testing.T) {
		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			ResultReady:          true,
			ResultStorageVersion: 99,
		}
		meta := createMetaTestRefreshMeta(t, newJob(), []*datapb.ExternalCollectionRefreshTask{task})

		_, err := meta.GetCommittedTaskResultsByJobID(1)
		assert.ErrorContains(t, err, "unsupported result storage version 99")
	})
}

func TestExternalCollectionRefreshMeta_DropJob(t *testing.T) {
	ctx := context.Background()

	t.Run("catalog_update_failed", func(t *testing.T) {
		catalog := &stubCatalog{updateErr: errors.New("update error")}
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		err = meta.DropJob(ctx, 1)
		assert.Error(t, err)

		// A failed composite write must not desync memory from disk: job and
		// task are still present.
		assert.NotNil(t, meta.GetJob(1))
		assert.NotNil(t, meta.GetTask(1001))
	})

	t.Run("success_with_tasks", func(t *testing.T) {
		jobs := []*datapb.ExternalCollectionRefreshJob{
			{JobId: 1, CollectionId: 100},
		}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, CollectionId: 100},
			{TaskId: 1002, JobId: 1, CollectionId: 100},
		}
		catalog := &stubCatalog{jobs: jobs, tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(ctx, catalog)
		assert.NoError(t, err)

		err = meta.DropJob(ctx, 1)
		assert.NoError(t, err)

		// Verify job and tasks removed
		assert.Nil(t, meta.GetJob(1))
		assert.Nil(t, meta.GetTask(1001))
		assert.Nil(t, meta.GetTask(1002))

		// One composite catalog.Update call: a DropRefreshTask action per
		// task, followed by a single DropRefreshJob action landing last (the
		// job is the failover anchor).
		assert.Len(t, catalog.updateActions, 1)
		actions := catalog.updateActions[0]
		assert.Len(t, actions, 3)

		taskIDs := make(map[int64]bool)
		for _, action := range actions[:2] {
			assert.Equal(t, metastore.ActionDelete, action.Type)
			entry, ok := action.Entry.(metastore.RefreshTaskEntry)
			assert.True(t, ok)
			taskIDs[entry.TaskID] = true
		}
		assert.Equal(t, map[int64]bool{1001: true, 1002: true}, taskIDs)

		lastAction := actions[2]
		assert.Equal(t, metastore.ActionDelete, lastAction.Type)
		jobEntry, ok := lastAction.Entry.(metastore.RefreshJobEntry)
		assert.True(t, ok)
		assert.Equal(t, int64(1), jobEntry.JobID)
	})

	t.Run("job_not_exists", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		// Should not error if job doesn't exist
		err := meta.DropJob(ctx, 999)
		assert.NoError(t, err)
	})

	t.Run("removes_external_result_prefix", func(t *testing.T) {
		resultStore, chunkManager := createMetaTestRefreshResultStore(t)
		job := &datapb.ExternalCollectionRefreshJob{JobId: 1, CollectionId: 100}
		catalog := &stubCatalog{jobs: []*datapb.ExternalCollectionRefreshJob{job}}
		meta, err := newExternalCollectionRefreshMeta(
			ctx,
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		assert.NoError(t, err)

		orphanPath := path.Join(
			chunkManager.RootPath(),
			externalRefreshTaskResultRoot,
			"100",
			"1",
			"1001",
			"1",
			"orphan.pb",
		)
		assert.NoError(t, chunkManager.Write(ctx, orphanPath, []byte("orphan")))

		assert.NoError(t, meta.DropJob(ctx, 1))
		exists, err := chunkManager.Exist(ctx, orphanPath)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("result_cleanup_failure_keeps_meta_for_retry", func(t *testing.T) {
		resultStore, _ := createMetaTestRefreshResultStore(t)
		job := &datapb.ExternalCollectionRefreshJob{JobId: 1, CollectionId: 100}
		catalog := &stubCatalog{jobs: []*datapb.ExternalCollectionRefreshJob{job}}
		meta, err := newExternalCollectionRefreshMeta(
			ctx,
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		assert.NoError(t, err)

		mockRemovePrefix := mockey.Mock((*storage.LocalChunkManager).RemoveWithPrefix).
			Return(merr.WrapErrIoFailed("result", errors.New("remove prefix failed"))).
			Build()
		defer mockRemovePrefix.UnPatch()

		assert.Error(t, meta.DropJob(ctx, 1))
		assert.NotNil(t, meta.GetJob(1))
		assert.Empty(t, catalog.updateActions, "catalog meta must remain until object cleanup succeeds")
	})
}

func TestExternalCollectionRefreshMeta_DropJobSerializesTaskMutation(t *testing.T) {
	newMeta := func(t *testing.T, catalog *controlledRefreshCatalog) *externalCollectionRefreshMeta {
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)
		return meta
	}
	newCatalog := func() *controlledRefreshCatalog {
		return &controlledRefreshCatalog{stubCatalog: &stubCatalog{
			jobs: []*datapb.ExternalCollectionRefreshJob{
				{JobId: 1, CollectionId: 100, TaskIds: []int64{1001}},
			},
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, CollectionId: 100, State: indexpb.JobState_JobStateInProgress},
			},
		}}
	}

	t.Run("mutation_commits_before_drop", func(t *testing.T) {
		catalog := newCatalog()
		meta := newMeta(t, catalog)
		saveStarted := make(chan struct{})
		releaseSave := make(chan struct{})
		dropEnteredCatalog := make(chan struct{})
		var jobLockHeldAtSave atomic.Bool
		var taskLockHeldAtSave atomic.Bool
		catalog.onTaskSave = func() {
			if meta.jobLock.TryLock(int64(100)) {
				meta.jobLock.Unlock(int64(100))
			} else {
				jobLockHeldAtSave.Store(true)
			}
			if meta.taskLock.TryLock(int64(1)) {
				meta.taskLock.Unlock(int64(1))
			} else {
				taskLockHeldAtSave.Store(true)
			}
			close(saveStarted)
			<-releaseSave
		}
		catalog.onUpdate = func() { close(dropEnteredCatalog) }

		mutateDone := make(chan error, 1)
		go func() {
			mutateDone <- meta.UpdateTaskProgress(1001, 50)
		}()
		<-saveStarted

		dropDone := make(chan error, 1)
		go func() {
			dropDone <- meta.DropJob(context.Background(), 1)
		}()
		select {
		case <-dropEnteredCatalog:
			t.Fatal("DropJob entered its catalog transaction while task Save held the ownership locks")
		default:
		}

		close(releaseSave)
		assert.NoError(t, <-mutateDone)
		assert.NoError(t, <-dropDone)
		assert.True(t, jobLockHeldAtSave.Load())
		assert.True(t, taskLockHeldAtSave.Load())
		assert.Nil(t, meta.GetJob(1))
		assert.Nil(t, meta.GetTask(1001))
	})

	t.Run("drop_commits_before_mutation", func(t *testing.T) {
		catalog := newCatalog()
		meta := newMeta(t, catalog)
		dropStarted := make(chan struct{})
		releaseDrop := make(chan struct{})
		var jobLockHeldAtDrop atomic.Bool
		var taskLockHeldAtDrop atomic.Bool
		var taskSaveCalls atomic.Int32
		catalog.onUpdate = func() {
			if meta.jobLock.TryLock(int64(100)) {
				meta.jobLock.Unlock(int64(100))
			} else {
				jobLockHeldAtDrop.Store(true)
			}
			if meta.taskLock.TryLock(int64(1)) {
				meta.taskLock.Unlock(int64(1))
			} else {
				taskLockHeldAtDrop.Store(true)
			}
			close(dropStarted)
			<-releaseDrop
		}
		catalog.onTaskSave = func() { taskSaveCalls.Add(1) }

		dropDone := make(chan error, 1)
		go func() {
			dropDone <- meta.DropJob(context.Background(), 1)
		}()
		<-dropStarted

		mutateInvoked := make(chan struct{})
		mutateDone := make(chan error, 1)
		go func() {
			close(mutateInvoked)
			mutateDone <- meta.UpdateTaskProgress(1001, 50)
		}()
		<-mutateInvoked
		close(releaseDrop)

		assert.NoError(t, <-dropDone)
		assert.Error(t, <-mutateDone)
		assert.True(t, jobLockHeldAtDrop.Load())
		assert.True(t, taskLockHeldAtDrop.Load())
		assert.Equal(t, int32(0), taskSaveCalls.Load(),
			"a task Save must never land after the composite job drop")
		assert.Nil(t, meta.GetJob(1))
		assert.Nil(t, meta.GetTask(1001))
	})
}

func TestExternalCollectionRefreshMeta_AddTask(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		task := &datapb.ExternalCollectionRefreshTask{
			TaskId:       1001,
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInit,
		}

		err := meta.AddTask(task)
		assert.NoError(t, err)

		// Verify task added
		got := meta.GetTask(1001)
		assert.NotNil(t, got)
		assert.Equal(t, int64(1001), got.GetTaskId())
	})

	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		// Mock SaveExternalCollectionRefreshTask to return error
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		task := &datapb.ExternalCollectionRefreshTask{
			TaskId: 1001,
			JobId:  1,
		}

		err = meta.AddTask(task)
		assert.Error(t, err)

		// Verify task not added
		got := meta.GetTask(1001)
		assert.Nil(t, got)
	})
}

func TestExternalCollectionRefreshMeta_GetTask(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, CollectionId: 100},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	t.Run("task_exists", func(t *testing.T) {
		task := meta.GetTask(1001)
		assert.NotNil(t, task)
		assert.Equal(t, int64(1001), task.GetTaskId())
	})

	t.Run("task_not_exists", func(t *testing.T) {
		task := meta.GetTask(9999)
		assert.Nil(t, task)
	})
}

func TestExternalCollectionRefreshMeta_GetTasksByJobID(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, CollectionId: 100},
		{TaskId: 1002, JobId: 1, CollectionId: 100},
		{TaskId: 2001, JobId: 2, CollectionId: 200},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	t.Run("has_tasks", func(t *testing.T) {
		tasks := meta.GetTasksByJobID(1)
		assert.Len(t, tasks, 2)
	})

	t.Run("no_tasks", func(t *testing.T) {
		tasks := meta.GetTasksByJobID(999)
		assert.Nil(t, tasks)
	})
}

func TestExternalCollectionRefreshMeta_GetAllTasks(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1},
		{TaskId: 1002, JobId: 1},
		{TaskId: 2001, JobId: 2},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	allTasks := meta.GetAllTasks()
	assert.Len(t, allTasks, 3)
}

func TestExternalCollectionRefreshMeta_GetTaskState(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
	}
	meta := createMetaTestRefreshMeta(t, nil, tasks)

	t.Run("task_exists", func(t *testing.T) {
		state := meta.GetTaskState(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, state)
	})

	t.Run("task_not_exists", func(t *testing.T) {
		state := meta.GetTaskState(9999)
		assert.Equal(t, indexpb.JobState_JobStateNone, state)
	})
}

func TestExternalCollectionRefreshMeta_UpdateTaskState(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateTaskState(1001, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
		assert.Equal(t, indexpb.JobState_JobStateInit, meta.GetTask(1001).GetState())
	})

	t.Run("success", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskState(1001, indexpb.JobState_JobStateInProgress, "")
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
	})

	t.Run("failed_state", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskState(1001, indexpb.JobState_JobStateFailed, "connection timeout")
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFailed, task.GetState())
		assert.Equal(t, "connection timeout", task.GetFailReason())
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateTaskState(9999, indexpb.JobState_JobStateInProgress, "")
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_UpdateTaskResult(t *testing.T) {
	t.Run("persists_result_and_clones_segments", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		var savedTask *datapb.ExternalCollectionRefreshTask
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(_ context.Context, task *datapb.ExternalCollectionRefreshTask) error {
				savedTask = task
				return nil
			}).Build()
		defer mockSave.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		updatedSegment := &datapb.SegmentInfo{ID: 10, CollectionID: 100, NumOfRows: 7}
		err = meta.UpdateTaskResult(
			1001,
			indexpb.JobState_JobStateFinished,
			"",
			[]int64{1, 2},
			[]*datapb.SegmentInfo{updatedSegment},
		)
		assert.NoError(t, err)
		assert.NotNil(t, savedTask)
		assert.Equal(t, indexpb.JobState_JobStateFinished, savedTask.GetState())
		assert.Equal(t, int64(100), savedTask.GetProgress())
		assert.True(t, savedTask.GetResultReady())
		assert.Equal(t, []int64{1, 2}, savedTask.GetKeptSegments())
		assert.Len(t, savedTask.GetUpdatedSegments(), 1)
		assert.Equal(t, int64(10), savedTask.GetUpdatedSegments()[0].GetID())
		assert.Equal(t, int64(7), savedTask.GetUpdatedSegments()[0].GetNumOfRows())

		updatedSegment.NumOfRows = 99
		task := meta.GetTask(1001)
		assert.Equal(t, int64(7), task.GetUpdatedSegments()[0].GetNumOfRows())
		assert.Equal(t, int64(7), savedTask.GetUpdatedSegments()[0].GetNumOfRows())
	})

	t.Run("save_failed_keeps_original_task", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInProgress},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()
		var fatalCalled atomic.Bool
		var fatalHeldJobLock atomic.Bool
		mockFatal := mockey.Mock(mlog.Fatal).
			To(func(context.Context, string, ...mlog.Field) {
				fatalCalled.Store(true)
				if meta.jobLock.TryLock(0) {
					meta.jobLock.Unlock(0)
					return
				}
				fatalHeldJobLock.Store(true)
			}).
			Build()
		defer mockFatal.UnPatch()

		err = meta.UpdateTaskResult(
			1001,
			indexpb.JobState_JobStateFinished,
			"",
			[]int64{1},
			[]*datapb.SegmentInfo{{ID: 10, CollectionID: 100, NumOfRows: 7}},
		)
		assert.Error(t, err)
		assert.True(t, fatalCalled.Load())
		assert.True(t, fatalHeldJobLock.Load())

		task := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
		assert.Empty(t, task.GetKeptSegments())
		assert.Empty(t, task.GetUpdatedSegments())
	})

	t.Run("stores_version_two_result_outside_task_metadata", func(t *testing.T) {
		resultStore, chunkManager := createMetaTestRefreshResultStore(t)
		jobs := []*datapb.ExternalCollectionRefreshJob{{
			JobId:        1,
			CollectionId: 100,
			TaskIds:      []int64{1001},
		}}
		tasks := []*datapb.ExternalCollectionRefreshTask{{
			TaskId:               1001,
			JobId:                1,
			CollectionId:         100,
			Version:              3,
			State:                indexpb.JobState_JobStateInProgress,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
		}}
		catalog := &stubCatalog{jobs: jobs, tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(
			context.Background(),
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		assert.NoError(t, err)

		updatedSegment := &datapb.SegmentInfo{ID: 10, CollectionID: 100, NumOfRows: 7}
		err = meta.UpdateTaskResult(
			1001,
			indexpb.JobState_JobStateFinished,
			"",
			[]int64{1, 2},
			[]*datapb.SegmentInfo{updatedSegment},
		)
		assert.NoError(t, err)

		header := meta.GetTask(1001)
		assert.Equal(t, externalRefreshTaskResultStorageVersion, header.GetResultStorageVersion())
		assert.NotEmpty(t, header.GetResultPath())
		assert.Len(t, header.GetResultChecksum(), sha256.Size)
		assert.Empty(t, header.GetKeptSegments())
		assert.Empty(t, header.GetUpdatedSegments())
		exists, err := chunkManager.Exist(context.Background(), header.GetResultPath())
		assert.NoError(t, err)
		assert.True(t, exists)

		updatedSegment.NumOfRows = 99
		resultTasks, err := meta.GetCommittedTaskResultsByJobID(1)
		assert.NoError(t, err)
		assert.Len(t, resultTasks, 1)
		assert.Equal(t, []int64{1, 2}, resultTasks[0].GetKeptSegments())
		assert.Equal(t, int64(7), resultTasks[0].GetUpdatedSegments()[0].GetNumOfRows())

		resultPath := header.GetResultPath()
		assert.NoError(t, meta.ClearTaskResult(1001))
		cleared := meta.GetTask(1001)
		assert.True(t, cleared.GetResultReady())
		assert.Zero(t, cleared.GetResultStorageVersion())
		assert.Empty(t, cleared.GetResultPath())
		assert.Empty(t, cleared.GetResultChecksum())
		exists, err = chunkManager.Exist(context.Background(), resultPath)
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("object_write_failed_does_not_update_catalog", func(t *testing.T) {
		resultStore, _ := createMetaTestRefreshResultStore(t)
		tasks := []*datapb.ExternalCollectionRefreshTask{{
			TaskId:               1001,
			JobId:                1,
			CollectionId:         100,
			State:                indexpb.JobState_JobStateInProgress,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
		}}
		catalog := &stubCatalog{tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(
			context.Background(),
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		assert.NoError(t, err)

		mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).
			Return(merr.WrapErrIoFailed("result", errors.New("write failed"))).
			Build()
		defer mockWrite.UnPatch()
		var fatalCalled atomic.Bool
		mockFatal := mockey.Mock(mlog.Fatal).
			To(func(context.Context, string, ...mlog.Field) { fatalCalled.Store(true) }).
			Build()
		defer mockFatal.UnPatch()
		saveCalls := 0
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(_ context.Context, _ *datapb.ExternalCollectionRefreshTask) error {
				saveCalls++
				return nil
			}).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateTaskResult(1001, indexpb.JobState_JobStateFinished, "", []int64{1}, nil)
		assert.Error(t, err)
		assert.False(t, fatalCalled.Load())
		assert.Zero(t, saveCalls)
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetTask(1001).GetState())
		assert.Empty(t, meta.GetTask(1001).GetResultPath())
	})

	t.Run("version_two_requires_result_store", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{{
			TaskId:               1001,
			JobId:                1,
			CollectionId:         100,
			State:                indexpb.JobState_JobStateInProgress,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
		}}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskResult(1001, indexpb.JobState_JobStateFinished, "", nil, nil)
		assert.ErrorContains(t, err, "unconfigured result store")
	})

	t.Run("rejects_unknown_ownership_version", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{{
			TaskId:               1001,
			JobId:                1,
			CollectionId:         100,
			State:                indexpb.JobState_JobStateInProgress,
			OwnershipPlanVersion: 99,
		}}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskResult(1001, indexpb.JobState_JobStateFinished, "", nil, nil)
		assert.ErrorContains(t, err, "unsupported ownership plan version 99")
	})

	t.Run("external_result_write_uses_existing_meta_locks", func(t *testing.T) {
		resultStore, _ := createMetaTestRefreshResultStore(t)
		jobs := []*datapb.ExternalCollectionRefreshJob{{
			JobId:        1,
			CollectionId: 100,
			TaskIds:      []int64{1001},
		}}
		tasks := []*datapb.ExternalCollectionRefreshTask{{
			TaskId:               1001,
			JobId:                1,
			CollectionId:         100,
			Version:              1,
			State:                indexpb.JobState_JobStateInProgress,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
		}}
		catalog := &stubCatalog{jobs: jobs, tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(
			context.Background(),
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		assert.NoError(t, err)

		var jobLockHeld atomic.Bool
		var taskLockHeld atomic.Bool
		mockWrite := mockey.Mock((*storage.LocalChunkManager).Write).
			To(func(_ context.Context, _ string, _ []byte) error {
				if meta.jobLock.TryLock(int64(100)) {
					meta.jobLock.Unlock(int64(100))
				} else {
					jobLockHeld.Store(true)
				}
				if meta.taskLock.TryLock(int64(1)) {
					meta.taskLock.Unlock(int64(1))
				} else {
					taskLockHeld.Store(true)
				}
				return nil
			}).Build()
		defer mockWrite.UnPatch()

		err = meta.UpdateTaskResult(1001, indexpb.JobState_JobStateFinished, "", nil, nil)
		assert.NoError(t, err)
		assert.True(t, jobLockHeld.Load())
		assert.True(t, taskLockHeld.Load())
		assert.Equal(t, indexpb.JobState_JobStateFinished, meta.GetTask(1001).GetState())
		assert.NotEmpty(t, meta.GetTask(1001).GetResultPath())
	})

	t.Run("catalog_failure_leaves_external_result_for_job_gc", func(t *testing.T) {
		resultStore, chunkManager := createMetaTestRefreshResultStore(t)
		tasks := []*datapb.ExternalCollectionRefreshTask{{
			TaskId:               1001,
			JobId:                1,
			CollectionId:         100,
			Version:              1,
			State:                indexpb.JobState_JobStateInProgress,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
		}}
		catalog := &stubCatalog{tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(
			context.Background(),
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			Return(errors.New("save error")).
			Build()
		defer mockSave.UnPatch()
		var fatalCalled atomic.Bool
		mockFatal := mockey.Mock(mlog.Fatal).
			To(func(context.Context, string, ...mlog.Field) { fatalCalled.Store(true) }).
			Build()
		defer mockFatal.UnPatch()

		err = meta.UpdateTaskResult(1001, indexpb.JobState_JobStateFinished, "", []int64{1}, nil)
		assert.Error(t, err)
		assert.True(t, fatalCalled.Load())
		assert.Equal(t, indexpb.JobState_JobStateInProgress, meta.GetTask(1001).GetState())
		assert.Empty(t, meta.GetTask(1001).GetResultPath())

		prefix := path.Join(
			chunkManager.RootPath(),
			externalRefreshTaskResultRoot,
			"100",
			"1",
		) + "/"
		paths, _, err := storage.ListAllChunkWithPrefix(context.Background(), chunkManager, prefix, true)
		assert.NoError(t, err)
		assert.Len(t, paths, 1)
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateTaskResult(
			9999,
			indexpb.JobState_JobStateFinished,
			"",
			[]int64{1},
			[]*datapb.SegmentInfo{{ID: 10}},
		)
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_ConsumeCommittedTaskResults(t *testing.T) {
	consumedTask := func(taskID int64) *datapb.ExternalCollectionRefreshTask {
		return &datapb.ExternalCollectionRefreshTask{
			TaskId:               taskID,
			JobId:                1,
			CollectionId:         100,
			State:                indexpb.JobState_JobStateFinished,
			Progress:             100,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
			ResultReady:          true,
			BaseManifests:        map[int64]string{taskID: "manifest-v1"},
		}
	}
	job := func(taskIDs ...int64) []*datapb.ExternalCollectionRefreshJob {
		return []*datapb.ExternalCollectionRefreshJob{{
			JobId:        1,
			CollectionId: 100,
			State:        indexpb.JobState_JobStateInProgress,
			TaskIds:      taskIDs,
		}}
	}

	t.Run("all_consumed_is_noop", func(t *testing.T) {
		catalog := &stubCatalog{
			jobs:  job(1001, 1002),
			tasks: []*datapb.ExternalCollectionRefreshTask{consumedTask(1001), consumedTask(1002)},
		}
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		require.NoError(t, err)

		consumeCalls := 0
		err = meta.ConsumeCommittedTaskResults(1, func([]*datapb.ExternalCollectionRefreshTask, []metastore.UpdateAction) error {
			consumeCalls++
			return nil
		})

		require.NoError(t, err)
		assert.Zero(t, consumeCalls)
		assert.Empty(t, catalog.updateActions)
		assert.True(t, isExternalRefreshTaskResultConsumed(meta.GetTask(1001)))
		assert.True(t, isExternalRefreshTaskResultConsumed(meta.GetTask(1002)))
	})

	t.Run("partially_consumed_is_integrity_error", func(t *testing.T) {
		pending := &datapb.ExternalCollectionRefreshTask{
			TaskId:               1002,
			JobId:                1,
			CollectionId:         100,
			State:                indexpb.JobState_JobStateFinished,
			Progress:             100,
			OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
			ResultReady:          true,
			ResultStorageVersion: externalRefreshTaskResultStorageVersion,
			ResultPath:           "results/1002.pb",
			ResultChecksum:       make([]byte, sha256.Size),
		}
		catalog := &stubCatalog{
			jobs:  job(1001, 1002),
			tasks: []*datapb.ExternalCollectionRefreshTask{consumedTask(1001), pending},
		}
		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		require.NoError(t, err)

		consumeCalls := 0
		err = meta.ConsumeCommittedTaskResults(1, func([]*datapb.ExternalCollectionRefreshTask, []metastore.UpdateAction) error {
			consumeCalls++
			return nil
		})

		assert.ErrorIs(t, err, merr.ErrDataIntegrity)
		assert.Contains(t, err.Error(), "partially consumed")
		assert.Zero(t, consumeCalls)
		assert.Empty(t, catalog.updateActions)
		assert.Equal(t, pending.GetResultPath(), meta.GetTask(1002).GetResultPath())
	})

	type consumeFixture struct {
		meta         *externalCollectionRefreshMeta
		catalog      *stubCatalog
		chunkManager *storage.LocalChunkManager
		resultPaths  map[int64]string
	}
	newFixture := func(t *testing.T, taskIDs ...int64) *consumeFixture {
		t.Helper()
		resultStore, chunkManager := createMetaTestRefreshResultStore(t)
		tasks := make([]*datapb.ExternalCollectionRefreshTask, 0, len(taskIDs))
		for _, taskID := range taskIDs {
			tasks = append(tasks, &datapb.ExternalCollectionRefreshTask{
				TaskId:               taskID,
				JobId:                1,
				CollectionId:         100,
				State:                indexpb.JobState_JobStateInProgress,
				OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
				OwnedSegmentIds:      []int64{taskID},
				BaseManifests:        map[int64]string{taskID: "manifest-v1"},
			})
		}
		catalog := &stubCatalog{jobs: job(taskIDs...), tasks: tasks}
		meta, err := newExternalCollectionRefreshMeta(
			context.Background(),
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		require.NoError(t, err)

		resultPaths := make(map[int64]string, len(taskIDs))
		for _, taskID := range taskIDs {
			require.NoError(t, meta.UpdateTaskResult(
				taskID,
				indexpb.JobState_JobStateFinished,
				"",
				[]int64{taskID},
				[]*datapb.SegmentInfo{{ID: taskID + 10000, CollectionID: 100, NumOfRows: 7}},
			))
			resultPaths[taskID] = meta.GetTask(taskID).GetResultPath()
			require.NotEmpty(t, resultPaths[taskID])
		}
		return &consumeFixture{
			meta:         meta,
			catalog:      catalog,
			chunkManager: chunkManager,
			resultPaths:  resultPaths,
		}
	}

	t.Run("success_clears_persisted_and_memory_references", func(t *testing.T) {
		fixture := newFixture(t, 1001, 1002)
		consumeCalls := 0
		err := fixture.meta.ConsumeCommittedTaskResults(1, func(tasks []*datapb.ExternalCollectionRefreshTask, actions []metastore.UpdateAction) error {
			consumeCalls++
			require.Len(t, tasks, 2)
			require.Len(t, actions, 2)
			for i, task := range tasks {
				assert.Equal(t, []int64{task.GetTaskId()}, task.GetKeptSegments())
				assert.Len(t, task.GetUpdatedSegments(), 1)

				entry, ok := actions[i].Entry.(metastore.RefreshTaskEntry)
				require.True(t, ok)
				persisted := entry.Task
				require.NotNil(t, persisted)
				assert.Equal(t, indexpb.JobState_JobStateFinished, persisted.GetState())
				assert.True(t, persisted.GetResultReady())
				assert.False(t, externalRefreshTaskHasResultPayload(persisted))
				assert.Equal(t, map[int64]string{task.GetTaskId(): "manifest-v1"}, persisted.GetBaseManifests())
			}
			return fixture.catalog.Update(context.Background(), actions...)
		})

		require.NoError(t, err)
		assert.Equal(t, 1, consumeCalls)
		require.Len(t, fixture.catalog.updateActions, 1)
		for _, taskID := range []int64{1001, 1002} {
			inMemory := fixture.meta.GetTask(taskID)
			assert.True(t, isExternalRefreshTaskResultConsumed(inMemory))
			assert.False(t, externalRefreshTaskHasResultPayload(inMemory))
			assert.Equal(t, map[int64]string{taskID: "manifest-v1"}, inMemory.GetBaseManifests())

			exists, existErr := fixture.chunkManager.Exist(context.Background(), fixture.resultPaths[taskID])
			require.NoError(t, existErr)
			assert.False(t, exists)
		}

		// The durable empty-payload marker makes a replay a true no-op.
		err = fixture.meta.ConsumeCommittedTaskResults(1, func([]*datapb.ExternalCollectionRefreshTask, []metastore.UpdateAction) error {
			consumeCalls++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, consumeCalls)
		assert.Len(t, fixture.catalog.updateActions, 1)
	})

	t.Run("consumer_error_keeps_references", func(t *testing.T) {
		fixture := newFixture(t, 1001)
		resultPath := fixture.resultPaths[1001]
		consumeErr := errors.New("segment transaction failed")

		err := fixture.meta.ConsumeCommittedTaskResults(1, func(tasks []*datapb.ExternalCollectionRefreshTask, actions []metastore.UpdateAction) error {
			require.Len(t, tasks, 1)
			require.Len(t, actions, 1)
			assert.Equal(t, []int64{1001}, tasks[0].GetKeptSegments())
			return consumeErr
		})

		assert.ErrorIs(t, err, consumeErr)
		assert.Empty(t, fixture.catalog.updateActions)
		inMemory := fixture.meta.GetTask(1001)
		assert.Equal(t, resultPath, inMemory.GetResultPath())
		assert.Equal(t, externalRefreshTaskResultStorageVersion, inMemory.GetResultStorageVersion())
		assert.Len(t, inMemory.GetResultChecksum(), sha256.Size)
		assert.True(t, inMemory.GetResultReady())
		assert.False(t, isExternalRefreshTaskResultConsumed(inMemory))

		exists, existErr := fixture.chunkManager.Exist(context.Background(), resultPath)
		require.NoError(t, existErr)
		assert.True(t, exists)
	})

	t.Run("late_worker_result_does_not_resurrect_consumed_payload", func(t *testing.T) {
		fixture := newFixture(t, 1001)
		err := fixture.meta.ConsumeCommittedTaskResults(1, func(_ []*datapb.ExternalCollectionRefreshTask, actions []metastore.UpdateAction) error {
			return fixture.catalog.Update(context.Background(), actions...)
		})
		require.NoError(t, err)
		require.True(t, isExternalRefreshTaskResultConsumed(fixture.meta.GetTask(1001)))

		var saveCalls atomic.Int32
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(context.Context, *datapb.ExternalCollectionRefreshTask) error {
				saveCalls.Add(1)
				return nil
			}).
			Build()
		defer mockSave.UnPatch()

		err = fixture.meta.UpdateTaskResult(
			1001,
			indexpb.JobState_JobStateFinished,
			"",
			[]int64{1001},
			[]*datapb.SegmentInfo{{ID: 99999, CollectionID: 100, NumOfRows: 9}},
		)
		require.NoError(t, err)
		assert.Zero(t, saveCalls.Load())
		assert.True(t, isExternalRefreshTaskResultConsumed(fixture.meta.GetTask(1001)))

		prefix := path.Join(
			fixture.chunkManager.RootPath(),
			externalRefreshTaskResultRoot,
			"100",
			"1",
		) + "/"
		paths, _, listErr := storage.ListAllChunkWithPrefix(context.Background(), fixture.chunkManager, prefix, true)
		require.NoError(t, listErr)
		assert.Empty(t, paths)
	})
}

func TestExternalCollectionRefreshMeta_RetryFinishedTaskOnManifestConflict(t *testing.T) {
	newMeta := func(
		t *testing.T,
		resultPath string,
		resultChecksum []byte,
	) (*externalCollectionRefreshMeta, *stubCatalog, *storage.LocalChunkManager) {
		t.Helper()
		resultStore, chunkManager := createMetaTestRefreshResultStore(t)
		require.NoError(t, chunkManager.Write(context.Background(), resultPath, []byte("result")))
		catalog := &stubCatalog{
			jobs: []*datapb.ExternalCollectionRefreshJob{{
				JobId:        1,
				CollectionId: 100,
				State:        indexpb.JobState_JobStateInProgress,
				TaskIds:      []int64{1001},
			}},
			tasks: []*datapb.ExternalCollectionRefreshTask{{
				TaskId:               1001,
				JobId:                1,
				CollectionId:         100,
				State:                indexpb.JobState_JobStateFinished,
				Progress:             100,
				OwnershipPlanVersion: externalRefreshOwnershipPlanVersion,
				ResultReady:          true,
				ResultStorageVersion: externalRefreshTaskResultStorageVersion,
				ResultPath:           resultPath,
				ResultChecksum:       append([]byte(nil), resultChecksum...),
				BaseManifests:        map[int64]string{10: "manifest-v1"},
			}},
		}
		meta, err := newExternalCollectionRefreshMeta(
			context.Background(),
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		require.NoError(t, err)
		return meta, catalog, chunkManager
	}

	t.Run("exact_token_moves_finished_to_retry", func(t *testing.T) {
		resultChecksum := sha256.Sum256([]byte("result"))
		resultPath := path.Join(t.TempDir(), "exact-result.pb")
		meta, _, chunkManager := newMeta(t, resultPath, resultChecksum[:])

		var savedTask *datapb.ExternalCollectionRefreshTask
		var saveCalls atomic.Int32
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(_ context.Context, task *datapb.ExternalCollectionRefreshTask) error {
				saveCalls.Add(1)
				savedTask = proto.Clone(task).(*datapb.ExternalCollectionRefreshTask)
				return nil
			}).
			Build()
		defer mockSave.UnPatch()

		token := &externalRefreshRetryTaskError{
			taskID:               1001,
			segmentID:            10,
			resultStorageVersion: externalRefreshTaskResultStorageVersion,
			resultPath:           resultPath,
			resultChecksum:       append([]byte(nil), resultChecksum[:]...),
			cause:                errors.New("base manifest changed"),
		}
		applied, err := meta.RetryFinishedTaskOnManifestConflict(token)

		require.NoError(t, err)
		assert.True(t, applied)
		assert.Equal(t, int32(1), saveCalls.Load())
		require.NotNil(t, savedTask)
		assert.Equal(t, indexpb.JobState_JobStateRetry, savedTask.GetState())
		assert.Zero(t, savedTask.GetProgress())
		assert.False(t, savedTask.GetResultReady())
		assert.False(t, externalRefreshTaskHasResultPayload(savedTask))
		assert.Empty(t, savedTask.GetBaseManifests())

		inMemory := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateRetry, inMemory.GetState())
		assert.Zero(t, inMemory.GetProgress())
		assert.False(t, inMemory.GetResultReady())
		assert.False(t, externalRefreshTaskHasResultPayload(inMemory))
		assert.Empty(t, inMemory.GetBaseManifests())
		assert.Contains(t, inMemory.GetFailReason(), "base manifest changed")

		exists, existErr := chunkManager.Exist(context.Background(), resultPath)
		require.NoError(t, existErr)
		assert.False(t, exists)

		// Replaying the now-stale token cannot move or rewrite the Retry task.
		again, err := meta.RetryFinishedTaskOnManifestConflict(token)
		require.NoError(t, err)
		assert.False(t, again)
		assert.Equal(t, int32(1), saveCalls.Load())
	})

	t.Run("stale_token_is_noop", func(t *testing.T) {
		currentChecksum := sha256.Sum256([]byte("result"))
		resultPath := path.Join(t.TempDir(), "current-result.pb")
		meta, _, chunkManager := newMeta(t, resultPath, currentChecksum[:])

		var saveCalls atomic.Int32
		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
			To(func(context.Context, *datapb.ExternalCollectionRefreshTask) error {
				saveCalls.Add(1)
				return nil
			}).
			Build()
		defer mockSave.UnPatch()

		staleChecksum := sha256.Sum256([]byte("old result"))
		applied, err := meta.RetryFinishedTaskOnManifestConflict(&externalRefreshRetryTaskError{
			taskID:               1001,
			segmentID:            10,
			resultStorageVersion: externalRefreshTaskResultStorageVersion,
			resultPath:           resultPath,
			resultChecksum:       staleChecksum[:],
			cause:                errors.New("stale manifest conflict"),
		})

		require.NoError(t, err)
		assert.False(t, applied)
		assert.Zero(t, saveCalls.Load())
		inMemory := meta.GetTask(1001)
		assert.Equal(t, indexpb.JobState_JobStateFinished, inMemory.GetState())
		assert.True(t, inMemory.GetResultReady())
		assert.Equal(t, resultPath, inMemory.GetResultPath())
		assert.Equal(t, currentChecksum[:], inMemory.GetResultChecksum())
		assert.Equal(t, map[int64]string{10: "manifest-v1"}, inMemory.GetBaseManifests())

		exists, existErr := chunkManager.Exist(context.Background(), resultPath)
		require.NoError(t, existErr)
		assert.True(t, exists)
	})
}

func TestExternalCollectionRefreshMeta_ClearTaskResultsByJobID_PartialFailure(t *testing.T) {
	catalog := &stubCatalog{}
	jobs := []*datapb.ExternalCollectionRefreshJob{
		{JobId: 1, CollectionId: 100, TaskIds: []int64{1001, 1002}},
	}
	tasks := []*datapb.ExternalCollectionRefreshTask{
		{
			TaskId:          1001,
			JobId:           1,
			State:           indexpb.JobState_JobStateFinished,
			ResultReady:     true,
			KeptSegments:    []int64{1},
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 10}},
		},
		{
			TaskId:          1002,
			JobId:           1,
			State:           indexpb.JobState_JobStateFinished,
			ResultReady:     true,
			KeptSegments:    []int64{2},
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 20}},
		},
	}
	mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(jobs, nil).Build()
	defer mockListJobs.UnPatch()
	mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
	defer mockListTasks.UnPatch()

	saveCalls := 0
	mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).
		To(func(_ context.Context, task *datapb.ExternalCollectionRefreshTask) error {
			saveCalls++
			if task.GetTaskId() == 1002 {
				return errors.New("save error")
			}
			return nil
		}).Build()
	defer mockSave.UnPatch()

	meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
	assert.NoError(t, err)

	err = meta.ClearTaskResultsByJobID(1)
	assert.Error(t, err)
	assert.Equal(t, 2, saveCalls)
	assert.Empty(t, meta.GetTask(1001).GetKeptSegments())
	assert.Empty(t, meta.GetTask(1001).GetUpdatedSegments())
	assert.Equal(t, []int64{2}, meta.GetTask(1002).GetKeptSegments())
	assert.Len(t, meta.GetTask(1002).GetUpdatedSegments(), 1)
}

func TestExternalCollectionRefreshMeta_ClearExternalTaskResultBestEffort(t *testing.T) {
	newTask := func() *datapb.ExternalCollectionRefreshTask {
		return &datapb.ExternalCollectionRefreshTask{
			TaskId:               1001,
			JobId:                1,
			State:                indexpb.JobState_JobStateFinished,
			ResultReady:          true,
			ResultStorageVersion: externalRefreshTaskResultStorageVersion,
			ResultPath:           "result.pb",
			ResultChecksum:       make([]byte, sha256.Size),
		}
	}

	t.Run("missing_store", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, []*datapb.ExternalCollectionRefreshTask{newTask()})

		assert.NoError(t, meta.ClearTaskResult(1001))
		assert.Empty(t, meta.GetTask(1001).GetResultPath())
	})

	t.Run("remove_failed", func(t *testing.T) {
		resultStore, _ := createMetaTestRefreshResultStore(t)
		catalog := &stubCatalog{tasks: []*datapb.ExternalCollectionRefreshTask{newTask()}}
		meta, err := newExternalCollectionRefreshMeta(
			context.Background(),
			catalog,
			withExternalCollectionRefreshResultStore(resultStore),
		)
		assert.NoError(t, err)

		mockRemove := mockey.Mock((*storage.LocalChunkManager).Remove).
			Return(merr.WrapErrIoFailed("result", errors.New("remove failed"))).
			Build()
		defer mockRemove.UnPatch()

		assert.NoError(t, meta.ClearTaskResult(1001))
		assert.Empty(t, meta.GetTask(1001).GetResultPath())
	})
}

func TestExternalCollectionRefreshMeta_UpdateTaskProgress(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Progress: 0},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.UpdateTaskProgress(1001, 50)
		assert.Error(t, err)
		assert.Equal(t, int64(0), meta.GetTask(1001).GetProgress())
	})

	t.Run("success", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Progress: 0},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.UpdateTaskProgress(1001, 50)
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, int64(50), task.GetProgress())
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.UpdateTaskProgress(9999, 50)
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_StartTaskAttempt(t *testing.T) {
	t.Run("save_failed", func(t *testing.T) {
		catalog := &stubCatalog{}
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Version: 0, NodeId: 0},
		}
		mockListJobs := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshJobs).Return(nil, nil).Build()
		defer mockListJobs.UnPatch()
		mockListTasks := mockey.Mock((*stubCatalog).ListExternalCollectionRefreshTasks).Return(tasks, nil).Build()
		defer mockListTasks.UnPatch()

		meta, err := newExternalCollectionRefreshMeta(context.Background(), catalog)
		assert.NoError(t, err)

		mockSave := mockey.Mock((*stubCatalog).SaveExternalCollectionRefreshTask).Return(errors.New("save error")).Build()
		defer mockSave.UnPatch()

		err = meta.StartTaskAttempt(1001, 10, map[int64]string{1: "manifest-1"})
		assert.Error(t, err)
		// No part of the attempt snapshot becomes visible when the task write fails.
		task := meta.GetTask(1001)
		assert.Equal(t, int64(0), task.GetVersion())
		assert.Equal(t, int64(0), task.GetNodeId())
		assert.Equal(t, indexpb.JobState_JobStateNone, task.GetState())
		assert.Empty(t, task.GetBaseManifests())
	})

	t.Run("success", func(t *testing.T) {
		tasks := []*datapb.ExternalCollectionRefreshTask{
			{TaskId: 1001, JobId: 1, Version: 7, NodeId: 0},
		}
		meta := createMetaTestRefreshMeta(t, nil, tasks)

		err := meta.StartTaskAttempt(1001, 10, map[int64]string{1: "manifest-1"})
		assert.NoError(t, err)

		task := meta.GetTask(1001)
		assert.Equal(t, int64(7), task.GetVersion())
		assert.Equal(t, int64(10), task.GetNodeId())
		assert.Equal(t, indexpb.JobState_JobStateInProgress, task.GetState())
		assert.Equal(t, map[int64]string{1: "manifest-1"}, task.GetBaseManifests())
	})

	t.Run("task_not_found", func(t *testing.T) {
		meta := createMetaTestRefreshMeta(t, nil, nil)

		err := meta.StartTaskAttempt(9999, 10, nil)
		assert.Error(t, err)
	})
}

func TestExternalCollectionRefreshMeta_WorkerFailureCountIsProcessLocal(t *testing.T) {
	tasks := []*datapb.ExternalCollectionRefreshTask{{
		TaskId: 1001,
		JobId:  1,
		State:  indexpb.JobState_JobStateInProgress,
	}}
	meta, err := newExternalCollectionRefreshMeta(context.Background(), &stubCatalog{tasks: tasks})
	assert.NoError(t, err)
	_, count, applied, err := meta.RecordTaskWorkerFailure(1001, 10, "failed")
	assert.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, int64(1), count)
	assert.NoError(t, meta.UpdateTaskState(1001, indexpb.JobState_JobStateInProgress, ""))
	_, count, applied, err = meta.RecordTaskWorkerFailure(1001, 10, "failed again")
	assert.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, int64(2), count)

	// A new meta instance models DataCoord restart: retry history is not loaded
	// from catalog and the task receives a fresh in-process retry budget.
	restarted, err := newExternalCollectionRefreshMeta(context.Background(), &stubCatalog{tasks: tasks})
	assert.NoError(t, err)
	_, count, applied, err = restarted.RecordTaskWorkerFailure(1001, 10, "failed")
	assert.NoError(t, err)
	assert.True(t, applied)
	assert.Equal(t, int64(1), count)
}

func TestExternalCollectionRefreshMeta_StaleWorkerFailureDoesNotReopenTerminalTask(t *testing.T) {
	for _, state := range []indexpb.JobState{
		indexpb.JobState_JobStateFinished,
		indexpb.JobState_JobStateFailed,
		indexpb.JobState_JobStateRetry,
	} {
		t.Run(state.String(), func(t *testing.T) {
			task := &datapb.ExternalCollectionRefreshTask{
				TaskId: 1001,
				JobId:  1,
				State:  state,
			}
			meta, err := newExternalCollectionRefreshMeta(context.Background(), &stubCatalog{tasks: []*datapb.ExternalCollectionRefreshTask{task}})
			assert.NoError(t, err)

			updated, count, applied, err := meta.RecordTaskWorkerFailure(task.GetTaskId(), 10, "stale failure")
			assert.NoError(t, err)
			assert.False(t, applied)
			assert.Zero(t, count)
			assert.Equal(t, state, updated.GetState())
			assert.Equal(t, state, meta.GetTask(task.GetTaskId()).GetState())
		})
	}
}

func TestExternalCollectionRefreshMeta_AggregateJobStateFromTasks(t *testing.T) {
	testCases := []struct {
		name             string
		tasks            []*datapb.ExternalCollectionRefreshTask
		expectedState    indexpb.JobState
		expectedProgress int64
	}{
		{
			name:             "no_tasks",
			tasks:            nil,
			expectedState:    indexpb.JobState_JobStateNone,
			expectedProgress: 0,
		},
		{
			name: "all_init",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateInit, Progress: 0},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInit, Progress: 0},
			},
			expectedState:    indexpb.JobState_JobStateInit,
			expectedProgress: 0,
		},
		{
			name: "all_finished",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
			},
			expectedState:    indexpb.JobState_JobStateFinished,
			expectedProgress: 100,
		},
		{
			name: "has_in_progress",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 50},
			},
			expectedState:    indexpb.JobState_JobStateInProgress,
			expectedProgress: 75, // (100+50)/2
		},
		{
			name: "has_failed",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInProgress, Progress: 50},
				{TaskId: 1003, JobId: 1, State: indexpb.JobState_JobStateFailed, Progress: 30},
			},
			expectedState:    indexpb.JobState_JobStateFailed,
			expectedProgress: 60, // (100+50+30)/3
		},
		{
			name: "has_retry",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateRetry, Progress: 20},
			},
			expectedState:    indexpb.JobState_JobStateRetry,
			expectedProgress: 60, // (100+20)/2
		},
		{
			name: "init_over_finished",
			tasks: []*datapb.ExternalCollectionRefreshTask{
				{TaskId: 1001, JobId: 1, State: indexpb.JobState_JobStateFinished, Progress: 100},
				{TaskId: 1002, JobId: 1, State: indexpb.JobState_JobStateInit, Progress: 0},
			},
			expectedState:    indexpb.JobState_JobStateInit,
			expectedProgress: 50, // (100+0)/2
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskIDs := make([]int64, 0, len(tc.tasks))
			for _, task := range tc.tasks {
				taskIDs = append(taskIDs, task.GetTaskId())
			}
			meta := createMetaTestRefreshMeta(t, []*datapb.ExternalCollectionRefreshJob{{
				JobId:        1,
				CollectionId: 100,
				TaskIds:      taskIDs,
			}}, tc.tasks)

			state, progress, err := meta.AggregateJobStateFromTasks(1)
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedState, state)
			assert.Equal(t, tc.expectedProgress, progress)
		})
	}
}
