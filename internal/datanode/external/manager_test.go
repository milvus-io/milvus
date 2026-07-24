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

package external

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestExternalCollectionManager_Basic(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(1)
	collID := int64(100)

	// registerAttempt - first time should succeed
	info := &TaskInfo{
		Cancel:     func() {},
		State:      indexpb.JobState_JobStateInProgress,
		FailReason: "",
		CollID:     collID,
		Version:    1,
	}
	assert.True(t, manager.registerAttempt(clusterID, taskID, info))

	// Test Get
	retrievedInfo := manager.Get(clusterID, taskID)
	assert.NotNil(t, retrievedInfo)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, retrievedInfo.State)
	assert.Equal(t, collID, retrievedInfo.CollID)

	// registerAttempt - same version is a duplicate dispatch: keep existing entry
	newInfo := &TaskInfo{
		Cancel:     func() {},
		State:      indexpb.JobState_JobStateFinished,
		FailReason: "",
		CollID:     collID,
		Version:    1,
	}
	assert.False(t, manager.registerAttempt(clusterID, taskID, newInfo))
	assert.Equal(t, indexpb.JobState_JobStateInProgress, manager.Get(clusterID, taskID).State) // should still be old state

	// A result write with a stale version must be dropped
	manager.UpdateResult(clusterID, taskID, 0, indexpb.JobState_JobStateFinished, "", nil, nil)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, manager.Get(clusterID, taskID).State)

	// A result write with the matching version lands
	manager.UpdateResult(clusterID, taskID, 1, indexpb.JobState_JobStateFinished, "", nil, nil)
	retrievedInfo = manager.Get(clusterID, taskID)
	assert.Equal(t, indexpb.JobState_JobStateFinished, retrievedInfo.State)

	// registerAttempt - a NEWER version supersedes: cancels and replaces the entry
	superseding := &TaskInfo{
		Cancel:  func() {},
		State:   indexpb.JobState_JobStateInProgress,
		CollID:  collID,
		Version: 2,
	}
	assert.True(t, manager.registerAttempt(clusterID, taskID, superseding))
	assert.Equal(t, int64(2), manager.Get(clusterID, taskID).Version)
	// The superseded attempt's result write (version 1) is dropped.
	manager.UpdateResult(clusterID, taskID, 1, indexpb.JobState_JobStateFinished, "", nil, nil)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, manager.Get(clusterID, taskID).State)

	// Drop via the version-aware API (version 0 forces removal).
	deletedInfo := manager.DeleteIfVersion(clusterID, taskID, 0)
	assert.NotNil(t, deletedInfo)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, deletedInfo.State)

	// Verify task is deleted
	retrievedInfo = manager.Get(clusterID, taskID)
	assert.Nil(t, retrievedInfo)
}

func TestExternalCollectionManager_SubmitTask_Success(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(2)
	collID := int64(200)

	req := &datapb.RefreshExternalCollectionTaskRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	// Track task execution
	var executed atomic.Bool
	taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		executed.Store(true)
		return &datapb.RefreshExternalCollectionTaskResponse{
			State:        indexpb.JobState_JobStateFinished,
			KeptSegments: []int64{1, 2},
		}, nil
	}

	// Submit task
	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err)

	require.Eventually(t, executed.Load, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateFinished
	}, time.Second, 10*time.Millisecond)

	// Verify task was executed
	assert.True(t, executed.Load())

	// Task info should be retained until explicit drop
	info := manager.Get(clusterID, taskID)
	assert.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateFinished, info.State)
	assert.Equal(t, []int64{1, 2}, info.KeptSegments)
}

func TestExternalCollectionManager_SubmitTask_DefaultsNoneStateToFinished(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(20)
	collID := int64(2000)

	req := &datapb.RefreshExternalCollectionTaskRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	err := manager.SubmitTask(clusterID, req, func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		return &datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateNone,
		}, nil
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateFinished
	}, time.Second, 10*time.Millisecond)
}

func TestExternalCollectionManager_SubmitTask_Failure(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(3)
	collID := int64(300)

	req := &datapb.RefreshExternalCollectionTaskRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	// Task function that fails with a KNOWN-transient error (object-store
	// throttling): only known-transient failures are re-dispatched, so this
	// reports Retry rather than failing the whole refresh job.
	expectedError := merr.WrapErrIoTooManyRequests("k", errors.New("throttled"))
	taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		return nil, expectedError
	}

	// Submit task
	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err) // Submit should succeed

	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateRetry
	}, time.Second, 10*time.Millisecond)

	// Task info should still be present with the retryable failure state
	info := manager.Get(clusterID, taskID)
	assert.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateRetry, info.State)
	assert.Equal(t, expectedError.Error(), info.FailReason)
}

// A non-retriable data/storage failure (a corrupt manifest, a hard storage
// error) is reproduced by any rerun, so it must surface as Failed rather than
// being re-dispatched to the job deadline like a transient blip.
func TestExternalCollectionManager_SubmitTask_PermanentDataError(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"data_integrity", merr.WrapErrDataIntegrityMsg("corrupt external manifest")},
		{"storage", merr.WrapErrStorageMsg("hard storage error")},
		{"parameter_invalid", merr.WrapErrParameterInvalidMsg("bad external field")},
		// The e2e regression: adding a field whose external column is absent from
		// the source surfaces a non-retriable segcore "column not found"
		// (FieldIDInvalid, code 2020). It must FAIL the task, not loop the refresh
		// forever (job stuck RefreshPending).
		{"missing_external_column", merr.SegcoreError(2020, "Column 'score' not found in schema")},
		// An unknown / untyped build error whose transience we cannot prove also
		// fails fast rather than retrying.
		{"untyped_error", errors.New("unexpected external build failure")},
	}
	for i, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			manager := NewExternalCollectionManager(ctx, 4)
			defer manager.Close()

			clusterID := "test-cluster"
			taskID := int64(500 + i)
			req := &datapb.RefreshExternalCollectionTaskRequest{TaskID: taskID, CollectionID: int64(600 + i)}

			failErr := c.err
			taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
				return nil, failErr
			}

			err := manager.SubmitTask(clusterID, req, taskFunc)
			assert.NoError(t, err)

			require.Eventually(t, func() bool {
				info := manager.Get(clusterID, taskID)
				return info != nil && info.State == indexpb.JobState_JobStateFailed
			}, time.Second, 10*time.Millisecond)

			info := manager.Get(clusterID, taskID)
			assert.NotNil(t, info)
			assert.Equal(t, indexpb.JobState_JobStateFailed, info.State)
		})
	}
}

// Regression for #49225: a panic inside taskFunc (e.g. divide-by-zero from a
// malformed external parquet) must be isolated to the task — the manager pool
// goroutine must NOT crash the process, and the task must surface as Failed.
func TestExternalCollectionManager_SubmitTask_PanicIsolated(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(4242)
	collID := int64(9999)

	req := &datapb.RefreshExternalCollectionTaskRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		var zero int64
		// Reproduces the original #49225 crash shape.
		_ = int64(1) / zero
		return nil, nil
	}

	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateFailed
	}, time.Second, 10*time.Millisecond)

	info := manager.Get(clusterID, taskID)
	assert.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateFailed, info.State)
	assert.Contains(t, info.FailReason, "panic")
}

func TestExternalCollectionManager_DropCancelsAndRemoves(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(30)
	collID := int64(3000)

	req := &datapb.RefreshExternalCollectionTaskRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	cancelObserved := make(chan struct{})
	taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		select {
		case <-ctx.Done():
			close(cancelObserved)
			return nil, ctx.Err()
		case <-time.After(time.Second):
			return &datapb.RefreshExternalCollectionTaskResponse{
				State: indexpb.JobState_JobStateFinished,
			}, nil
		}
	}

	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		// ensure task has been registered
		info := manager.Get(clusterID, taskID)
		return info != nil
	}, time.Second, 10*time.Millisecond)

	// Dropping a running task cancels its context and removes the entry — the
	// production cancel path (services.go Drop -> DeleteIfVersion). version 0
	// forces the removal.
	removed := manager.DeleteIfVersion(clusterID, taskID, 0)
	require.NotNil(t, removed)

	// The running attempt observes the cancellation and aborts.
	require.Eventually(t, func() bool {
		select {
		case <-cancelObserved:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	// The entry is gone after the drop.
	assert.Nil(t, manager.Get(clusterID, taskID))
}

func TestCloneSegmentIDs(t *testing.T) {
	src := []int64{1, 2, 3}
	dst := cloneSegmentIDs(src)

	assert.Equal(t, src, dst)
	dst[0] = 42
	assert.NotEqual(t, src[0], dst[0], "modifying clone should not affect source")
}

func TestExtractSegmentIDs(t *testing.T) {
	assert.Nil(t, extractSegmentIDs(nil))

	segments := []*datapb.SegmentInfo{
		nil,
		{ID: 1},
		{ID: 2},
	}
	assert.Equal(t, []int64{1, 2}, extractSegmentIDs(segments))
}

func TestDeleteIfVersionIdempotent(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 1)
	defer manager.Close()

	var calls int32
	cancelFn := func() {
		atomic.AddInt32(&calls, 1)
	}

	clusterID := "cluster"
	taskID := int64(999)

	manager.registerAttempt(clusterID, taskID, &TaskInfo{
		Cancel: cancelFn,
	})

	// First drop removes the entry and cancels its context exactly once.
	require.NotNil(t, manager.DeleteIfVersion(clusterID, taskID, 0))
	// A second drop is a no-op: the entry is already gone, so Cancel is not
	// invoked again.
	require.Nil(t, manager.DeleteIfVersion(clusterID, taskID, 0))
	assert.Equal(t, int32(1), calls)
}

func TestExternalCollectionManager_SubmitTask_Duplicate(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(4)
	collID := int64(400)

	req := &datapb.RefreshExternalCollectionTaskRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	// Task function that blocks
	blockChan := make(chan struct{})
	taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		<-blockChan
		return &datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateFinished,
		}, nil
	}

	// Submit first task
	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err)

	// Verify task is in progress
	info := manager.Get(clusterID, taskID)
	assert.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, info.State)

	// Duplicate submit should be idempotent (no error)
	err = manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err)

	// Unblock the task
	close(blockChan)

	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateFinished
	}, time.Second, 10*time.Millisecond)
}

func TestExternalCollectionManager_MultipleTasksConcurrent(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	numTasks := 10

	// Submit multiple tasks concurrently
	for i := 0; i < numTasks; i++ {
		taskID := int64(i + 100)
		collID := int64(i + 1000)

		req := &datapb.RefreshExternalCollectionTaskRequest{
			TaskID:       taskID,
			CollectionID: collID,
		}

		taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			return &datapb.RefreshExternalCollectionTaskResponse{
				State: indexpb.JobState_JobStateFinished,
			}, nil
		}

		err := manager.SubmitTask(clusterID, req, taskFunc)
		assert.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		for i := 0; i < numTasks; i++ {
			taskID := int64(i + 100)
			info := manager.Get(clusterID, taskID)
			if info == nil || info.State != indexpb.JobState_JobStateFinished {
				return false
			}
		}
		return true
	}, time.Second, 10*time.Millisecond)

	// Tasks remain queryable until dropped
	for i := 0; i < numTasks; i++ {
		taskID := int64(i + 100)
		info := manager.Get(clusterID, taskID)
		assert.NotNil(t, info)
		assert.Equal(t, indexpb.JobState_JobStateFinished, info.State)
	}
}

func TestExternalCollectionManager_Close(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)

	clusterID := "test-cluster"
	taskID := int64(5)
	collID := int64(500)

	req := &datapb.RefreshExternalCollectionTaskRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	// Submit a task
	var executed atomic.Bool
	started := make(chan struct{})
	unblock := make(chan struct{})
	taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		close(started)
		select {
		case <-unblock:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		executed.Store(true)
		return &datapb.RefreshExternalCollectionTaskResponse{
			State: indexpb.JobState_JobStateFinished,
		}, nil
	}

	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err)

	require.Eventually(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	// Close manager while the task is still running
	manager.Close()

	close(unblock)

	require.Eventually(t, executed.Load, time.Second, 10*time.Millisecond)

	// Task should have executed before close
	assert.True(t, executed.Load())
}

func TestExternalCollectionManager_UpdateResultNonExistent(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(999)

	// Try to update result of non-existent task (should not panic)
	manager.UpdateResult(clusterID, taskID, 0, indexpb.JobState_JobStateFinished, "", nil, nil)

	// Get should return nil
	info := manager.Get(clusterID, taskID)
	assert.Nil(t, info)
}

func TestExternalCollectionManager_DeleteIfVersionNonExistent(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(888)

	// Try to drop a non-existent task
	info := manager.DeleteIfVersion(clusterID, taskID, 0)
	assert.Nil(t, info)
}

// TestExternalCollectionManager_VersionFencesSupersededAttempt reproduces the
// retry ABA race: attempt v1 is dispatched and still running when DataCoord
// re-dispatches the same taskID as attempt v2 (after a stale-manifest reset).
// The v2 registration supersedes v1; when the slow v1 goroutine finally
// completes, its result write must be dropped so it cannot overwrite v2's
// state with a stale finished result.
func TestExternalCollectionManager_VersionFencesSupersededAttempt(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(4242)

	release := make(chan struct{})
	started := make(chan struct{})
	reqV1 := &datapb.RefreshExternalCollectionTaskRequest{TaskID: taskID, TaskVersion: 1}
	err := manager.SubmitTask(clusterID, reqV1, func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		close(started)
		<-release // simulate a long-running attempt that ignores cancellation
		return &datapb.RefreshExternalCollectionTaskResponse{
			State:           indexpb.JobState_JobStateFinished,
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 1, ManifestPath: "stale-manifest"}},
		}, nil
	})
	require.NoError(t, err)
	<-started

	// Re-dispatch as attempt v2 while v1 is still running.
	reqV2 := &datapb.RefreshExternalCollectionTaskRequest{TaskID: taskID, TaskVersion: 2}
	v2Done := make(chan struct{})
	err = manager.SubmitTask(clusterID, reqV2, func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		close(v2Done)
		return &datapb.RefreshExternalCollectionTaskResponse{
			State:           indexpb.JobState_JobStateFinished,
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 1, ManifestPath: "fresh-manifest"}},
		}, nil
	})
	require.NoError(t, err)
	<-v2Done

	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateFinished &&
			len(info.UpdatedSegments) == 1 && info.UpdatedSegments[0].GetManifestPath() == "fresh-manifest"
	}, time.Second, 10*time.Millisecond)

	// Let the superseded v1 attempt finish; its result write carries version 1
	// and must be dropped.
	close(release)
	assert.Never(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info == nil || info.UpdatedSegments[0].GetManifestPath() == "stale-manifest"
	}, 200*time.Millisecond, 20*time.Millisecond)
	info := manager.Get(clusterID, taskID)
	assert.Equal(t, int64(2), info.Version)
	assert.Equal(t, "fresh-manifest", info.UpdatedSegments[0].GetManifestPath())
}

// TestExternalCollectionManager_ClassifiesExecutionFailures verifies the worker-side
// blame-test classification: a transient execution failure (object-store I/O etc.)
// reports Retry so DataCoord re-dispatches, while a request/config error reports a
// permanent Failed.
func TestExternalCollectionManager_ClassifiesExecutionFailures(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"

	// Transient failure → Retry. A real object-store timeout from the worker's
	// build/sample surfaces as a typed-retriable segcore error (S3Error, code
	// 2018), not a bare error — only known-transient failures are re-dispatched.
	transientID := int64(5001)
	err := manager.SubmitTask(clusterID,
		&datapb.RefreshExternalCollectionTaskRequest{TaskID: transientID, TaskVersion: 1},
		func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			return nil, merr.SegcoreError(2018, "S3Error: read timeout")
		})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, transientID)
		return info != nil && info.State == indexpb.JobState_JobStateRetry
	}, time.Second, 10*time.Millisecond)

	// Request/config error → permanent Failed.
	permanentID := int64(5002)
	err = manager.SubmitTask(clusterID,
		&datapb.RefreshExternalCollectionTaskRequest{TaskID: permanentID, TaskVersion: 1},
		func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			return nil, merr.WrapErrParameterInvalidMsg("bad external spec")
		})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, permanentID)
		return info != nil && info.State == indexpb.JobState_JobStateFailed
	}, time.Second, 10*time.Millisecond)
}

// TestExternalCollectionManager_DeleteIfVersionFencesStaleDrop reproduces the
// stale-drop ABA: a delayed Drop for a superseded attempt (v1) must NOT delete
// the entry now belonging to the re-dispatched attempt (v2).
func TestExternalCollectionManager_DeleteIfVersionFencesStaleDrop(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(7001)

	require.True(t, manager.registerAttempt(clusterID, taskID, &TaskInfo{
		Cancel: func() {}, State: indexpb.JobState_JobStateInProgress, Version: 1,
	}))
	// Supersede with v2.
	require.True(t, manager.registerAttempt(clusterID, taskID, &TaskInfo{
		Cancel: func() {}, State: indexpb.JobState_JobStateInProgress, Version: 2,
	}))

	// A delayed v1 drop must be ignored — the entry is v2 now.
	removed := manager.DeleteIfVersion(clusterID, taskID, 1)
	assert.Nil(t, removed)
	require.NotNil(t, manager.Get(clusterID, taskID))
	assert.Equal(t, int64(2), manager.Get(clusterID, taskID).Version)

	// The matching v2 drop removes it.
	removed = manager.DeleteIfVersion(clusterID, taskID, 2)
	require.NotNil(t, removed)
	assert.Nil(t, manager.Get(clusterID, taskID))

	// version 0 forces removal regardless (legacy drop).
	require.True(t, manager.registerAttempt(clusterID, taskID, &TaskInfo{Cancel: func() {}, Version: 5}))
	assert.NotNil(t, manager.DeleteIfVersion(clusterID, taskID, 0))
	assert.Nil(t, manager.Get(clusterID, taskID))
}
