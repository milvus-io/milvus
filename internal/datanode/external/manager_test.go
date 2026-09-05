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
	"sync"
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

	// Test LoadOrStore - first time should succeed
	info := &TaskInfo{
		Cancel:     func() {},
		State:      indexpb.JobState_JobStateInProgress,
		FailReason: "",
		CollID:     collID,
	}
	oldInfo := manager.LoadOrStore(clusterID, taskID, info)
	assert.Nil(t, oldInfo)

	// Test Get
	retrievedInfo := manager.Get(clusterID, taskID)
	assert.NotNil(t, retrievedInfo)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, retrievedInfo.State)
	assert.Equal(t, collID, retrievedInfo.CollID)

	// Test LoadOrStore - second time should return existing
	newInfo := &TaskInfo{
		Cancel:     func() {},
		State:      indexpb.JobState_JobStateFinished,
		FailReason: "",
		CollID:     collID,
	}
	oldInfo = manager.LoadOrStore(clusterID, taskID, newInfo)
	assert.NotNil(t, oldInfo)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, oldInfo.State) // should still be old state

	// Test UpdateState
	manager.UpdateState(clusterID, taskID, indexpb.JobState_JobStateFinished, "")
	retrievedInfo = manager.Get(clusterID, taskID)
	assert.Equal(t, indexpb.JobState_JobStateFinished, retrievedInfo.State)

	// Test Delete
	deletedInfo := manager.Delete(clusterID, taskID)
	assert.NotNil(t, deletedInfo)
	assert.Equal(t, indexpb.JobState_JobStateFinished, deletedInfo.State)

	// Verify task is deleted
	retrievedInfo = manager.Get(clusterID, taskID)
	assert.Nil(t, retrievedInfo)
}

func TestExternalCollectionManager_RemoveExpiredTasks(t *testing.T) {
	manager := NewExternalCollectionManager(context.Background(), 1)
	defer manager.Close()

	now := time.Now()
	cutoff := now.Add(-24 * time.Hour)
	expiredCtx, expiredCancel := context.WithCancel(context.Background())
	defer expiredCancel()
	runningCtx, runningCancel := context.WithCancel(context.Background())
	defer runningCancel()
	manager.LoadOrStore("cluster", 1, &TaskInfo{
		Cancel:    expiredCancel,
		State:     indexpb.JobState_JobStateFinished,
		StartedAt: cutoff,
	})
	manager.LoadOrStore("cluster", 2, &TaskInfo{
		State:     indexpb.JobState_JobStateFailed,
		StartedAt: cutoff.Add(time.Second),
	})
	manager.LoadOrStore("cluster", 3, &TaskInfo{
		Cancel:    runningCancel,
		State:     indexpb.JobState_JobStateInProgress,
		StartedAt: cutoff.Add(-time.Hour),
	})

	assert.Equal(t, 2, manager.RemoveExpiredTasks(context.Background(), cutoff))

	assert.Nil(t, manager.Get("cluster", 1))
	assert.NotNil(t, manager.Get("cluster", 2))
	assert.Nil(t, manager.Get("cluster", 3))
	for _, taskCtx := range []context.Context{expiredCtx, runningCtx} {
		select {
		case <-taskCtx.Done():
		default:
			t.Fatal("reclaiming an expired task must cancel its context")
		}
	}
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

	// Task function that fails
	expectedError := errors.New("task execution failed")
	taskFunc := func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		return nil, expectedError
	}

	// Submit task
	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err) // Submit should succeed

	// An error that does not blame the request is reported as Retry, so
	// DataCoord spends one of the task's attempts instead of failing the whole
	// refresh on a fault another node might not hit.
	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateRetry
	}, time.Second, 10*time.Millisecond)

	// Task info should still be present with failure state
	info := manager.Get(clusterID, taskID)
	assert.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateRetry, info.State)
	assert.Equal(t, expectedError.Error(), info.FailReason)
}

func TestExternalRefreshFailureState(t *testing.T) {
	t.Run("invalid external field is permanent", func(t *testing.T) {
		err := merr.SegcoreError(2042, "Column 'wrong_col_a' not found in schema")
		assert.Equal(t, merr.InputError, merr.GetErrorType(err))
		assert.Equal(t, indexpb.JobState_JobStateFailed, externalRefreshFailureState(err))
	})

	t.Run("storage failure remains retryable", func(t *testing.T) {
		err := merr.SegcoreError(2045, "object store temporarily unavailable")
		assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
		assert.Equal(t, indexpb.JobState_JobStateRetry, externalRefreshFailureState(err))
	})
}

// Regression for #49225: a panic inside taskFunc (e.g. divide-by-zero from a
// malformed external parquet) must be isolated to the task — the manager pool
// goroutine must NOT crash the process. The recovered panic is a system error,
// so the same failure classifier used by ordinary task errors reports Retry.
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
		return info != nil && info.State == indexpb.JobState_JobStateRetry
	}, time.Second, 10*time.Millisecond)

	info := manager.Get(clusterID, taskID)
	assert.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateRetry, info.State)
	assert.Contains(t, info.FailReason, "panic")

	panicErr := merr.WrapErrServiceInternalMsg("task panicked: integer divide by zero")
	assert.Equal(t, merr.SystemError, merr.GetErrorType(panicErr))
	assert.Equal(t, indexpb.JobState_JobStateRetry, externalRefreshFailureState(panicErr))
}

func TestExternalCollectionManager_CancelTask(t *testing.T) {
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

	canceled := manager.CancelTask(clusterID, taskID)
	assert.True(t, canceled)

	require.Eventually(t, func() bool {
		select {
		case <-cancelObserved:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	// A cancellation is a condition of this node, not of the request, so it is
	// reported as retriable like any other system failure.
	var info *TaskInfo
	require.Eventually(t, func() bool {
		info = manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateRetry
	}, time.Second, 10*time.Millisecond)

	require.NotNil(t, info)
	assert.Contains(t, info.FailReason, "context canceled")
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

func TestCancelTaskMultipleTimes(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 1)
	defer manager.Close()

	var calls int32
	cancelFn := func() {
		atomic.AddInt32(&calls, 1)
	}

	clusterID := "cluster"
	taskID := int64(999)

	manager.LoadOrStore(clusterID, taskID, &TaskInfo{
		Cancel: cancelFn,
	})

	require.True(t, manager.CancelTask(clusterID, taskID))
	require.True(t, manager.CancelTask(clusterID, taskID))
	assert.Equal(t, int32(2), calls)
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

		// SubmitTask refuses when all workers are busy; in production DataCoord
		// retries the dispatch with backoff. Model that here instead of assuming
		// the old blocking-submit behavior.
		require.Eventually(t, func() bool {
			return manager.SubmitTask(clusterID, req, taskFunc) == nil
		}, 5*time.Second, 10*time.Millisecond)
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

func TestExternalCollectionManager_SubmitTaskAfterCloseRejectsWithoutAdmission(t *testing.T) {
	manager := NewExternalCollectionManager(context.Background(), 1)
	manager.Close()

	const clusterID = "test-cluster"
	const taskID = int64(6)
	var executed atomic.Bool
	err := manager.SubmitTask(clusterID,
		&datapb.RefreshExternalCollectionTaskRequest{TaskID: taskID},
		func(context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			executed.Store(true)
			return &datapb.RefreshExternalCollectionTaskResponse{
				State: indexpb.JobState_JobStateFinished,
			}, nil
		})

	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	assert.False(t, executed.Load())
	assert.Nil(t, manager.Get(clusterID, taskID))
	assert.Empty(t, manager.slots)
}

func TestExternalCollectionManager_ConcurrentCloseAndSubmit(t *testing.T) {
	for i := 0; i < 32; i++ {
		manager := NewExternalCollectionManager(context.Background(), 1)
		taskID := int64(i + 100)
		start := make(chan struct{})
		submitResult := make(chan error, 1)
		closeDone := make(chan struct{})
		var executed atomic.Bool

		go func() {
			<-start
			submitResult <- manager.SubmitTask("test-cluster",
				&datapb.RefreshExternalCollectionTaskRequest{TaskID: taskID},
				func(context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
					executed.Store(true)
					return &datapb.RefreshExternalCollectionTaskResponse{
						State: indexpb.JobState_JobStateFinished,
					}, nil
				})
		}()
		go func() {
			<-start
			manager.Close()
			close(closeDone)
		}()

		close(start)
		err := <-submitResult
		<-closeDone
		if err == nil {
			require.Eventually(t, executed.Load, time.Second, time.Millisecond)
		} else {
			assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
			assert.Nil(t, manager.Get("test-cluster", taskID))
		}
		require.Eventually(t, func() bool { return len(manager.slots) == 0 }, time.Second, time.Millisecond)
	}
}

func TestExternalCollectionManager_UpdateStateNonExistent(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(999)

	// Try to update state of non-existent task (should not panic)
	manager.UpdateState(clusterID, taskID, indexpb.JobState_JobStateFinished, "")

	// Get should return nil
	info := manager.Get(clusterID, taskID)
	assert.Nil(t, info)
}

func TestExternalCollectionManager_DeleteNonExistent(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(888)

	// Try to delete non-existent task
	info := manager.Delete(clusterID, taskID)
	assert.Nil(t, info)
}

// A saturated pool is normal operation -- refreshes are long external scans --
// and the caller is a DataCoord RPC handler. Blocking it past the RPC deadline
// made DataCoord give up on a task this node would still run later; the manager
// must refuse instead, with the registration rolled back so the retry can
// re-register cleanly.
func TestExternalCollectionManager_SubmitTask_RefusesWhenPoolSaturated(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 1)
	defer manager.Close()

	clusterID := "test-cluster"
	block := make(chan struct{})
	started := make(chan struct{})

	// Occupy the single worker with a long-running refresh.
	err := manager.SubmitTask(clusterID, &datapb.RefreshExternalCollectionTaskRequest{TaskID: 1},
		func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			close(started)
			<-block
			return &datapb.RefreshExternalCollectionTaskResponse{State: indexpb.JobState_JobStateFinished}, nil
		})
	require.NoError(t, err)
	<-started

	// The next submission must fail fast, not park the RPC handler.
	returned := make(chan error, 1)
	go func() {
		returned <- manager.SubmitTask(clusterID, &datapb.RefreshExternalCollectionTaskRequest{TaskID: 2},
			func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
				t.Error("the refused task must not run")
				return nil, nil
			})
	}()
	select {
	case err := <-returned:
		require.Error(t, err, "a saturated pool must refuse, not accept")
	case <-time.After(5 * time.Second):
		t.Fatal("SubmitTask blocked on a saturated pool")
	}

	// The refused task's registration must be rolled back, or the retry would
	// hit the duplicate-dispatch path and be silently treated as running.
	assert.Nil(t, manager.Get(clusterID, 2),
		"a refused submission must not leave a registered task behind")

	// Once the worker frees, the retry succeeds end to end.
	close(block)
	require.Eventually(t, func() bool {
		return manager.SubmitTask(clusterID, &datapb.RefreshExternalCollectionTaskRequest{TaskID: 2},
			func(ctx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
				return &datapb.RefreshExternalCollectionTaskResponse{State: indexpb.JobState_JobStateFinished}, nil
			}) == nil
	}, 5*time.Second, 20*time.Millisecond, "the retry must succeed once a worker frees")
}

func TestExternalCollectionManager_ConcurrentDuplicateAdmissionWhileSaturated(t *testing.T) {
	manager := NewExternalCollectionManager(context.Background(), 1)
	defer manager.Close()

	const clusterID = "test-cluster"
	blockWorker := make(chan struct{})
	workerStarted := make(chan struct{})
	require.NoError(t, manager.SubmitTask(clusterID,
		&datapb.RefreshExternalCollectionTaskRequest{TaskID: 1},
		func(context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			close(workerStarted)
			<-blockWorker
			return &datapb.RefreshExternalCollectionTaskResponse{
				State: indexpb.JobState_JobStateFinished,
			}, nil
		}))
	<-workerStarted

	const submitters = 64
	start := make(chan struct{})
	errs := make(chan error, submitters)
	var wg sync.WaitGroup
	var rejectedTaskRuns atomic.Int64
	for range submitters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- manager.SubmitTask(clusterID,
				&datapb.RefreshExternalCollectionTaskRequest{TaskID: 2},
				func(context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
					rejectedTaskRuns.Add(1)
					return &datapb.RefreshExternalCollectionTaskResponse{
						State: indexpb.JobState_JobStateFinished,
					}, nil
				})
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.ErrorIs(t, err, merr.ErrServiceTooManyRequests,
			"no duplicate may report success unless an admitted owner exists")
	}
	assert.Nil(t, manager.Get(clusterID, 2),
		"a saturated attempt must never become visible to duplicate submissions")
	assert.Zero(t, rejectedTaskRuns.Load())

	close(blockWorker)
}
