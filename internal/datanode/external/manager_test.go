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

	"github.com/milvus-io/milvus/internal/storagev2/packed"
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
	done := make(chan struct{})
	close(done)

	// registerTask - first time should succeed
	info := &TaskInfo{
		Cancel:     func() {},
		Done:       done,
		State:      indexpb.JobState_JobStateInProgress,
		FailReason: "",
		CollID:     collID,
	}
	assert.True(t, manager.registerTask(clusterID, taskID, info))

	// Test Get
	retrievedInfo := manager.Get(clusterID, taskID)
	assert.NotNil(t, retrievedInfo)
	assert.Equal(t, indexpb.JobState_JobStateInProgress, retrievedInfo.State)
	assert.Equal(t, collID, retrievedInfo.CollID)

	// registerTask - an occupied taskID rejects the incoming attempt outright;
	// the resident entry is kept untouched and is NOT canceled.
	newInfo := &TaskInfo{
		Cancel:     func() {},
		State:      indexpb.JobState_JobStateFinished,
		FailReason: "",
		CollID:     collID,
	}
	assert.False(t, manager.registerTask(clusterID, taskID, newInfo))
	assert.Equal(t, indexpb.JobState_JobStateInProgress, manager.Get(clusterID, taskID).State) // should still be old state

	manager.updateResult(clusterID, taskID, indexpb.JobState_JobStateFinished, "", nil, nil, nil)
	retrievedInfo = manager.Get(clusterID, taskID)
	assert.Equal(t, indexpb.JobState_JobStateFinished, retrievedInfo.State)

	// Drop removes the entry unconditionally.
	deletedInfo, err := manager.Delete(context.Background(), clusterID, taskID)
	require.NoError(t, err)
	assert.NotNil(t, deletedInfo)
	assert.Equal(t, indexpb.JobState_JobStateFinished, deletedInfo.State)

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
	// production cancel path (services.go Drop -> Delete).
	removed, err := manager.Delete(context.Background(), clusterID, taskID)
	require.NoError(t, err)
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

func TestDeleteIdempotent(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 1)
	defer manager.Close()

	var calls int32
	cancelFn := func() {
		atomic.AddInt32(&calls, 1)
	}

	clusterID := "cluster"
	taskID := int64(999)

	manager.registerTask(clusterID, taskID, &TaskInfo{
		Cancel: cancelFn,
		Done: func() chan struct{} {
			done := make(chan struct{})
			close(done)
			return done
		}(),
	})

	// First drop removes the entry and cancels its context exactly once.
	removed, err := manager.Delete(context.Background(), clusterID, taskID)
	require.NoError(t, err)
	require.NotNil(t, removed)
	// A second drop is a no-op: the entry is already gone, so Cancel is not
	// invoked again.
	removed, err = manager.Delete(context.Background(), clusterID, taskID)
	require.NoError(t, err)
	require.Nil(t, removed)
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

	// A duplicate submit is REJECTED rather than absorbed: this dispatch carries
	// its own pre-allocated segment ID range, so the resident attempt's result is
	// not a valid substitute for it. DataCoord must drop before re-dispatching.
	err = manager.SubmitTask(clusterID, req, taskFunc)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrTaskDuplicate))

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

func TestExternalCollectionManager_UpdateResultOnMissingTask(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(999)

	// Updating a missing task is a no-op and must not resurrect an entry.
	manager.updateResult(clusterID, taskID, indexpb.JobState_JobStateFinished, "", nil, nil, nil)

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

	// Try to drop a non-existent task
	info, err := manager.Delete(context.Background(), clusterID, taskID)
	require.NoError(t, err)
	assert.Nil(t, info)
}

// TestExternalCollectionManager_RejectsDispatchOntoResidentAttempt covers the
// registration policy: while an attempt is resident under a taskID, a second
// dispatch of the SAME taskID is rejected with ErrTaskDuplicate instead of
// evicting it. The resident attempt keeps running and keeps owning the entry, so
// DataCoord must drop it before a re-dispatch can land.
func TestExternalCollectionManager_RejectsDispatchOntoResidentAttempt(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(4242)

	release := make(chan struct{})
	started := make(chan struct{})
	firstDone := make(chan struct{})
	req := &datapb.RefreshExternalCollectionTaskRequest{TaskID: taskID}
	err := manager.SubmitTask(clusterID, req, func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		close(started)
		<-release
		close(firstDone)
		return &datapb.RefreshExternalCollectionTaskResponse{
			State:           indexpb.JobState_JobStateFinished,
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 1, ManifestPath: "stale-manifest"}},
		}, nil
	})
	require.NoError(t, err)
	<-started

	// A re-dispatch onto the occupied taskID is refused, and its task func never runs.
	var secondRan atomic.Bool
	err = manager.SubmitTask(clusterID, req, func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		secondRan.Store(true)
		return &datapb.RefreshExternalCollectionTaskResponse{State: indexpb.JobState_JobStateFinished}, nil
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrTaskDuplicate))
	assert.False(t, secondRan.Load())
	assert.Equal(t, indexpb.JobState_JobStateInProgress, manager.Get(clusterID, taskID).State)

	// Drop must keep the resident entry installed until the old closure exits.
	dropDone := make(chan struct{})
	var removed *TaskInfo
	var dropErr error
	go func() {
		removed, dropErr = manager.Delete(context.Background(), clusterID, taskID)
		close(dropDone)
	}()
	require.Never(t, func() bool {
		select {
		case <-dropDone:
			return true
		default:
			return false
		}
	}, 100*time.Millisecond, 10*time.Millisecond)
	require.NotNil(t, manager.Get(clusterID, taskID))
	close(release)
	<-firstDone
	<-dropDone
	require.NoError(t, dropErr)
	require.NotNil(t, removed)
	require.Nil(t, manager.Get(clusterID, taskID))

	// Only after Drop returns can the replacement register and execute.
	secondDone := make(chan struct{})
	err = manager.SubmitTask(clusterID, req, func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
		defer close(secondDone)
		return &datapb.RefreshExternalCollectionTaskResponse{
			State:           indexpb.JobState_JobStateFinished,
			UpdatedSegments: []*datapb.SegmentInfo{{ID: 1, ManifestPath: "fresh-manifest"}},
		}, nil
	})
	require.NoError(t, err)
	<-secondDone
	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateFinished &&
			len(info.UpdatedSegments) == 1 && info.UpdatedSegments[0].GetManifestPath() == "fresh-manifest"
	}, time.Second, 10*time.Millisecond)
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
		&datapb.RefreshExternalCollectionTaskRequest{TaskID: transientID},
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
		&datapb.RefreshExternalCollectionTaskRequest{TaskID: permanentID},
		func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			return nil, merr.WrapErrParameterInvalidMsg("bad external spec")
		})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		info := manager.Get(clusterID, permanentID)
		return info != nil && info.State == indexpb.JobState_JobStateFailed
	}, time.Second, 10*time.Millisecond)
}

// TestExternalCollectionManager_DeleteIsUnconditional covers the drop contract
// under the reject-on-occupied policy: at most one attempt is ever resident, so
// a drop never needs to identify WHICH attempt it targets. It removes whatever
// is there and is a no-op when nothing is.
func TestExternalCollectionManager_DeleteIsUnconditional(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(7001)

	require.True(t, manager.registerTask(clusterID, taskID, &TaskInfo{
		Cancel: func() {}, Done: closedDone(), State: indexpb.JobState_JobStateInProgress,
	}))
	// A second dispatch cannot take the slot, so the resident attempt is the
	// only thing a drop can ever remove.
	require.False(t, manager.registerTask(clusterID, taskID, &TaskInfo{
		Cancel: func() {}, State: indexpb.JobState_JobStateInProgress,
	}))

	removed, err := manager.Delete(context.Background(), clusterID, taskID)
	require.NoError(t, err)
	require.NotNil(t, removed)
	assert.Nil(t, manager.Get(clusterID, taskID))

	// Dropping again is a no-op.
	removed, err = manager.Delete(context.Background(), clusterID, taskID)
	require.NoError(t, err)
	assert.Nil(t, removed)
}

func closedDone() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

// TestIsKnownRetryableRefreshError_LoonCompatibility pins both sides of the
// compatibility bridge: the current inner sentinel and a future typed merr.
func TestIsKnownRetryableRefreshError_LoonCompatibility(t *testing.T) {
	current := merr.WrapErrStorage(packed.ErrLoonTransient, "loon write failed")
	require.False(t, merr.IsRetryableErr(current))
	assert.True(t, isKnownRetryableRefreshError(current))

	future := merr.Wrap(
		merr.WrapErrIoTooManyRequests("external object", errors.New("slow down")),
		"loon write failed")
	require.True(t, merr.IsRetryableErr(future))
	assert.True(t, isKnownRetryableRefreshError(future))

	// Permanent storage and build errors stay permanent.
	assert.False(t, isKnownRetryableRefreshError(merr.WrapErrStorageMsg("invalid storage properties")))
	assert.False(t, isKnownRetryableRefreshError(
		merr.WrapErrFieldNotFound("missing_column", "external column not found")))
}

// TestSubmitTask_LoonTransientReportsRetry proves the current wrapped sentinel
// reaches the reported state as Retry rather than permanently failing the job.
func TestSubmitTask_LoonTransientReportsRetry(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(9100)

	done := make(chan struct{})
	err := manager.SubmitTask(clusterID,
		&datapb.RefreshExternalCollectionTaskRequest{TaskID: taskID},
		func(taskCtx context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error) {
			defer close(done)
			return nil, merr.WrapErrStorage(
				packed.ErrLoonTransient,
				"loon commit failed: AWS Error SLOW_DOWN")
		})
	require.NoError(t, err)
	<-done

	assert.Eventually(t, func() bool {
		info := manager.Get(clusterID, taskID)
		return info != nil && info.State == indexpb.JobState_JobStateRetry
	}, 5*time.Second, 10*time.Millisecond)
}
