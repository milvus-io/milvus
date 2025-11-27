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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
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

func TestExternalCollectionManager_SubmitTask_Success(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(2)
	collID := int64(200)

	req := &datapb.UpdateExternalCollectionRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	// Track task execution
	executed := false
	taskFunc := func(ctx context.Context) (*datapb.UpdateExternalCollectionResponse, error) {
		executed = true
		return &datapb.UpdateExternalCollectionResponse{
			State:        indexpb.JobState_JobStateFinished,
			KeptSegments: []int64{1, 2},
		}, nil
	}

	// Submit task
	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err)

	// Wait for task to execute
	time.Sleep(100 * time.Millisecond)

	// Verify task was executed
	assert.True(t, executed)

	// Task info should be retained until explicit drop
	info := manager.Get(clusterID, taskID)
	assert.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateFinished, info.State)
	assert.Equal(t, []int64{1, 2}, info.KeptSegments)
}

func TestExternalCollectionManager_SubmitTask_Failure(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(3)
	collID := int64(300)

	req := &datapb.UpdateExternalCollectionRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	// Task function that fails
	expectedError := errors.New("task execution failed")
	taskFunc := func(ctx context.Context) (*datapb.UpdateExternalCollectionResponse, error) {
		return nil, expectedError
	}

	// Submit task
	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err) // Submit should succeed

	// Wait a bit for task to execute
	time.Sleep(100 * time.Millisecond)

	// Task info should still be present with failure state
	info := manager.Get(clusterID, taskID)
	assert.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateFailed, info.State)
	assert.Equal(t, expectedError.Error(), info.FailReason)
}

func TestExternalCollectionManager_CancelTask(t *testing.T) {
	ctx := context.Background()
	manager := NewExternalCollectionManager(ctx, 4)
	defer manager.Close()

	clusterID := "test-cluster"
	taskID := int64(30)
	collID := int64(3000)

	req := &datapb.UpdateExternalCollectionRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	cancelObserved := make(chan struct{})
	taskFunc := func(ctx context.Context) (*datapb.UpdateExternalCollectionResponse, error) {
		select {
		case <-ctx.Done():
			close(cancelObserved)
			return nil, ctx.Err()
		case <-time.After(time.Second):
			return &datapb.UpdateExternalCollectionResponse{
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

	cancelled := manager.CancelTask(clusterID, taskID)
	assert.True(t, cancelled)

	require.Eventually(t, func() bool {
		select {
		case <-cancelObserved:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	info := manager.Get(clusterID, taskID)
	require.NotNil(t, info)
	assert.Equal(t, indexpb.JobState_JobStateFailed, info.State)
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

	req := &datapb.UpdateExternalCollectionRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	// Task function that blocks
	blockChan := make(chan struct{})
	taskFunc := func(ctx context.Context) (*datapb.UpdateExternalCollectionResponse, error) {
		<-blockChan
		return &datapb.UpdateExternalCollectionResponse{
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

	// Try to submit duplicate task
	err = manager.SubmitTask(clusterID, req, taskFunc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task already exists")

	// Unblock the task
	close(blockChan)

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)
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

		req := &datapb.UpdateExternalCollectionRequest{
			TaskID:       taskID,
			CollectionID: collID,
		}

		taskFunc := func(ctx context.Context) (*datapb.UpdateExternalCollectionResponse, error) {
			time.Sleep(10 * time.Millisecond)
			return &datapb.UpdateExternalCollectionResponse{
				State: indexpb.JobState_JobStateFinished,
			}, nil
		}

		err := manager.SubmitTask(clusterID, req, taskFunc)
		assert.NoError(t, err)
	}

	// Wait for all tasks to complete
	time.Sleep(200 * time.Millisecond)

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

	req := &datapb.UpdateExternalCollectionRequest{
		TaskID:       taskID,
		CollectionID: collID,
	}

	// Submit a task
	executed := false
	taskFunc := func(ctx context.Context) (*datapb.UpdateExternalCollectionResponse, error) {
		time.Sleep(50 * time.Millisecond)
		executed = true
		return &datapb.UpdateExternalCollectionResponse{
			State: indexpb.JobState_JobStateFinished,
		}, nil
	}

	err := manager.SubmitTask(clusterID, req, taskFunc)
	assert.NoError(t, err)

	// Close manager
	manager.Close()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Task should have executed before close
	assert.True(t, executed)
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
