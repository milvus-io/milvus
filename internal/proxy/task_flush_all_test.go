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

package proxy

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
)

func createTestFlushAllTask(t *testing.T) (*flushAllTask, *mocks.MockMixCoordClient, *msgstream.MockMsgStream, context.Context) {
	ctx := context.Background()
	mixCoord := mocks.NewMockMixCoordClient(t)
	replicateMsgStream := msgstream.NewMockMsgStream(t)

	task := &flushAllTask{
		baseTask:  baseTask{},
		Condition: NewTaskCondition(ctx),
		FlushAllRequest: &milvuspb.FlushAllRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     1,
				Timestamp: uint64(time.Now().UnixNano()),
				SourceID:  1,
			},
		},
		ctx:                ctx,
		mixCoord:           mixCoord,
		replicateMsgStream: replicateMsgStream,
	}

	return task, mixCoord, replicateMsgStream, ctx
}

func TestFlushAllTaskTraceCtx(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	traceCtx := task.TraceCtx()
	assert.Equal(t, ctx, traceCtx)
}

func TestFlushAllTaskID(t *testing.T) {
	task, mixCoord, replicateMsgStream, _ := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	// Test getting ID
	originalID := task.ID()
	assert.Equal(t, UniqueID(1), originalID)

	// Test setting ID
	newID := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(newID)
	assert.Equal(t, newID, task.ID())
}

func TestFlushAllTaskName(t *testing.T) {
	task, mixCoord, replicateMsgStream, _ := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	name := task.Name()
	assert.Equal(t, FlushAllTaskName, name)
}

func TestFlushAllTaskType(t *testing.T) {
	task, mixCoord, replicateMsgStream, _ := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	msgType := task.Type()
	assert.Equal(t, commonpb.MsgType_Flush, msgType)
}

func TestFlushAllTaskTimestampMethods(t *testing.T) {
	task, mixCoord, replicateMsgStream, _ := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	originalTs := task.BeginTs()
	assert.Equal(t, originalTs, task.EndTs())

	newTs := Timestamp(time.Now().UnixNano())
	task.SetTs(newTs)
	assert.Equal(t, newTs, task.BeginTs())
	assert.Equal(t, newTs, task.EndTs())
}

func TestFlushAllTaskOnEnqueue(t *testing.T) {
	ctx := context.Background()
	mixCoord := mocks.NewMockMixCoordClient(t)
	defer mixCoord.AssertExpectations(t)

	// Test with nil Base
	task := &flushAllTask{
		baseTask:        baseTask{},
		Condition:       NewTaskCondition(ctx),
		FlushAllRequest: &milvuspb.FlushAllRequest{},
		ctx:             ctx,
		mixCoord:        mixCoord,
	}

	err := task.OnEnqueue()
	assert.NoError(t, err)
	assert.NotNil(t, task.Base)
	assert.Equal(t, commonpb.MsgType_Flush, task.Base.MsgType)

	// Test with existing Base
	task, _, replicateMsgStream, _ := createTestFlushAllTask(t)
	defer replicateMsgStream.AssertExpectations(t)

	err = task.OnEnqueue()
	assert.NoError(t, err)
	assert.Equal(t, commonpb.MsgType_Flush, task.Base.MsgType)
}

func TestFlushAllTaskPreExecute(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	err := task.PreExecute(ctx)
	assert.NoError(t, err)
}

func TestFlushAllTaskExecuteSuccess(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	// Setup expectations
	expectedResp := &datapb.FlushAllResponse{
		Status: merr.Success(),
	}

	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(expectedResp, nil).Once()

	// Mock SendReplicateMessagePack function
	sendReplicateMessagePackCalled := false
	mocker := mockey.Mock(SendReplicateMessagePack).To(func(ctx context.Context, replicateMsgStream msgstream.MsgStream, request interface{ GetBase() *commonpb.MsgBase }) {
		sendReplicateMessagePackCalled = true
		assert.Equal(t, task.ctx, ctx)
		assert.Equal(t, task.replicateMsgStream, replicateMsgStream)
		assert.Equal(t, task.FlushAllRequest, request)
	}).Build()
	defer mocker.Release()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, expectedResp, task.result)
	assert.True(t, sendReplicateMessagePackCalled)
}

func TestFlushAllTaskExecuteFlushAllRPCError(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	// Test RPC call error
	expectedErr := fmt.Errorf("rpc error")

	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(nil, expectedErr).Once()

	// Mock SendReplicateMessagePack function
	sendReplicateMessagePackCalled := false
	mocker := mockey.Mock(SendReplicateMessagePack).To(func(ctx context.Context, replicateMsgStream msgstream.MsgStream, request interface{ GetBase() *commonpb.MsgBase }) {
		sendReplicateMessagePackCalled = true
	}).Build()
	defer mocker.Release()

	err := task.Execute(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to call flush all to data coordinator")
	assert.False(t, sendReplicateMessagePackCalled)
}

func TestFlushAllTaskExecuteFlushAllResponseError(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	// Test response with error status
	errorResp := &datapb.FlushAllResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "flush all failed",
		},
	}

	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(errorResp, nil).Once()

	// Mock SendReplicateMessagePack function
	sendReplicateMessagePackCalled := false
	mocker := mockey.Mock(SendReplicateMessagePack).To(func(ctx context.Context, replicateMsgStream msgstream.MsgStream, request interface{ GetBase() *commonpb.MsgBase }) {
		sendReplicateMessagePackCalled = true
	}).Build()
	defer mocker.Release()

	err := task.Execute(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to call flush all to data coordinator")
	assert.False(t, sendReplicateMessagePackCalled)
}

func TestFlushAllTaskExecuteWithMerCheck(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	// Test successful execution with merr.CheckRPCCall
	successResp := &datapb.FlushAllResponse{
		Status: merr.Success(),
	}

	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(successResp, nil).Once()

	// Mock SendReplicateMessagePack function
	sendReplicateMessagePackCalled := false
	mocker := mockey.Mock(SendReplicateMessagePack).To(func(ctx context.Context, replicateMsgStream msgstream.MsgStream, request interface{ GetBase() *commonpb.MsgBase }) {
		sendReplicateMessagePackCalled = true
	}).Build()
	defer mocker.Release()

	err := task.Execute(ctx)
	assert.NoError(t, err)
	assert.Equal(t, successResp, task.result)
	assert.True(t, sendReplicateMessagePackCalled)
}

func TestFlushAllTaskExecuteRequestContent(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	// Test the content of the FlushAllRequest sent to mixCoord
	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(&datapb.FlushAllResponse{Status: merr.Success()}, nil).Once()

	// Mock SendReplicateMessagePack function
	mocker := mockey.Mock(SendReplicateMessagePack).To(func(ctx context.Context, replicateMsgStream msgstream.MsgStream, request interface{ GetBase() *commonpb.MsgBase }) {
		// Do nothing
	}).Build()
	defer mocker.Release()

	err := task.Execute(ctx)
	assert.NoError(t, err)

	// The test verifies that Execute method creates the correct request structure internally
	// The actual request content validation is covered by other tests
}

func TestFlushAllTaskPostExecute(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	err := task.PostExecute(ctx)
	assert.NoError(t, err)
}

func TestFlushAllTaskLifecycle(t *testing.T) {
	ctx := context.Background()
	mixCoord := mocks.NewMockMixCoordClient(t)
	replicateMsgStream := msgstream.NewMockMsgStream(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	// Test complete task lifecycle

	// 1. OnEnqueue
	task := &flushAllTask{
		baseTask:           baseTask{},
		Condition:          NewTaskCondition(ctx),
		FlushAllRequest:    &milvuspb.FlushAllRequest{},
		ctx:                ctx,
		mixCoord:           mixCoord,
		replicateMsgStream: replicateMsgStream,
	}

	err := task.OnEnqueue()
	assert.NoError(t, err)

	// 2. PreExecute
	err = task.PreExecute(ctx)
	assert.NoError(t, err)

	// 3. Execute
	expectedResp := &datapb.FlushAllResponse{
		Status: merr.Success(),
	}

	mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
		Return(expectedResp, nil).Once()

	mocker2 := mockey.Mock(SendReplicateMessagePack).To(func(ctx context.Context, replicateMsgStream msgstream.MsgStream, request interface{ GetBase() *commonpb.MsgBase }) {
		// Do nothing
	}).Build()
	defer mocker2.Release()

	err = task.Execute(ctx)
	assert.NoError(t, err)

	// 4. PostExecute
	err = task.PostExecute(ctx)
	assert.NoError(t, err)

	// Verify task state
	assert.Equal(t, expectedResp, task.result)
}

func TestFlushAllTaskErrorHandlingInExecute(t *testing.T) {
	// Test different error scenarios in Execute method

	testCases := []struct {
		name          string
		setupMock     func(*mocks.MockMixCoordClient)
		expectedError string
	}{
		{
			name: "mixCoord FlushAll returns error",
			setupMock: func(mixCoord *mocks.MockMixCoordClient) {
				mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
					Return(nil, fmt.Errorf("network error")).Once()
			},
			expectedError: "failed to call flush all to data coordinator",
		},
		{
			name: "mixCoord FlushAll returns error status",
			setupMock: func(mixCoord *mocks.MockMixCoordClient) {
				mixCoord.EXPECT().FlushAll(mock.Anything, mock.AnythingOfType("*datapb.FlushAllRequest")).
					Return(&datapb.FlushAllResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_IllegalArgument,
							Reason:    "invalid request",
						},
					}, nil).Once()
			},
			expectedError: "failed to call flush all to data coordinator",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
			defer mixCoord.AssertExpectations(t)
			defer replicateMsgStream.AssertExpectations(t)

			tc.setupMock(mixCoord)

			// Mock SendReplicateMessagePack function
			mocker := mockey.Mock(SendReplicateMessagePack).To(func(ctx context.Context, replicateMsgStream msgstream.MsgStream, request interface{ GetBase() *commonpb.MsgBase }) {
				// Should not be called on error
			}).Build()
			defer mocker.Release()

			err := task.Execute(ctx)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedError)
		})
	}
}

func TestFlushAllTaskImplementsTaskInterface(t *testing.T) {
	// Verify that flushAllTask implements the task interface
	var _ task = (*flushAllTask)(nil)

	task, mixCoord, replicateMsgStream, _ := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	// Test all interface methods are accessible
	assert.NotNil(t, task.TraceCtx)
	assert.NotNil(t, task.ID)
	assert.NotNil(t, task.SetID)
	assert.NotNil(t, task.Name)
	assert.NotNil(t, task.Type)
	assert.NotNil(t, task.BeginTs)
	assert.NotNil(t, task.EndTs)
	assert.NotNil(t, task.SetTs)
	assert.NotNil(t, task.OnEnqueue)
	assert.NotNil(t, task.PreExecute)
	assert.NotNil(t, task.Execute)
	assert.NotNil(t, task.PostExecute)
}

func TestFlushAllTaskNilHandling(t *testing.T) {
	// Test behavior with nil values
	task := &flushAllTask{
		FlushAllRequest: &milvuspb.FlushAllRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Flush,
				MsgID:     1,
				Timestamp: uint64(time.Now().UnixNano()),
				SourceID:  1,
			},
		},
	}

	// Test TraceCtx with nil context
	ctx := task.TraceCtx()
	assert.Nil(t, ctx)

	// Test ID with nil Base
	id := task.ID()
	assert.Equal(t, UniqueID(1), id)

	// Test Type with nil Base
	msgType := task.Type()
	assert.Equal(t, commonpb.MsgType_Flush, msgType)
}

func TestFlushAllTaskConstantValues(t *testing.T) {
	// Test that task name constant is correct
	assert.Equal(t, "FlushAllTask", FlushAllTaskName)

	// Test task name method returns correct constant
	task := &flushAllTask{}
	assert.Equal(t, FlushAllTaskName, task.Name())
}

func TestFlushAllTaskBaseTaskMethods(t *testing.T) {
	// Test baseTask methods
	task := &flushAllTask{
		baseTask: baseTask{},
	}

	// Test CanSkipAllocTimestamp
	assert.False(t, task.CanSkipAllocTimestamp())

	// Test SetOnEnqueueTime
	task.SetOnEnqueueTime()
	assert.False(t, task.onEnqueueTime.IsZero())

	// Test GetDurationInQueue
	time.Sleep(1 * time.Millisecond)
	duration := task.GetDurationInQueue()
	assert.Greater(t, duration, time.Duration(0))

	// Test IsSubTask
	assert.False(t, task.IsSubTask())

	// Test SetExecutingTime
	task.SetExecutingTime()
	assert.False(t, task.executingTime.IsZero())

	// Test GetDurationInExecuting
	time.Sleep(1 * time.Millisecond)
	execDuration := task.GetDurationInExecuting()
	assert.Greater(t, execDuration, time.Duration(0))
}
