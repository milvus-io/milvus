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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
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
		ctx:      ctx,
		mixCoord: mixCoord,
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

func TestFlushAllTaskPostExecute(t *testing.T) {
	task, mixCoord, replicateMsgStream, ctx := createTestFlushAllTask(t)
	defer mixCoord.AssertExpectations(t)
	defer replicateMsgStream.AssertExpectations(t)

	err := task.PostExecute(ctx)
	assert.NoError(t, err)
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
