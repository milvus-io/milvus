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

package task

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func init() {
	paramtable.Init()
}

func newSchedulerForTest() *taskScheduler {
	return NewScheduler(
		context.Background(),
		nil, nil, nil, nil, nil, nil,
	)
}

func makeTestReplica() *meta.Replica {
	return meta.NewReplica(
		&querypb.Replica{ID: 1, CollectionID: 100},
		nil,
	)
}

// =============================================================================
// HasTaskForNode Tests
// =============================================================================

func TestHasTaskForNode_EmptyQueues(t *testing.T) {
	s := newSchedulerForTest()
	assert.False(t, s.HasTaskForNode(1))
	assert.False(t, s.HasTaskForNode(99))
}

func TestHasTaskForNode_TaskInWaitQueue(t *testing.T) {
	s := newSchedulerForTest()

	task, err := NewSegmentTask(
		context.Background(),
		10*time.Second,
		WrapIDSource(0),
		100,
		makeTestReplica(),
		commonpb.LoadPriority_LOW,
		NewSegmentAction(42, ActionTypeGrow, "ch1", 1000),
	)
	assert.NoError(t, err)
	task.SetID(1)
	s.waitQueue.Add(task)

	assert.True(t, s.HasTaskForNode(42), "should find task for node 42 in waitQueue")
	assert.False(t, s.HasTaskForNode(99), "should not find task for non-existent node")
}

func TestHasTaskForNode_TaskInProcessQueue(t *testing.T) {
	s := newSchedulerForTest()

	task, err := NewSegmentTask(
		context.Background(),
		10*time.Second,
		WrapIDSource(0),
		100,
		makeTestReplica(),
		commonpb.LoadPriority_LOW,
		NewSegmentAction(7, ActionTypeGrow, "ch1", 1000),
	)
	assert.NoError(t, err)
	task.SetID(2)
	s.processQueue.Add(task)

	assert.True(t, s.HasTaskForNode(7), "should find task for node 7 in processQueue")
	assert.False(t, s.HasTaskForNode(99), "should not find task for non-existent node")
}

func TestHasTaskForNode_ChannelTask(t *testing.T) {
	s := newSchedulerForTest()

	task, err := NewChannelTask(
		context.Background(),
		10*time.Second,
		WrapIDSource(0),
		100,
		makeTestReplica(),
		NewChannelAction(55, ActionTypeGrow, "ch1"),
	)
	assert.NoError(t, err)
	task.SetID(3)
	s.waitQueue.Add(task)

	assert.True(t, s.HasTaskForNode(55), "should find channel task for node")
	assert.False(t, s.HasTaskForNode(1), "should not find for different node")
}

func TestHasTaskForNode_MoveTask_BothNodes(t *testing.T) {
	s := newSchedulerForTest()

	// Move task has two actions: grow on dest (node 10), reduce on source (node 20)
	task, err := NewSegmentTask(
		context.Background(),
		10*time.Second,
		WrapIDSource(0),
		100,
		makeTestReplica(),
		commonpb.LoadPriority_LOW,
		NewSegmentAction(10, ActionTypeGrow, "ch1", 1000),
		NewSegmentAction(20, ActionTypeReduce, "ch1", 1000),
	)
	assert.NoError(t, err)
	task.SetID(4)
	s.processQueue.Add(task)

	assert.True(t, s.HasTaskForNode(10), "should find move task for grow node")
	assert.True(t, s.HasTaskForNode(20), "should find move task for reduce node")
	assert.False(t, s.HasTaskForNode(30), "should not find for unrelated node")
}

func TestHasTaskForNode_MultipleTasksMultipleNodes(t *testing.T) {
	s := newSchedulerForTest()

	for i := int64(1); i <= 5; i++ {
		task, err := NewSegmentTask(
			context.Background(),
			10*time.Second,
			WrapIDSource(0),
			100,
			makeTestReplica(),
			commonpb.LoadPriority_LOW,
			NewSegmentAction(i, ActionTypeGrow, "ch1", i*100),
		)
		assert.NoError(t, err)
		task.SetID(i)
		if i%2 == 0 {
			s.processQueue.Add(task)
		} else {
			s.waitQueue.Add(task)
		}
	}

	for i := int64(1); i <= 5; i++ {
		assert.True(t, s.HasTaskForNode(i), "should find task for node %d", i)
	}
	assert.False(t, s.HasTaskForNode(99))
}

func TestHasTaskForNode_AfterRemoveFromQueue(t *testing.T) {
	s := newSchedulerForTest()

	task, err := NewSegmentTask(
		context.Background(),
		10*time.Second,
		WrapIDSource(0),
		100,
		makeTestReplica(),
		commonpb.LoadPriority_LOW,
		NewSegmentAction(42, ActionTypeGrow, "ch1", 1000),
	)
	assert.NoError(t, err)
	task.SetID(5)
	s.waitQueue.Add(task)

	assert.True(t, s.HasTaskForNode(42))

	s.waitQueue.Remove(task)
	assert.False(t, s.HasTaskForNode(42), "should not find task after removal")
}
