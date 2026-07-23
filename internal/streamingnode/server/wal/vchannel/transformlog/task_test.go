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

package transformlog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/util/nodescheduler"
)

func TestTransformLogRegistersTaskBeforeSubmitting(t *testing.T) {
	scheduler := &inspectingTransformTaskScheduler{}
	transformLog := New(Config{
		VChannel: "v1",
		Runtime:  moduleapi.Runtime{Scheduler: scheduler},
	})
	scheduler.onSubmit = func(task nodescheduler.Task) {
		require.Len(t, transformLog.flushTasks, 1)
		assert.Same(t, task, transformLog.flushTasks[0])
	}

	transformLog.submitFlushTask(10)
}

func TestTransformTaskDelaysUntilPredecessorCompletes(t *testing.T) {
	predecessor := &testTransformTask{}
	task := &transformFlushTask{transformTaskBase: transformTaskBase{
		log:          New(Config{VChannel: "v1"}),
		predecessors: []transformTask{predecessor},
	}}

	require.ErrorIs(t, task.Execute(context.Background()), nodescheduler.ErrDelay)
	assert.False(t, task.Done())

	predecessor.done.Store(true)
	require.NoError(t, task.Execute(context.Background()))
	assert.True(t, task.Done())
}

func TestTransformMaterializeTaskDelaysUntilFrontierArrives(t *testing.T) {
	transformLog := New(Config{VChannel: "v1"})
	task := &transformMaterializeTask{transformTaskBase: transformTaskBase{
		log:      transformLog,
		timetick: 10,
	}}

	require.ErrorIs(t, task.Execute(context.Background()), nodescheduler.ErrDelay)
	assert.False(t, task.Done())

	require.True(t, transformLog.syncUp(10).Appended)
	require.NoError(t, task.Execute(context.Background()))
	assert.True(t, task.Done())
}

type testTransformTask struct {
	done atomic.Bool
}

func (t *testTransformTask) Execute(context.Context) error {
	t.done.Store(true)
	return nil
}

func (t *testTransformTask) Done() bool {
	return t.done.Load()
}

type inspectingTransformTaskScheduler struct {
	onSubmit func(nodescheduler.Task)
}

func (s *inspectingTransformTaskScheduler) Submit(task nodescheduler.Task) nodescheduler.TaskHandle {
	s.onSubmit(task)
	return transformTaskHandle{}
}

type transformTaskHandle struct{}

func (transformTaskHandle) Cancel() {}

func (transformTaskHandle) Wait(context.Context) error { return nil }
