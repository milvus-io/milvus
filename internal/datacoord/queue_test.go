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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_taskQueue(t *testing.T) {
	collID := UniqueID(1234)
	q := newTaskQueue(collID)

	q.push(&indexBuildTask{
		collectionID: collID,
		buildID:      1,
		state:        indexTaskInit,
		failReason:   "",
	})

	q.push(&indexBuildTask{
		collectionID: collID,
		buildID:      2,
		state:        indexTaskInit,
		failReason:   "",
	})

	q.push(&indexBuildTask{
		collectionID: collID,
		buildID:      3,
		state:        indexTaskInit,
		failReason:   "",
	})

	assert.True(t, q.exist(&indexBuildTask{
		collectionID: collID,
		buildID:      1,
		state:        indexTaskInit,
		failReason:   "",
	}))

	assert.False(t, q.exist(&indexBuildTask{
		collectionID: UniqueID(2345),
		buildID:      1,
		state:        indexTaskInit,
		failReason:   "",
	}))

	assert.False(t, q.exist(&indexBuildTask{
		collectionID: collID,
		buildID:      4,
		state:        indexTaskInit,
		failReason:   "",
	}))
	assert.Equal(t, 3, q.len())

	task := q.pop()
	assert.Equal(t, 2, q.len())
	assert.NotNil(t, task)

	q.remove(1)
	assert.Equal(t, 2, q.len())

	q.remove(2)
	assert.Equal(t, 1, q.len())

	task = q.pop()
	assert.Equal(t, 0, q.len())
	assert.NotNil(t, task)

	task = q.pop()
	assert.Equal(t, 0, q.len())
	assert.Nil(t, task)
}

func Test_fairPollingTaskQueue(t *testing.T) {
	t.Run("rotate task", func(t *testing.T) {
		f := newFairQueuePolicy()
		taskCount := 100
		instanceCount := 5
		tasks := make([]*indexBuildTask, 0, taskCount)
		for i := 0; i < taskCount; i++ {
			task := &indexBuildTask{
				collectionID: UniqueID(i % instanceCount),
				buildID:      int64(i),
				state:        0,
				failReason:   "",
			}
			f.Push(task)
			tasks = append(tasks, task)
		}

		assert.Equal(t, taskCount, f.TaskCount())
		i := 0
		for f.TaskCount() != 0 {
			task := f.Pop()
			assert.Equal(t, task.collectionID, tasks[i].collectionID)
			assert.Equal(t, task.buildID, tasks[i].buildID)
			i++
		}
	})

	t.Run("random task", func(t *testing.T) {
		f := newFairQueuePolicy()
		taskCount := 100
		instanceCount := 5
		for i := 0; i < instanceCount; i++ {
			for j := 0; j < taskCount; j++ {
				task := &indexBuildTask{
					collectionID: UniqueID(i % instanceCount),
					buildID:      int64(j),
					state:        0,
					failReason:   "",
				}
				f.Push(task)
			}
		}

		assert.Equal(t, taskCount*instanceCount, f.TaskCount())
		i := 0
		j := 0
		for f.TaskCount() != 0 {
			task := f.Pop()
			assert.Equal(t, task.collectionID, int64(i%instanceCount))
			assert.Equal(t, task.buildID, int64(j/instanceCount))
			i++
			j++
		}
	})
}
