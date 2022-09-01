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

package querynode

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBaseTaskQueue_addUnissuedTask(t *testing.T) {
	s := &taskScheduler{}

	t.Run("test full", func(t *testing.T) {
		taskQueue := newQueryNodeTaskQueue(s)
		task := &mockTask{}
		for i := 0; i < maxTaskNum; i++ {
			err := taskQueue.addUnissuedTask(task)
			assert.NoError(t, err)
		}
		err := taskQueue.addUnissuedTask(task)
		assert.Error(t, err)
	})

	t.Run("add task to front", func(t *testing.T) {
		taskQueue := newQueryNodeTaskQueue(s)
		mt := &mockTask{
			timestamp: 1000,
		}
		err := taskQueue.addUnissuedTask(mt)
		fmt.Println(taskQueue.unissuedTasks.Back().Value.(task).Timestamp())
		assert.NoError(t, err)
		err = taskQueue.addUnissuedTask(mt)
		fmt.Println(taskQueue.unissuedTasks.Back().Value.(task).Timestamp())
		assert.NoError(t, err)
		mt2 := &mockTask{
			timestamp: 0,
		}
		err = taskQueue.addUnissuedTask(mt2)
		assert.NoError(t, err)
	})
}
