// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBaseTaskQueue_addUnissuedTask(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &taskScheduler{
		ctx:    ctx,
		cancel: cancel,
	}

	t.Run("test full", func(t *testing.T) {
		taskQueue := newLoadAndReleaseTaskQueue(s)
		task := &mockTask{}
		for i := 0; i < maxTaskNum; i++ {
			err := taskQueue.addUnissuedTask(task)
			assert.NoError(t, err)
		}
		err := taskQueue.addUnissuedTask(task)
		assert.Error(t, err)
	})

	t.Run("add task to front", func(t *testing.T) {
		taskQueue := newLoadAndReleaseTaskQueue(s)
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
