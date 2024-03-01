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

package importv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestImportManager(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := NewTaskManager()
	task1 := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Pending,
		},
		ctx:    ctx,
		cancel: cancel,
	}
	manager.Add(task1)
	manager.Add(task1)
	res := manager.Get(task1.GetTaskID())
	assert.Equal(t, task1, res)

	task2 := &ImportTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       8,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        datapb.ImportTaskStateV2_Completed,
		},
		ctx:    ctx,
		cancel: cancel,
	}
	manager.Add(task2)

	tasks := manager.GetBy()
	assert.Equal(t, 2, len(tasks))
	tasks = manager.GetBy(WithStates(datapb.ImportTaskStateV2_Completed))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, task2.GetTaskID(), tasks[0].GetTaskID())

	manager.Update(task1.GetTaskID(), UpdateState(datapb.ImportTaskStateV2_Failed))
	task := manager.Get(task1.GetTaskID())
	assert.Equal(t, datapb.ImportTaskStateV2_Failed, task.GetState())

	manager.Remove(task1.GetTaskID())
	tasks = manager.GetBy()
	assert.Equal(t, 1, len(tasks))
	manager.Remove(10)
	tasks = manager.GetBy()
	assert.Equal(t, 1, len(tasks))
}
