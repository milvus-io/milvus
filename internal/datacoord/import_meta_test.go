// Licensed to the LF AI & Data foundation under one
// or more contributor license agreementassert. See the NOTICE file
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
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func TestImportMeta_Restore(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportTasks().Return([]any{
		&datapb.PreImportTask{
			TaskID: 1,
		},
		&datapb.ImportTaskV2{
			TaskID: 2,
		},
	}, nil)

	meta, err := NewImportMeta(nil, catalog)
	assert.NoError(t, err)

	tasks := meta.GetBy()
	assert.Equal(t, 2, len(tasks))
	tasks = meta.GetBy(WithType(PreImportTaskType))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, int64(1), tasks[0].GetTaskID())
	tasks = meta.GetBy(WithType(ImportTaskType))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, int64(2), tasks[0].GetTaskID())
}

func TestImportMeta_Normal(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().DropImportTask(mock.Anything).Return(nil)

	meta, err := NewImportMeta(nil, catalog)
	assert.NoError(t, err)

	task1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        internalpb.ImportState_Pending,
		},
	}
	err = meta.Add(task1)
	assert.NoError(t, err)
	err = meta.Add(task1)
	assert.NoError(t, err)
	res := meta.Get(task1.GetTaskID())
	assert.Equal(t, task1, res)

	task2 := task1.Clone()
	task2.(*importTask).TaskID = 8
	task2.(*importTask).State = internalpb.ImportState_Completed
	err = meta.Add(task2)
	assert.NoError(t, err)

	tasks := meta.GetBy(WithJob(task1.GetJobID()))
	assert.Equal(t, 2, len(tasks))
	tasks = meta.GetBy(WithType(ImportTaskType), WithStates(internalpb.ImportState_Completed))
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, task2.GetTaskID(), tasks[0].GetTaskID())

	err = meta.Update(task1.GetTaskID(), UpdateNodeID(9),
		UpdateState(internalpb.ImportState_Failed),
		UpdateFileStats([]*datapb.ImportFileStats{{
			FileSize: 100,
		}}))
	assert.NoError(t, err)
	task := meta.Get(task1.GetTaskID())
	assert.Equal(t, int64(9), task.GetNodeID())
	assert.Equal(t, internalpb.ImportState_Failed, task.GetState())

	err = meta.Remove(task1.GetTaskID())
	assert.NoError(t, err)
	tasks = meta.GetBy()
	assert.Equal(t, 1, len(tasks))
	err = meta.Remove(10)
	assert.NoError(t, err)
	tasks = meta.GetBy()
	assert.Equal(t, 1, len(tasks))
}

func TestImportMeta_Failed(t *testing.T) {
	mockErr := errors.New("mock err")
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(mockErr)
	catalog.EXPECT().DropImportTask(mock.Anything).Return(mockErr)

	meta, err := NewImportMeta(nil, catalog)
	assert.NoError(t, err)

	task1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:        1,
			TaskID:       2,
			CollectionID: 3,
			SegmentIDs:   []int64{5, 6},
			NodeID:       7,
			State:        internalpb.ImportState_Pending,
		},
	}
	err = meta.Add(task1)
	assert.Error(t, err)
	meta.(*importMeta).tasks[task1.GetTaskID()] = task1
	err = meta.Update(task1.GetTaskID(), UpdateNodeID(9))
	assert.Error(t, err)
	err = meta.Remove(task1.GetTaskID())
	assert.Error(t, err)
}
