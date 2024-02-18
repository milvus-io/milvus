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
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func TestImportChecker(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().DropImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().ListSegments(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListChannelCheckpoint(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentIndexes(mock.Anything).Return(nil, nil)
	catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil)

	cluster := NewMockCluster(t)
	cluster.EXPECT().DropImport(mock.Anything, mock.Anything).Return(nil)
	cluster.EXPECT().AddImportSegment(mock.Anything, mock.Anything).Return(nil, nil)

	alloc := NewNMockAllocator(t)
	alloc.EXPECT().allocID(mock.Anything).RunAndReturn(func(ctx context.Context) (int64, error) {
		return rand.Int63(), nil
	})

	imeta, err := NewImportMeta(nil, catalog)
	assert.NoError(t, err)

	job := &importJob{
		ImportJob: &datapb.ImportJob{
			JobID: 0,
		},
	}
	err = imeta.AddJob(job)
	assert.NoError(t, err)

	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  0,
			TaskID: 1,
			State:  internalpb.ImportState_Pending,
		},
	}
	err = imeta.AddTask(pit1)
	assert.NoError(t, err)

	pit2 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  0,
			TaskID: 2,
			State:  internalpb.ImportState_Completed,
		},
	}
	err = imeta.AddTask(pit2)
	assert.NoError(t, err)

	pit3 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			JobID:  0,
			TaskID: 3,
			State:  internalpb.ImportState_Completed,
		},
	}
	err = imeta.AddTask(pit3)
	assert.NoError(t, err)

	it1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:  0,
			TaskID: 4,
			State:  internalpb.ImportState_Completed,
		},
	}
	err = imeta.AddTask(it1)
	assert.NoError(t, err)

	it2 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			JobID:  0,
			TaskID: 5,
			State:  internalpb.ImportState_Completed,
		},
	}
	err = imeta.AddTask(it2)
	assert.NoError(t, err)
	tasks := imeta.GetTaskBy(WithJob(0))
	assert.Equal(t, 5, len(tasks))

	meta, err := newMeta(context.TODO(), catalog, nil)
	assert.Nil(t, err)
	checker := NewImportChecker(meta, nil, cluster, alloc, nil, imeta, make(chan UniqueID, 1024)).(*importChecker)

	// preimport tasks are not fully completed
	checker.checkLackPreImport(job)
	tasks = imeta.GetTaskBy(WithJob(0))
	assert.Equal(t, 5, len(tasks))

	// preimport tasks are all completed, should generate import tasks
	err = imeta.UpdateTask(1, UpdateState(internalpb.ImportState_Completed))
	assert.NoError(t, err)
	checker.checkLackPreImport(job)
	tasks = imeta.GetTaskBy(WithJob(0))
	assert.Equal(t, 6, len(tasks))
	tasks = imeta.GetTaskBy(WithJob(0), WithType(ImportTaskType))
	assert.Equal(t, 3, len(tasks))
	for _, task := range tasks {
		assert.Equal(t, internalpb.ImportState_Pending, task.GetState())
	}

	// import tasks are not fully completed
	tasks = imeta.GetTaskBy(WithJob(0), WithType(ImportTaskType))
	assert.Equal(t, 3, len(tasks))
	checker.checkImportState(job)
	for _, task := range tasks {
		assert.Equal(t, 0, len(task.(*importTask).GetSegmentIDs()))
	}

	// import tasks are all completed
	tasks = imeta.GetTaskBy(WithJob(0), WithType(ImportTaskType))
	assert.Equal(t, 3, len(tasks))
	for _, task := range tasks {
		err = imeta.UpdateTask(task.GetTaskID(), UpdateState(internalpb.ImportState_Completed))
		assert.NoError(t, err)
		segmentID := task.GetTaskID()
		err = meta.AddSegment(context.TODO(), &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:          segmentID,
				IsImporting: true,
			},
		})
		assert.NoError(t, err)
	}
	checker.checkImportState(job)
	tasks = imeta.GetTaskBy(WithJob(0), WithType(ImportTaskType))
	assert.Equal(t, 3, len(tasks))
	for _, task := range tasks {
		for _, segmentID := range task.(*importTask).GetSegmentIDs() {
			segment := meta.GetSegment(segmentID)
			assert.False(t, segment.GetIsImporting())
		}
	}
}
