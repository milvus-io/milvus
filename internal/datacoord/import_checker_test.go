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

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
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

	imeta, err := NewImportMeta(catalog)
	assert.NoError(t, err)

	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			RequestID: 0,
			TaskID:    1,
			State:     milvuspb.ImportState_Pending,
		},
	}
	err = imeta.Add(pit1)
	assert.NoError(t, err)

	pit2 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			RequestID: 0,
			TaskID:    2,
			State:     milvuspb.ImportState_Completed,
		},
	}
	err = imeta.Add(pit2)
	assert.NoError(t, err)

	pit3 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			RequestID: 0,
			TaskID:    3,
			State:     milvuspb.ImportState_Completed,
		},
	}
	err = imeta.Add(pit3)
	assert.NoError(t, err)

	it1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			RequestID: 0,
			TaskID:    4,
			State:     milvuspb.ImportState_Completed,
		},
	}
	err = imeta.Add(it1)
	assert.NoError(t, err)

	it2 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			RequestID: 0,
			TaskID:    5,
			State:     milvuspb.ImportState_Completed,
		},
	}
	err = imeta.Add(it2)
	assert.NoError(t, err)
	tasks := imeta.GetBy(WithReq(0))
	assert.Equal(t, 5, len(tasks))

	meta, err := newMeta(context.TODO(), catalog, nil)
	assert.Nil(t, err)
	checker := NewImportChecker(meta, cluster, alloc, nil, imeta).(*importChecker) // TODO: dyh, fix sm

	// preimport tasks are not fully completed
	checker.checkPreImportState(0)
	tasks = imeta.GetBy(WithReq(0))
	assert.Equal(t, 5, len(tasks))

	// preimport tasks are all completed, should generate import tasks
	err = imeta.Update(1, UpdateState(milvuspb.ImportState_Completed))
	assert.NoError(t, err)
	checker.checkPreImportState(0)
	tasks = imeta.GetBy(WithReq(0))
	assert.Equal(t, 6, len(tasks))
	tasks = imeta.GetBy(WithReq(0), WithType(ImportTaskType))
	assert.Equal(t, 3, len(tasks))
	for _, task := range tasks {
		assert.Equal(t, milvuspb.ImportState_Pending, task.GetState())
	}

	// import tasks are not fully completed
	tasks = imeta.GetBy(WithReq(0), WithType(ImportTaskType))
	assert.Equal(t, 3, len(tasks))
	checker.checkImportState(0)
	for _, task := range tasks {
		assert.Equal(t, 0, len(task.(*importTask).GetSegmentIDs()))
	}

	// import tasks are all completed
	tasks = imeta.GetBy(WithReq(0), WithType(ImportTaskType))
	assert.Equal(t, 3, len(tasks))
	for _, task := range tasks {
		err = imeta.Update(task.GetTaskID(), UpdateState(milvuspb.ImportState_Completed))
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
	checker.checkImportState(0)
	tasks = imeta.GetBy(WithReq(0), WithType(ImportTaskType))
	assert.Equal(t, 3, len(tasks))
	for _, task := range tasks {
		for _, segmentID := range task.(*importTask).GetSegmentIDs() {
			segment := meta.GetSegment(segmentID)
			assert.False(t, segment.GetIsImporting())
		}
	}
}
