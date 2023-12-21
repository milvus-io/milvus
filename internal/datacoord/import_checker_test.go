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
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func TestImportChecker(t *testing.T) {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListImportTasks().Return(nil, nil)
	catalog.EXPECT().SaveImportTask(mock.Anything).Return(nil)
	catalog.EXPECT().DropImportTask(mock.Anything).Return(nil)

	imeta, err := NewImportMeta(catalog)
	assert.NoError(t, err)

	pit1 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			RequestID: 0,
			TaskID:    1,
			State:     datapb.ImportState_Pending,
		},
	}
	err = imeta.Add(pit1)
	assert.NoError(t, err)

	pit2 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			RequestID: 0,
			TaskID:    2,
			State:     datapb.ImportState_Pending,
		},
	}
	err = imeta.Add(pit2)
	assert.NoError(t, err)

	pit3 := &preImportTask{
		PreImportTask: &datapb.PreImportTask{
			RequestID: 0,
			TaskID:    3,
			State:     datapb.ImportState_Pending,
		},
	}
	err = imeta.Add(pit3)
	assert.NoError(t, err)

	it1 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			RequestID: 0,
			TaskID:    4,
			State:     datapb.ImportState_Pending,
		},
	}
	err = imeta.Add(it1)
	assert.NoError(t, err)

	it2 := &importTask{
		ImportTaskV2: &datapb.ImportTaskV2{
			RequestID: 0,
			TaskID:    5,
			State:     datapb.ImportState_Pending,
		},
	}
	err = imeta.Add(it2)
	assert.NoError(t, err)

	tasks := imeta.GetBy(WithReq(0))
	assert.Equal(t, 5, len(tasks))

	meta, err := newMeta(context.TODO(), catalog, nil)
	assert.Nil(t, err)
	cluster := NewMockCluster(t)
	alloc := NewNMockAllocator(t)
	checker := NewImportChecker(meta, cluster, alloc, imeta)
}
