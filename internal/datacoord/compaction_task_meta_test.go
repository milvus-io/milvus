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
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
)

func TestCompactionTaskMetaSuite(t *testing.T) {
	suite.Run(t, new(CompactionTaskMetaSuite))
}

type CompactionTaskMetaSuite struct {
	suite.Suite
	catalog *mocks.DataCoordCatalog
	meta    *compactionTaskMeta
}

func (suite *CompactionTaskMetaSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(suite.T())
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil)
	suite.catalog = catalog
	meta, err := newCompactionTaskMeta(context.TODO(), catalog)
	suite.NoError(err)
	suite.meta = meta
}

func newTestCompactionTaskMeta(t *testing.T) *compactionTaskMeta {
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().ListCompactionTask(mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Maybe()
	meta, _ := newCompactionTaskMeta(context.TODO(), catalog)
	return meta
}

func (suite *CompactionTaskMetaSuite) TestGetCompactionTasksByCollection() {
	suite.meta.SaveCompactionTask(&datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
	})
	res := suite.meta.GetCompactionTasksByCollection(100)
	suite.Equal(1, len(res))
}

func (suite *CompactionTaskMetaSuite) TestGetCompactionTasksByCollectionAbnormal() {
	suite.meta.SaveCompactionTask(&datapb.CompactionTask{
		TriggerID:    1,
		PlanID:       10,
		CollectionID: 100,
	})
	suite.meta.SaveCompactionTask(&datapb.CompactionTask{
		TriggerID:    2,
		PlanID:       11,
		CollectionID: 101,
	})
	res := suite.meta.GetCompactionTasksByCollection(101)
	suite.Equal(1, len(res))
}
