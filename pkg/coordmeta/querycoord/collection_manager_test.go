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

package querycoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// CollectionManagerPkgSuite exercises CollectionManager against a mock catalog,
// so the moved package is unit-testable inside pkg/v3 without etcd. The etcd
// integration coverage (load/recover round-trips) stays in
// internal/querycoordv2/meta.
type CollectionManagerPkgSuite struct {
	suite.Suite

	catalog *mocks.QueryCoordCatalog
	mgr     *CollectionManager
	ctx     context.Context
}

func (suite *CollectionManagerPkgSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *CollectionManagerPkgSuite) SetupTest() {
	suite.ctx = context.Background()
	suite.catalog = mocks.NewQueryCoordCatalog(suite.T())
	suite.mgr = NewCollectionManager(suite.catalog)
}

func (suite *CollectionManagerPkgSuite) TestPutGetRemoveCollection() {
	suite.catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)

	col := &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{
			CollectionID:  100,
			ReplicaNumber: 3,
			Status:        querypb.LoadStatus_Loaded,
			LoadType:      querypb.LoadType_LoadCollection,
		},
		LoadPercentage: 100,
	}
	suite.NoError(suite.mgr.PutCollection(suite.ctx, col))

	suite.True(suite.mgr.Exist(suite.ctx, 100))
	suite.Equal(int32(3), suite.mgr.GetReplicaNumber(suite.ctx, 100))
	suite.Equal(querypb.LoadType_LoadCollection, suite.mgr.GetLoadType(suite.ctx, 100))
	suite.NotNil(suite.mgr.GetCollection(suite.ctx, 100))
	suite.ElementsMatch([]int64{100}, suite.mgr.GetAll(suite.ctx))

	suite.catalog.EXPECT().ReleaseCollection(mock.Anything, int64(100)).Return(nil)
	suite.NoError(suite.mgr.RemoveCollection(suite.ctx, 100))
	suite.False(suite.mgr.Exist(suite.ctx, 100))
	suite.Nil(suite.mgr.GetCollection(suite.ctx, 100))
}

func (suite *CollectionManagerPkgSuite) TestPutPartitionRejectsOrphan() {
	// partition whose collection is not loaded must be rejected
	part := &Partition{PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: 200, PartitionID: 20}}
	err := suite.mgr.PutPartition(suite.ctx, part)
	suite.Error(err)
	suite.ErrorIs(err, merr.ErrCollectionNotLoaded)
}

func (suite *CollectionManagerPkgSuite) TestPutCollectionWithPartitionThenLoadPercentage() {
	suite.catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	suite.catalog.EXPECT().SavePartition(mock.Anything, mock.Anything).Return(nil)

	col := &Collection{
		CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 300, ReplicaNumber: 1, LoadType: querypb.LoadType_LoadPartition},
	}
	part := &Partition{PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: 300, PartitionID: 30}}
	suite.NoError(suite.mgr.PutCollection(suite.ctx, col, part))

	// drive partition to 100% -> collection load percentage rolls up to 100
	suite.NoError(suite.mgr.UpdatePartitionLoadPercent(suite.ctx, 30, 100))
	suite.Equal(int32(100), suite.mgr.CalculateLoadPercentage(suite.ctx, 300))
	suite.Equal(querypb.LoadStatus_Loaded, suite.mgr.CalculateLoadStatus(suite.ctx, 300))
}

func (suite *CollectionManagerPkgSuite) TestRecoverWithBrokerUpgradeLoadFields() {
	loadInfo := &querypb.CollectionLoadInfo{
		CollectionID:  400,
		ReplicaNumber: 1,
		Status:        querypb.LoadStatus_Loaded,
		LoadType:      querypb.LoadType_LoadCollection,
		// LoadFields nil -> triggers broker.DescribeCollection upgrade path
	}
	suite.catalog.EXPECT().GetCollections(mock.Anything).Return([]*querypb.CollectionLoadInfo{loadInfo}, nil)
	suite.catalog.EXPECT().GetPartitions(mock.Anything, mock.Anything).Return(map[int64][]*querypb.PartitionLoadInfo{}, nil)
	// upgradeLoadFields saves the schema-filled collection, Recover re-saves CreatedAt
	suite.catalog.EXPECT().SaveCollection(mock.Anything, mock.Anything).Return(nil)

	broker := NewMockBroker(suite.T())
	broker.EXPECT().DescribeCollection(mock.Anything, int64(400)).Return(&milvuspb.DescribeCollectionResponse{
		Status: merr.Success(),
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk"},
				{FieldID: 101, Name: "vec"},
			},
		},
	}, nil)

	suite.NoError(suite.mgr.Recover(suite.ctx, broker))
	suite.True(suite.mgr.Exist(suite.ctx, 400))
	suite.ElementsMatch([]int64{100, 101}, suite.mgr.GetLoadFields(suite.ctx, 400))
}

func TestCollectionManagerPkg(t *testing.T) {
	suite.Run(t, new(CollectionManagerPkgSuite))
}
