// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package broker

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type BrokerSuite struct {
	suite.Suite

	mixCoord *mocks.MixCoord
	broker   *coordinatorBroker
}

func (s *BrokerSuite) SetupSuite() {
	paramtable.Init()
}

func (s *BrokerSuite) SetupTest() {
	s.mixCoord = mocks.NewMixCoord(s.T())
	s.broker = NewCoordinatorBroker(s.mixCoord)
}

func (s *BrokerSuite) TearDownTest() {
	if s.mixCoord != nil {
		s.mixCoord.AssertExpectations(s.T())
	}
	s.mixCoord = nil
}

func (s *BrokerSuite) TestDescribeCollectionInternal() {
	s.Run("return_success", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.DescribeCollectionResponse{
				Status:         merr.Status(nil),
				CollectionID:   collID,
				CollectionName: "test_collection",
			}, nil
		})

		resp, err := s.broker.DescribeCollectionInternal(context.Background(), collID)
		s.NoError(err)
		s.Equal(collID, resp.GetCollectionID())
		s.Equal("test_collection", resp.GetCollectionName())

		s.TearDownTest()
	})

	s.Run("return_error", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return nil, errors.New("mocked")
		})

		_, err := s.broker.DescribeCollectionInternal(context.Background(), collID)
		s.Error(err)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestShowPartitionsInternal() {
	s.Run("return_success", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.ShowPartitionsResponse{
				Status:            merr.Status(nil),
				PartitionIDs:      []int64{1, 2, 3},
				PartitionNames:    []string{"_default_1", "_default_2", "_default_3"},
				CreatedTimestamps: []uint64{0, 0, 0},
			}, nil
		})

		resp, err := s.broker.ShowPartitionsInternal(context.Background(), collID)
		s.NoError(err)
		s.ElementsMatch([]int64{1, 2, 3}, resp)

		s.TearDownTest()
	})

	s.Run("return_error", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return nil, errors.New("mocked")
		})

		_, err := s.broker.ShowPartitionsInternal(context.Background(), collID)
		s.Error(err)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestShowCollections() {
	s.Run("return_success", func() {
		s.SetupTest()

		dbName := fmt.Sprintf("db_%d", rand.Intn(100))

		s.mixCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
			s.Equal(dbName, req.GetDbName())
			return &milvuspb.ShowCollectionsResponse{
				Status:            merr.Status(nil),
				CollectionIds:     []int64{1, 2, 3},
				CollectionNames:   []string{"coll_1", "coll_2", "coll_3"},
				CreatedTimestamps: []uint64{0, 0, 0},
			}, nil
		})

		resp, err := s.broker.ShowCollections(context.Background(), dbName)
		s.NoError(err)
		s.ElementsMatch([]int64{1, 2, 3}, resp.GetCollectionIds())
		s.ElementsMatch([]string{"coll_1", "coll_2", "coll_3"}, resp.GetCollectionNames())

		s.TearDownTest()
	})

	s.Run("return_error", func() {
		s.SetupTest()

		dbName := fmt.Sprintf("db_%d", rand.Intn(100))

		s.mixCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
			s.Equal(dbName, req.GetDbName())

			return nil, errors.New("mocked")
		})

		_, err := s.broker.ShowCollections(context.Background(), dbName)
		s.Error(err)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestListDatabases() {
	s.Run("return_success", func() {
		s.SetupTest()

		s.mixCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
			return &milvuspb.ListDatabasesResponse{
				Status:  merr.Status(nil),
				DbNames: []string{"db_1", "db_2", "db_3"},
			}, nil
		})

		resp, err := s.broker.ListDatabases(context.Background())
		s.NoError(err)
		s.ElementsMatch([]string{"db_1", "db_2", "db_3"}, resp.GetDbNames())

		s.TearDownTest()
	})

	s.Run("return_error", func() {
		s.SetupTest()

		s.mixCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
			return nil, errors.New("mocked")
		})

		_, err := s.broker.ListDatabases(context.Background())
		s.Error(err)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestHasCollection() {
	s.Run("return_success", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.DescribeCollectionResponse{
				Status:         merr.Status(nil),
				CollectionID:   collID,
				CollectionName: "test_collection",
			}, nil
		})

		result, err := s.broker.HasCollection(context.Background(), collID)
		s.NoError(err)
		s.True(result)

		s.TearDownTest()
	})

	s.Run("return_collection_not_found", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.DescribeCollectionResponse{
				Status: merr.Status(merr.WrapErrCollectionNotFound(collID)),
			}, nil
		})

		result, err := s.broker.HasCollection(context.Background(), collID)
		s.NoError(err)
		s.False(result)

		s.TearDownTest()
	})

	s.Run("return_error", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return nil, errors.New("mocked")
		})

		_, err := s.broker.HasCollection(context.Background(), collID)
		s.Error(err)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestShowCollectionIDs() {
	s.Run("normal", func() {
		s.SetupTest()
		dbName := "test_db"
		expectedIDs := []int64{1, 2, 3}

		s.mixCoord.EXPECT().ShowCollectionIDs(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *rootcoordpb.ShowCollectionIDsRequest) (*rootcoordpb.ShowCollectionIDsResponse, error) {
			s.Equal([]string{dbName}, req.GetDbNames())
			return &rootcoordpb.ShowCollectionIDsResponse{
				Status: merr.Success(),
				DbCollections: []*rootcoordpb.DBCollections{
					{
						DbName:        dbName,
						CollectionIDs: expectedIDs,
					},
				},
			}, nil
		})

		resp, err := s.broker.ShowCollectionIDs(context.Background(), dbName)
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetDbCollections(), 1)
		s.Equal(dbName, resp.GetDbCollections()[0].GetDbName())
		s.ElementsMatch(expectedIDs, resp.GetDbCollections()[0].GetCollectionIDs())

		s.TearDownTest()
	})

	s.Run("rpc_error", func() {
		s.SetupTest()
		dbName := "test_db"
		expectedErr := errors.New("mock rpc error")

		s.mixCoord.EXPECT().ShowCollectionIDs(mock.Anything, mock.Anything).Return(nil, expectedErr)

		resp, err := s.broker.ShowCollectionIDs(context.Background(), dbName)
		s.Error(err)
		s.Equal(expectedErr, err)
		s.Nil(resp)

		s.TearDownTest()
	})

	s.Run("milvus_error", func() {
		s.SetupTest()
		dbName := "test_db"
		expectedErr := merr.ErrDatabaseNotFound
		s.mixCoord.EXPECT().ShowCollectionIDs(mock.Anything, mock.Anything).Return(&rootcoordpb.ShowCollectionIDsResponse{
			Status: merr.Status(expectedErr),
		}, nil)

		resp, err := s.broker.ShowCollectionIDs(context.Background(), dbName)
		s.Error(err)
		s.ErrorIs(err, expectedErr)
		s.Nil(resp)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestShowPartitions() {
	s.Run("return_success", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))
		expectedPartitionIDs := []int64{100, 101, 102}
		expectedPartitionNames := []string{"partition_0", "partition_1", "partition_2"}

		s.mixCoord.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.ShowPartitionsResponse{
				Status:            merr.Status(nil),
				PartitionIDs:      expectedPartitionIDs,
				PartitionNames:    expectedPartitionNames,
				CreatedTimestamps: []uint64{1000, 2000, 3000},
			}, nil
		})

		resp, err := s.broker.ShowPartitions(context.Background(), collID)
		s.NoError(err)
		s.NotNil(resp)
		s.ElementsMatch(expectedPartitionIDs, resp.GetPartitionIDs())
		s.ElementsMatch(expectedPartitionNames, resp.GetPartitionNames())
		s.Len(resp.GetCreatedTimestamps(), 3)

		s.TearDownTest()
	})

	s.Run("return_status_error", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.ShowPartitionsResponse{
				Status: merr.Status(merr.WrapErrCollectionNotFound(collID)),
			}, nil
		})

		resp, err := s.broker.ShowPartitions(context.Background(), collID)
		s.Error(err)
		s.Nil(resp)

		s.TearDownTest()
	})

	s.Run("return_rpc_error", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

		resp, err := s.broker.ShowPartitions(context.Background(), collID)
		s.Error(err)
		s.Nil(resp)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestDescribeCollectionInternal_StatusError() {
	s.Run("status_error", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.DescribeCollectionResponse{
				Status: merr.Status(merr.WrapErrCollectionNotFound(collID)),
			}, nil
		})

		resp, err := s.broker.DescribeCollectionInternal(context.Background(), collID)
		s.Error(err)
		s.Nil(resp)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestShowCollections_StatusError() {
	s.Run("status_error", func() {
		s.SetupTest()

		dbName := fmt.Sprintf("db_%d", rand.Intn(100))

		s.mixCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
			s.Equal(dbName, req.GetDbName())
			return &milvuspb.ShowCollectionsResponse{
				Status: merr.Status(merr.ErrDatabaseNotFound),
			}, nil
		})

		resp, err := s.broker.ShowCollections(context.Background(), dbName)
		s.Error(err)
		s.Nil(resp)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestListDatabases_StatusError() {
	s.Run("status_error", func() {
		s.SetupTest()

		s.mixCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
			return &milvuspb.ListDatabasesResponse{
				Status: merr.Status(errors.New("internal error")),
			}, nil
		})

		resp, err := s.broker.ListDatabases(context.Background())
		s.Error(err)
		s.Nil(resp)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestShowCollectionIDs_MultipleDatabases() {
	s.Run("multiple_databases", func() {
		s.SetupTest()
		dbNames := []string{"db_1", "db_2", "db_3"}

		s.mixCoord.EXPECT().ShowCollectionIDs(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *rootcoordpb.ShowCollectionIDsRequest) (*rootcoordpb.ShowCollectionIDsResponse, error) {
			s.ElementsMatch(dbNames, req.GetDbNames())
			s.True(req.GetAllowUnavailable())
			return &rootcoordpb.ShowCollectionIDsResponse{
				Status: merr.Success(),
				DbCollections: []*rootcoordpb.DBCollections{
					{
						DbName:        "db_1",
						CollectionIDs: []int64{1, 2},
					},
					{
						DbName:        "db_2",
						CollectionIDs: []int64{3, 4, 5},
					},
					{
						DbName:        "db_3",
						CollectionIDs: []int64{6},
					},
				},
			}, nil
		})

		resp, err := s.broker.ShowCollectionIDs(context.Background(), dbNames...)
		s.NoError(err)
		s.NotNil(resp)
		s.Len(resp.GetDbCollections(), 3)

		// Verify each database's collections
		dbCollectionMap := make(map[string][]int64)
		for _, dbColl := range resp.GetDbCollections() {
			dbCollectionMap[dbColl.GetDbName()] = dbColl.GetCollectionIDs()
		}

		s.ElementsMatch([]int64{1, 2}, dbCollectionMap["db_1"])
		s.ElementsMatch([]int64{3, 4, 5}, dbCollectionMap["db_2"])
		s.ElementsMatch([]int64{6}, dbCollectionMap["db_3"])

		s.TearDownTest()
	})

	s.Run("empty_databases", func() {
		s.SetupTest()

		s.mixCoord.EXPECT().ShowCollectionIDs(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *rootcoordpb.ShowCollectionIDsRequest) (*rootcoordpb.ShowCollectionIDsResponse, error) {
			s.Empty(req.GetDbNames())
			return &rootcoordpb.ShowCollectionIDsResponse{
				Status:        merr.Success(),
				DbCollections: []*rootcoordpb.DBCollections{},
			}, nil
		})

		resp, err := s.broker.ShowCollectionIDs(context.Background())
		s.NoError(err)
		s.NotNil(resp)
		s.Empty(resp.GetDbCollections())

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestHasCollection_OtherError() {
	s.Run("other_error", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.DescribeCollectionResponse{
				Status: merr.Status(errors.New("internal error")),
			}, nil
		})

		result, err := s.broker.HasCollection(context.Background(), collID)
		s.Error(err)
		s.False(result)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestShowPartitionsInternal_StatusError() {
	s.Run("status_error", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.ShowPartitionsResponse{
				Status: merr.Status(merr.WrapErrCollectionNotFound(collID)),
			}, nil
		})

		resp, err := s.broker.ShowPartitionsInternal(context.Background(), collID)
		s.Error(err)
		s.Nil(resp)

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestNewCoordinatorBroker() {
	s.Run("create_broker", func() {
		mockMixCoord := mocks.NewMixCoord(s.T())
		broker := NewCoordinatorBroker(mockMixCoord)
		s.NotNil(broker)
		s.Equal(mockMixCoord, broker.mixCoord)
	})
}

func (s *BrokerSuite) TestShowCollections_EmptyResult() {
	s.Run("empty_collections", func() {
		s.SetupTest()

		dbName := "empty_db"

		s.mixCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error) {
			s.Equal(dbName, req.GetDbName())
			return &milvuspb.ShowCollectionsResponse{
				Status:            merr.Status(nil),
				CollectionIds:     []int64{},
				CollectionNames:   []string{},
				CreatedTimestamps: []uint64{},
			}, nil
		})

		resp, err := s.broker.ShowCollections(context.Background(), dbName)
		s.NoError(err)
		s.NotNil(resp)
		s.Empty(resp.GetCollectionIds())
		s.Empty(resp.GetCollectionNames())

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestListDatabases_EmptyResult() {
	s.Run("empty_databases", func() {
		s.SetupTest()

		s.mixCoord.EXPECT().ListDatabases(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error) {
			return &milvuspb.ListDatabasesResponse{
				Status:  merr.Status(nil),
				DbNames: []string{},
			}, nil
		})

		resp, err := s.broker.ListDatabases(context.Background())
		s.NoError(err)
		s.NotNil(resp)
		s.Empty(resp.GetDbNames())

		s.TearDownTest()
	})
}

func (s *BrokerSuite) TestShowPartitions_EmptyResult() {
	s.Run("empty_partitions", func() {
		s.SetupTest()

		collID := int64(1000 + rand.Intn(500))

		s.mixCoord.EXPECT().ShowPartitionsInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
			s.Equal(collID, req.GetCollectionID())
			return &milvuspb.ShowPartitionsResponse{
				Status:            merr.Status(nil),
				PartitionIDs:      []int64{},
				PartitionNames:    []string{},
				CreatedTimestamps: []uint64{},
			}, nil
		})

		resp, err := s.broker.ShowPartitions(context.Background(), collID)
		s.NoError(err)
		s.NotNil(resp)
		s.Empty(resp.GetPartitionIDs())
		s.Empty(resp.GetPartitionNames())

		s.TearDownTest()
	})
}

func TestBrokerSuite(t *testing.T) {
	suite.Run(t, new(BrokerSuite))
}

// --- Tests using mockey for DropCollection, CreateCollection, CreatePartition ---

func TestDropCollection_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DropCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DropCollectionRequest) (*commonpb.Status, error) {
			assert.Equal(t, "test_db", req.GetDbName())
			assert.Equal(t, "test_collection", req.GetCollectionName())
			assert.NotNil(t, req.GetBase())
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.DropCollection(ctx, "test_db", "test_collection")

	assert.NoError(t, err)
}

func TestDropCollection_RPCError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.DropCollection(ctx, "test_db", "test_collection")

	assert.Error(t, err)
}

func TestDropCollection_StatusError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DropCollection(mock.Anything, mock.Anything).Return(
		merr.Status(merr.WrapErrCollectionNotFound("test_collection")), nil)

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.DropCollection(ctx, "test_db", "test_collection")

	assert.Error(t, err)
}

func TestCreateCollection_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
			assert.Equal(t, "test_db", req.GetDbName())
			assert.Equal(t, "test_collection", req.GetCollectionName())
			assert.NotNil(t, req.GetBase())
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.NoError(t, err)
}

func TestCreateCollection_RPCError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.Error(t, err)
}

func TestCreateCollection_StatusError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).Return(
		merr.Status(errors.New("collection already exists")), nil)

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.Error(t, err)
}

func TestCreateCollection_WithExistingBase(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreateCollection(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.CreateCollectionRequest) (*commonpb.Status, error) {
			// Base should be preserved if already set
			assert.NotNil(t, req.GetBase())
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
		DbName:         "test_db",
		CollectionName: "test_collection",
	})

	assert.NoError(t, err)
}

func TestCreatePartition_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreatePartition(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
			assert.Equal(t, "test_db", req.GetDbName())
			assert.Equal(t, "test_collection", req.GetCollectionName())
			assert.Equal(t, "test_partition", req.GetPartitionName())
			assert.NotNil(t, req.GetBase())
			return merr.Success(), nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		PartitionName:  "test_partition",
	})

	assert.NoError(t, err)
}

func TestCreatePartition_RPCError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		PartitionName:  "test_partition",
	})

	assert.Error(t, err)
}

func TestCreatePartition_StatusError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().CreatePartition(mock.Anything, mock.Anything).Return(
		merr.Status(errors.New("partition already exists")), nil)

	broker := NewCoordinatorBroker(mockMixCoord)
	err := broker.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{
		DbName:         "test_db",
		CollectionName: "test_collection",
		PartitionName:  "test_partition",
	})

	assert.Error(t, err)
}

func TestDescribeCollectionByName_Success(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest) (*milvuspb.DescribeCollectionResponse, error) {
			assert.Equal(t, "test_db", req.GetDbName())
			assert.Equal(t, "test_collection", req.GetCollectionName())
			return &milvuspb.DescribeCollectionResponse{
				Status:         merr.Success(),
				CollectionID:   1001,
				CollectionName: "test_collection",
				DbName:         "test_db",
			}, nil
		})

	broker := NewCoordinatorBroker(mockMixCoord)
	resp, err := broker.DescribeCollectionByName(ctx, "test_db", "test_collection")

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, int64(1001), resp.GetCollectionID())
	assert.Equal(t, "test_collection", resp.GetCollectionName())
}

func TestDescribeCollectionByName_RPCError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(nil, errors.New("rpc error"))

	broker := NewCoordinatorBroker(mockMixCoord)
	resp, err := broker.DescribeCollectionByName(ctx, "test_db", "test_collection")

	assert.Error(t, err)
	assert.Nil(t, resp)
}

func TestDescribeCollectionByName_StatusError(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()

	mockMixCoord := mocks.NewMixCoord(t)
	mockMixCoord.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).Return(
		&milvuspb.DescribeCollectionResponse{
			Status: merr.Status(merr.WrapErrCollectionNotFound("test_collection")),
		}, nil)

	broker := NewCoordinatorBroker(mockMixCoord)
	resp, err := broker.DescribeCollectionByName(ctx, "test_db", "test_collection")

	assert.Error(t, err)
	assert.Nil(t, resp)
}
