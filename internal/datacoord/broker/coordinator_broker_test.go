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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
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

func TestBrokerSuite(t *testing.T) {
	suite.Run(t, new(BrokerSuite))
}
