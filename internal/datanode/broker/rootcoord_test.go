package broker

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type rootCoordSuite struct {
	suite.Suite

	rc     *mocks.MockRootCoordClient
	broker Broker
}

func (s *rootCoordSuite) SetupSuite() {
	paramtable.Init()
}

func (s *rootCoordSuite) SetupTest() {
	s.rc = mocks.NewMockRootCoordClient(s.T())
	s.broker = NewCoordBroker(s.rc, nil, 1)
}

func (s *rootCoordSuite) resetMock() {
	s.rc.AssertExpectations(s.T())
	s.rc.ExpectedCalls = nil
}

func (s *rootCoordSuite) TestDescribeCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionID := int64(100)
	timestamp := tsoutil.ComposeTSByTime(time.Now(), 0)

	s.Run("normal_case", func() {
		collName := "test_collection_name"

		s.rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) {
				s.Equal(collectionID, req.GetCollectionID())
				s.Equal(timestamp, req.GetTimeStamp())
			}).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:         merr.Status(nil),
				CollectionID:   collectionID,
				CollectionName: collName,
			}, nil)

		resp, err := s.broker.DescribeCollection(ctx, collectionID, timestamp)
		s.NoError(err)
		s.Equal(collectionID, resp.GetCollectionID())
		s.Equal(collName, resp.GetCollectionName())
		s.resetMock()
	})

	s.Run("rootcoord_return_error", func() {
		s.rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		_, err := s.broker.DescribeCollection(ctx, collectionID, timestamp)
		s.Error(err)
		s.resetMock()
	})

	s.Run("rootcoord_return_failure_status", func() {
		s.rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status: merr.Status(errors.New("mocked")),
			}, nil)

		_, err := s.broker.DescribeCollection(ctx, collectionID, timestamp)
		s.Error(err)
		s.resetMock()
	})
}

func (s *rootCoordSuite) TestShowPartitions() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbName := "defaultDB"
	collName := "testCollection"

	s.Run("normal_case", func() {
		partitions := map[string]int64{
			"part1": 1001,
			"part2": 1002,
			"part3": 1003,
		}

		names := lo.Keys(partitions)
		ids := lo.Map(names, func(name string, _ int) int64 {
			return partitions[name]
		})

		s.rc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *milvuspb.ShowPartitionsRequest, _ ...grpc.CallOption) {
				s.Equal(dbName, req.GetDbName())
				s.Equal(collName, req.GetCollectionName())
			}).
			Return(&milvuspb.ShowPartitionsResponse{
				Status:         merr.Status(nil),
				PartitionIDs:   ids,
				PartitionNames: names,
			}, nil)
		partNameIDs, err := s.broker.ShowPartitions(ctx, dbName, collName)
		s.NoError(err)
		s.Equal(len(partitions), len(partNameIDs))
		for name, id := range partitions {
			result, ok := partNameIDs[name]
			s.True(ok)
			s.Equal(id, result)
		}
		s.resetMock()
	})

	s.Run("rootcoord_return_error", func() {
		s.rc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		_, err := s.broker.ShowPartitions(ctx, dbName, collName)
		s.Error(err)
		s.resetMock()
	})

	s.Run("partition_id_name_not_match", func() {
		s.rc.EXPECT().ShowPartitions(mock.Anything, mock.Anything).
			Return(&milvuspb.ShowPartitionsResponse{
				Status:         merr.Status(nil),
				PartitionIDs:   []int64{1, 2},
				PartitionNames: []string{"part1"},
			}, nil)

		_, err := s.broker.ShowPartitions(ctx, dbName, collName)
		s.Error(err)
		s.resetMock()
	})
}

func (s *rootCoordSuite) TestAllocTimestamp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("normal_case", func() {
		num := rand.Intn(10) + 1
		ts := tsoutil.ComposeTSByTime(time.Now(), 0)
		s.rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *rootcoordpb.AllocTimestampRequest, _ ...grpc.CallOption) {
				s.EqualValues(num, req.GetCount())
			}).
			Return(&rootcoordpb.AllocTimestampResponse{
				Status:    merr.Status(nil),
				Timestamp: ts,
				Count:     uint32(num),
			}, nil)

		timestamp, cnt, err := s.broker.AllocTimestamp(ctx, uint32(num))
		s.NoError(err)
		s.Equal(ts, timestamp)
		s.EqualValues(num, cnt)
		s.resetMock()
	})

	s.Run("rootcoord_return_error", func() {
		s.rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))
		_, _, err := s.broker.AllocTimestamp(ctx, 1)
		s.Error(err)
		s.resetMock()
	})

	s.Run("rootcoord_return_failure_status", func() {
		s.rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).
			Return(&rootcoordpb.AllocTimestampResponse{Status: merr.Status(errors.New("mock"))}, nil)
		_, _, err := s.broker.AllocTimestamp(ctx, 1)
		s.Error(err)
		s.resetMock()
	})
}

func TestRootCoordBroker(t *testing.T) {
	suite.Run(t, new(rootCoordSuite))
}
