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

package meta

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type CoordinatorBrokerRootCoordSuite struct {
	suite.Suite

	mixcoord *mocks.MixCoord
	broker   *CoordinatorBroker
}

func (s *CoordinatorBrokerRootCoordSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CoordinatorBrokerRootCoordSuite) SetupTest() {
	s.mixcoord = mocks.NewMixCoord(s.T())
	s.broker = NewCoordinatorBroker(s.mixcoord)
}

func (s *CoordinatorBrokerRootCoordSuite) resetMock() {
	s.mixcoord.AssertExpectations(s.T())
	s.mixcoord.ExpectedCalls = nil
}

func (s *CoordinatorBrokerRootCoordSuite) TestGetCollectionSchema() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	collectionID := int64(100)

	s.Run("normal case", func() {
		s.mixcoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status:         merr.Success(),
				Schema:         &schemapb.CollectionSchema{Name: "test_schema"},
				CollectionName: "test_schema",
			}, nil)

		resp, err := s.broker.DescribeCollection(ctx, collectionID)
		s.NoError(err)
		s.Equal("test_schema", resp.GetCollectionName())
		s.resetMock()
	})

	s.Run("rootcoord_return_error", func() {
		s.mixcoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock error"))

		_, err := s.broker.DescribeCollection(ctx, collectionID)
		s.Error(err)
		s.resetMock()
	})

	s.Run("return_failure_status", func() {
		s.mixcoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).
			Return(&milvuspb.DescribeCollectionResponse{
				Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_CollectionNotExists},
			}, nil)

		_, err := s.broker.DescribeCollection(ctx, collectionID)
		s.Error(err)
		s.resetMock()
	})
}

func (s *CoordinatorBrokerRootCoordSuite) TestGetPartitions() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	collection := int64(100)
	partitions := []int64{10, 11, 12}

	s.Run("normal_case", func() {
		s.mixcoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status:       merr.Status(nil),
			PartitionIDs: partitions,
		}, nil)

		retPartitions, err := s.broker.GetPartitions(ctx, collection)
		s.NoError(err)
		s.ElementsMatch(partitions, retPartitions)
		s.resetMock()
	})

	s.Run("collection_not_exist", func() {
		s.mixcoord.EXPECT().ShowPartitions(mock.Anything, mock.Anything).Return(&milvuspb.ShowPartitionsResponse{
			Status: merr.Status(merr.WrapErrCollectionNotFound("mock")),
		}, nil)

		_, err := s.broker.GetPartitions(ctx, collection)
		s.Error(err)
		s.ErrorIs(err, merr.ErrCollectionNotFound)
		s.resetMock()
	})
}

type CoordinatorBrokerDataCoordSuite struct {
	suite.Suite

	mixcoord *mocks.MixCoord
	broker   *CoordinatorBroker
}

func (s *CoordinatorBrokerDataCoordSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CoordinatorBrokerDataCoordSuite) SetupTest() {
	s.mixcoord = mocks.NewMixCoord(s.T())
	s.broker = NewCoordinatorBroker(s.mixcoord)
}

func (s *CoordinatorBrokerDataCoordSuite) resetMock() {
	s.mixcoord.AssertExpectations(s.T())
	s.mixcoord.ExpectedCalls = nil
}

func (s *CoordinatorBrokerDataCoordSuite) TestGetRecoveryInfo() {
	collectionID := int64(100)
	partitionID := int64(1000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("normal_case", func() {
		channels := []string{"dml_0"}
		segmentIDs := []int64{1, 2, 3}
		s.mixcoord.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything).
			Return(&datapb.GetRecoveryInfoResponse{
				Channels: lo.Map(channels, func(ch string, _ int) *datapb.VchannelInfo {
					return &datapb.VchannelInfo{
						CollectionID: collectionID,
						ChannelName:  "dml_0",
					}
				}),
				Binlogs: lo.Map(segmentIDs, func(id int64, _ int) *datapb.SegmentBinlogs {
					return &datapb.SegmentBinlogs{SegmentID: id}
				}),
			}, nil)

		vchans, segInfos, err := s.broker.GetRecoveryInfo(ctx, collectionID, partitionID)
		s.NoError(err)
		s.ElementsMatch(channels, lo.Map(vchans, func(info *datapb.VchannelInfo, _ int) string {
			return info.GetChannelName()
		}))
		s.ElementsMatch(segmentIDs, lo.Map(segInfos, func(info *datapb.SegmentBinlogs, _ int) int64 {
			return info.GetSegmentID()
		}))
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.mixcoord.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		_, _, err := s.broker.GetRecoveryInfo(ctx, collectionID, partitionID)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.mixcoord.EXPECT().GetRecoveryInfo(mock.Anything, mock.Anything).
			Return(&datapb.GetRecoveryInfoResponse{
				Status: merr.Status(errors.New("mocked")),
			}, nil)

		_, _, err := s.broker.GetRecoveryInfo(ctx, collectionID, partitionID)
		s.Error(err)
		s.resetMock()
	})
}

func (s *CoordinatorBrokerDataCoordSuite) TestGetRecoveryInfoV2() {
	collectionID := int64(100)
	partitionID := int64(1000)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("normal_case", func() {
		channels := []string{"dml_0"}
		segmentIDs := []int64{1, 2, 3}
		s.mixcoord.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).
			Return(&datapb.GetRecoveryInfoResponseV2{
				Channels: lo.Map(channels, func(ch string, _ int) *datapb.VchannelInfo {
					return &datapb.VchannelInfo{
						CollectionID: collectionID,
						ChannelName:  "dml_0",
					}
				}),
				Segments: lo.Map(segmentIDs, func(id int64, _ int) *datapb.SegmentInfo {
					return &datapb.SegmentInfo{ID: id}
				}),
			}, nil)

		vchans, segInfos, err := s.broker.GetRecoveryInfoV2(ctx, collectionID, partitionID)
		s.NoError(err)
		s.ElementsMatch(channels, lo.Map(vchans, func(info *datapb.VchannelInfo, _ int) string {
			return info.GetChannelName()
		}))
		s.ElementsMatch(segmentIDs, lo.Map(segInfos, func(info *datapb.SegmentInfo, _ int) int64 {
			return info.GetID()
		}))
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.mixcoord.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		_, _, err := s.broker.GetRecoveryInfoV2(ctx, collectionID, partitionID)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.mixcoord.EXPECT().GetRecoveryInfoV2(mock.Anything, mock.Anything).
			Return(&datapb.GetRecoveryInfoResponseV2{
				Status: merr.Status(errors.New("mocked")),
			}, nil)

		_, _, err := s.broker.GetRecoveryInfoV2(ctx, collectionID, partitionID)
		s.Error(err)
		s.resetMock()
	})
}

func (s *CoordinatorBrokerDataCoordSuite) TestDescribeIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collectionID := int64(100)

	s.Run("normal_case", func() {
		indexIDs := []int64{1, 2}
		s.mixcoord.EXPECT().DescribeIndex(mock.Anything, mock.Anything).
			Return(&indexpb.DescribeIndexResponse{
				Status: merr.Status(nil),
				IndexInfos: lo.Map(indexIDs, func(id int64, _ int) *indexpb.IndexInfo {
					return &indexpb.IndexInfo{IndexID: id}
				}),
			}, nil)
		infos, err := s.broker.describeIndex(ctx, collectionID)
		s.NoError(err)
		s.ElementsMatch(indexIDs, lo.Map(infos, func(info *indexpb.IndexInfo, _ int) int64 { return info.GetIndexID() }))
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.mixcoord.EXPECT().DescribeIndex(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		_, err := s.broker.describeIndex(ctx, collectionID)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.mixcoord.EXPECT().DescribeIndex(mock.Anything, mock.Anything).
			Return(&indexpb.DescribeIndexResponse{
				Status: merr.Status(errors.New("mocked")),
			}, nil)

		_, err := s.broker.describeIndex(ctx, collectionID)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_unimplemented", func() {
		// mock old version datacoord return unimplemented
		s.mixcoord.EXPECT().DescribeIndex(mock.Anything, mock.Anything).
			Return(nil, merr.ErrServiceUnimplemented).Times(1)

		// mock retry on new version datacoord return success
		indexIDs := []int64{1, 2}
		s.mixcoord.EXPECT().DescribeIndex(mock.Anything, mock.Anything).
			Return(&indexpb.DescribeIndexResponse{
				Status: merr.Status(nil),
				IndexInfos: lo.Map(indexIDs, func(id int64, _ int) *indexpb.IndexInfo {
					return &indexpb.IndexInfo{IndexID: id}
				}),
			}, nil)

		_, err := s.broker.describeIndex(ctx, collectionID)
		s.NoError(err)
		s.resetMock()
	})
}

func (s *CoordinatorBrokerDataCoordSuite) TestListIndexes() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collectionID := int64(100)

	s.Run("normal_case", func() {
		indexIDs := []int64{1, 2}
		s.mixcoord.EXPECT().ListIndexes(mock.Anything, mock.Anything).
			Return(&indexpb.ListIndexesResponse{
				Status: merr.Status(nil),
				IndexInfos: lo.Map(indexIDs, func(id int64, _ int) *indexpb.IndexInfo {
					return &indexpb.IndexInfo{IndexID: id}
				}),
			}, nil).Once()
		infos, err := s.broker.ListIndexes(ctx, collectionID)
		s.NoError(err)
		s.ElementsMatch(indexIDs, lo.Map(infos, func(info *indexpb.IndexInfo, _ int) int64 { return info.GetIndexID() }))
	})

	s.Run("datacoord_return_error", func() {
		s.mixcoord.EXPECT().ListIndexes(mock.Anything, mock.Anything).
			Return(nil, errors.New("mocked")).Once()

		_, err := s.broker.ListIndexes(ctx, collectionID)
		s.Error(err)
	})

	s.Run("datacoord_return_failure_status", func() {
		s.mixcoord.EXPECT().ListIndexes(mock.Anything, mock.Anything).
			Return(&indexpb.ListIndexesResponse{
				Status: merr.Status(errors.New("mocked")),
			}, nil).Once()

		_, err := s.broker.ListIndexes(ctx, collectionID)
		s.Error(err)
	})

	s.Run("datacoord_return_unimplemented", func() {
		// mock old version datacoord return unimplemented
		s.mixcoord.EXPECT().ListIndexes(mock.Anything, mock.Anything).
			Return(nil, merr.ErrServiceUnimplemented).Once()

		// mock retry on old version datacoord descibe index
		indexIDs := []int64{1, 2}
		s.mixcoord.EXPECT().DescribeIndex(mock.Anything, mock.Anything).
			Return(&indexpb.DescribeIndexResponse{
				Status: merr.Status(nil),
				IndexInfos: lo.Map(indexIDs, func(id int64, _ int) *indexpb.IndexInfo {
					return &indexpb.IndexInfo{IndexID: id}
				}),
			}, nil).Once()

		_, err := s.broker.ListIndexes(ctx, collectionID)
		s.NoError(err)
	})
}

func (s *CoordinatorBrokerDataCoordSuite) TestSegmentInfo() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	collectionID := int64(100)
	segmentIDs := []int64{10000, 10001, 10002}

	s.Run("normal_case", func() {
		s.mixcoord.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).
			Return(&datapb.GetSegmentInfoResponse{
				Status: merr.Status(nil),
				Infos: lo.Map(segmentIDs, func(id int64, _ int) *datapb.SegmentInfo {
					return &datapb.SegmentInfo{ID: id, CollectionID: collectionID}
				}),
			}, nil)

		infos, err := s.broker.GetSegmentInfo(ctx, segmentIDs...)
		s.NoError(err)
		s.ElementsMatch(segmentIDs, lo.Map(infos, func(info *datapb.SegmentInfo, _ int) int64 {
			return info.GetID()
		}))
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.mixcoord.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		_, err := s.broker.GetSegmentInfo(ctx, segmentIDs...)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.mixcoord.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).
			Return(&datapb.GetSegmentInfoResponse{Status: merr.Status(errors.New("mocked"))}, nil)

		_, err := s.broker.GetSegmentInfo(ctx, segmentIDs...)
		s.Error(err)
		s.resetMock()
	})
}

func (s *CoordinatorBrokerDataCoordSuite) TestGetIndexInfo() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionID := int64(100)
	segmentID := int64(10000)

	s.Run("normal_case", func() {
		indexIDs := []int64{1, 2, 3}
		s.mixcoord.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).
			Return(&indexpb.GetIndexInfoResponse{
				Status: merr.Status(nil),
				SegmentInfo: map[int64]*indexpb.SegmentInfo{
					segmentID: {
						SegmentID: segmentID,
						IndexInfos: lo.Map(indexIDs, func(id int64, _ int) *indexpb.IndexFilePathInfo {
							return &indexpb.IndexFilePathInfo{IndexID: id}
						}),
					},
				},
			}, nil)

		infos, err := s.broker.GetIndexInfo(ctx, collectionID, segmentID)
		s.NoError(err)
		s.ElementsMatch(indexIDs, lo.Map(infos[segmentID], func(info *querypb.FieldIndexInfo, _ int) int64 {
			return info.GetIndexID()
		}))
		s.resetMock()
	})

	s.Run("datacoord_return_error", func() {
		s.mixcoord.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).
			Return(nil, errors.New("mock"))

		_, err := s.broker.GetIndexInfo(ctx, collectionID, segmentID)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_failure_status", func() {
		s.mixcoord.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).
			Return(&indexpb.GetIndexInfoResponse{Status: merr.Status(errors.New("mock"))}, nil)

		_, err := s.broker.GetIndexInfo(ctx, collectionID, segmentID)
		s.Error(err)
		s.resetMock()
	})

	s.Run("datacoord_return_unimplemented", func() {
		// mock old version datacoord return unimplemented
		s.mixcoord.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).
			Return(nil, merr.ErrServiceUnimplemented).Times(1)

		// mock retry on new version datacoord return success
		indexIDs := []int64{1, 2, 3}
		s.mixcoord.EXPECT().GetIndexInfos(mock.Anything, mock.Anything).
			Return(&indexpb.GetIndexInfoResponse{
				Status: merr.Status(nil),
				SegmentInfo: map[int64]*indexpb.SegmentInfo{
					segmentID: {
						SegmentID: segmentID,
						IndexInfos: lo.Map(indexIDs, func(id int64, _ int) *indexpb.IndexFilePathInfo {
							return &indexpb.IndexFilePathInfo{IndexID: id}
						}),
					},
				},
			}, nil)

		_, err := s.broker.GetIndexInfo(ctx, collectionID, segmentID)
		s.NoError(err)
		s.resetMock()
	})
}

func (s *CoordinatorBrokerRootCoordSuite) TestDescribeDatabase() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("normal_case", func() {
		s.mixcoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).
			Return(&rootcoordpb.DescribeDatabaseResponse{
				Status: merr.Success(),
			}, nil)
		_, err := s.broker.DescribeDatabase(ctx, "fake_db1")
		s.NoError(err)
		s.resetMock()
	})

	s.Run("rootcoord_return_error", func() {
		s.mixcoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(nil, errors.New("fake error"))
		_, err := s.broker.DescribeDatabase(ctx, "fake_db1")
		s.Error(err)
		s.resetMock()
	})

	s.Run("rootcoord_return_failure_status", func() {
		s.mixcoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).
			Return(&rootcoordpb.DescribeDatabaseResponse{
				Status: merr.Status(errors.New("fake error")),
			}, nil)
		_, err := s.broker.DescribeDatabase(ctx, "fake_db1")
		s.Error(err)
		s.resetMock()
	})

	s.Run("rootcoord_return_unimplemented", func() {
		s.mixcoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).Return(nil, merr.ErrServiceUnimplemented)
		_, err := s.broker.DescribeDatabase(ctx, "fake_db1")
		s.Error(err)
		s.resetMock()
	})
}

func (s *CoordinatorBrokerRootCoordSuite) TestGetCollectionLoadInfo() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("normal_case", func() {
		s.mixcoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			DbName: "fake_db1",
		}, nil)
		s.mixcoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).
			Return(&rootcoordpb.DescribeDatabaseResponse{
				Status: merr.Success(),
				Properties: []*commonpb.KeyValuePair{
					{
						Key:   common.DatabaseReplicaNumber,
						Value: "3",
					},
					{
						Key:   common.DatabaseResourceGroups,
						Value: strings.Join([]string{"rg1", "rg2"}, ","),
					},
				},
			}, nil)
		rgs, replicas, err := s.broker.GetCollectionLoadInfo(ctx, 1)
		s.NoError(err)
		s.Equal(int64(3), replicas)
		s.Contains(rgs, "rg1")
		s.Contains(rgs, "rg2")
		s.resetMock()
	})

	s.Run("props not set", func() {
		s.mixcoord.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
			DbName: "fake_db1",
		}, nil)
		s.mixcoord.EXPECT().DescribeDatabase(mock.Anything, mock.Anything).
			Return(&rootcoordpb.DescribeDatabaseResponse{
				Status:     merr.Success(),
				Properties: []*commonpb.KeyValuePair{},
			}, nil)
		_, _, err := s.broker.GetCollectionLoadInfo(ctx, 1)
		s.NoError(err)
		s.resetMock()
	})
}

func TestCoordinatorBroker(t *testing.T) {
	suite.Run(t, new(CoordinatorBrokerRootCoordSuite))
	suite.Run(t, new(CoordinatorBrokerDataCoordSuite))
}
