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

package wrappers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type QnWrapperSuite struct {
	suite.Suite

	qn     *mocks.MockQueryNode
	client types.QueryNodeClient
}

func (s *QnWrapperSuite) SetupTest() {
	s.qn = mocks.NewMockQueryNode(s.T())
	s.client = WrapQueryNodeServerAsClient(s.qn)
}

func (s *QnWrapperSuite) TearDownTest() {
	s.client = nil
	s.qn = nil
}

func (s *QnWrapperSuite) TestGetComponentStates() {
	s.qn.EXPECT().GetComponentStates(mock.Anything, mock.Anything).
		Return(&milvuspb.ComponentStates{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetComponentStates(context.Background(), &milvuspb.GetComponentStatesRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestGetTimeTickChannel() {
	s.qn.EXPECT().GetTimeTickChannel(mock.Anything, mock.Anything).
		Return(&milvuspb.StringResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetTimeTickChannel(context.Background(), &internalpb.GetTimeTickChannelRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestGetStatisticsChannel() {
	s.qn.EXPECT().GetStatisticsChannel(mock.Anything, mock.Anything).
		Return(&milvuspb.StringResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetStatisticsChannel(context.Background(), &internalpb.GetStatisticsChannelRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestWatchDmChannels() {
	s.qn.EXPECT().WatchDmChannels(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.WatchDmChannels(context.Background(), &querypb.WatchDmChannelsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestUnsubDmChannel() {
	s.qn.EXPECT().UnsubDmChannel(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.UnsubDmChannel(context.Background(), &querypb.UnsubDmChannelRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestLoadSegments() {
	s.qn.EXPECT().LoadSegments(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.LoadSegments(context.Background(), &querypb.LoadSegmentsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestReleaseCollection() {
	s.qn.EXPECT().ReleaseCollection(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.ReleaseCollection(context.Background(), &querypb.ReleaseCollectionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestLoadPartitions() {
	s.qn.EXPECT().LoadPartitions(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.LoadPartitions(context.Background(), &querypb.LoadPartitionsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestReleasePartitions() {
	s.qn.EXPECT().ReleasePartitions(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.ReleasePartitions(context.Background(), &querypb.ReleasePartitionsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestReleaseSegments() {
	s.qn.EXPECT().ReleaseSegments(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.ReleaseSegments(context.Background(), &querypb.ReleaseSegmentsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestGetSegmentInfo() {
	s.qn.EXPECT().GetSegmentInfo(mock.Anything, mock.Anything).
		Return(&querypb.GetSegmentInfoResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetSegmentInfo(context.Background(), &querypb.GetSegmentInfoRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestSyncReplicaSegments() {
	s.qn.EXPECT().SyncReplicaSegments(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.SyncReplicaSegments(context.Background(), &querypb.SyncReplicaSegmentsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestGetStatistics() {
	s.qn.EXPECT().GetStatistics(mock.Anything, mock.Anything).
		Return(&internalpb.GetStatisticsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetStatistics(context.Background(), &querypb.GetStatisticsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestSearch() {
	s.qn.EXPECT().Search(mock.Anything, mock.Anything).
		Return(&internalpb.SearchResults{Status: merr.Status(nil)}, nil)

	resp, err := s.client.Search(context.Background(), &querypb.SearchRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestSearchSegments() {
	s.qn.EXPECT().SearchSegments(mock.Anything, mock.Anything).
		Return(&internalpb.SearchResults{Status: merr.Status(nil)}, nil)

	resp, err := s.client.SearchSegments(context.Background(), &querypb.SearchRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestQuery() {
	s.qn.EXPECT().Query(mock.Anything, mock.Anything).
		Return(&internalpb.RetrieveResults{Status: merr.Status(nil)}, nil)

	resp, err := s.client.Query(context.Background(), &querypb.QueryRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestQuerySegments() {
	s.qn.EXPECT().QuerySegments(mock.Anything, mock.Anything).
		Return(&internalpb.RetrieveResults{Status: merr.Status(nil)}, nil)

	resp, err := s.client.QuerySegments(context.Background(), &querypb.QueryRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestShowConfigurations() {
	s.qn.EXPECT().ShowConfigurations(mock.Anything, mock.Anything).
		Return(&internalpb.ShowConfigurationsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.ShowConfigurations(context.Background(), &internalpb.ShowConfigurationsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestGetMetrics() {
	s.qn.EXPECT().GetMetrics(mock.Anything, mock.Anything).
		Return(&milvuspb.GetMetricsResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetMetrics(context.Background(), &milvuspb.GetMetricsRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) GetDataDistribution() {
	s.qn.EXPECT().GetDataDistribution(mock.Anything, mock.Anything).
		Return(&querypb.GetDataDistributionResponse{Status: merr.Status(nil)}, nil)

	resp, err := s.client.GetDataDistribution(context.Background(), &querypb.GetDataDistributionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestSyncDistribution() {
	s.qn.EXPECT().SyncDistribution(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.SyncDistribution(context.Background(), &querypb.SyncDistributionRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestDelete() {
	s.qn.EXPECT().Delete(mock.Anything, mock.Anything).
		Return(merr.Status(nil), nil)

	resp, err := s.client.Delete(context.Background(), &querypb.DeleteRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

func (s *QnWrapperSuite) TestDeleteBatch() {
	s.qn.EXPECT().DeleteBatch(mock.Anything, mock.Anything).
		Return(&querypb.DeleteBatchResponse{
			Status: merr.Status(nil),
		}, nil)

	resp, err := s.client.DeleteBatch(context.Background(), &querypb.DeleteBatchRequest{})
	err = merr.CheckRPCCall(resp, err)
	s.NoError(err)
}

// Race caused by mock parameter check on once
/*
func (s *QnWrapperSuite) TestQueryStream() {
	s.qn.EXPECT().QueryStream(mock.Anything, mock.Anything).
		Run(func(_ *querypb.QueryRequest, server querypb.QueryNode_QueryStreamServer) {
			server.Send(&internalpb.RetrieveResults{})
		}).
		Return(nil)

	streamer, err := s.client.QueryStream(context.Background(), &querypb.QueryRequest{})
	s.NoError(err)
	inMemStreamer, ok := streamer.(*streamrpc.InMemoryStreamer[*internalpb.RetrieveResults])
	s.Require().True(ok)

	r, err := streamer.Recv()
	err = merr.CheckRPCCall(r, err)
	s.NoError(err)

	s.Eventually(func() bool {
		return inMemStreamer.IsClosed()
	}, time.Second, time.Millisecond*100)
}

func (s *QnWrapperSuite) TestQueryStreamSegments() {
	s.qn.EXPECT().QueryStreamSegments(mock.Anything, mock.Anything).
		Run(func(_ *querypb.QueryRequest, server querypb.QueryNode_QueryStreamSegmentsServer) {
			server.Send(&internalpb.RetrieveResults{})
		}).
		Return(nil)

	streamer, err := s.client.QueryStreamSegments(context.Background(), &querypb.QueryRequest{})
	s.NoError(err)
	inMemStreamer, ok := streamer.(*streamrpc.InMemoryStreamer[*internalpb.RetrieveResults])
	s.Require().True(ok)

	r, err := streamer.Recv()
	err = merr.CheckRPCCall(r, err)
	s.NoError(err)
	s.Eventually(func() bool {
		return inMemStreamer.IsClosed()
	}, time.Second, time.Millisecond*100)
}*/

func TestQnServerWrapper(t *testing.T) {
	suite.Run(t, new(QnWrapperSuite))
}
