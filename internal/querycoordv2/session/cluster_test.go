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

package session

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/querycoordv2/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const bufSize = 1024 * 1024

type ClusterTestSuite struct {
	suite.Suite
	svrs        []*grpc.Server
	listeners   []net.Listener
	cluster     *QueryCluster
	nodeManager *NodeManager
}

func (suite *ClusterTestSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save("grpc.client.maxMaxAttempts", "1")
	suite.setupServers()
}

func (suite *ClusterTestSuite) TearDownSuite() {
	paramtable.Get().Save("grpc.client.maxMaxAttempts", strconv.FormatInt(paramtable.DefaultMaxAttempts, 10))
	for _, svr := range suite.svrs {
		svr.GracefulStop()
	}
}

func (suite *ClusterTestSuite) SetupTest() {
	suite.setupCluster()
}

func (suite *ClusterTestSuite) TearDownTest() {
	suite.cluster.Stop()
}

func (suite *ClusterTestSuite) setupServers() {
	svrs := suite.createTestServers()
	for _, svr := range svrs {
		lis, err := net.Listen("tcp", ":0")
		suite.NoError(err)
		suite.listeners = append(suite.listeners, lis)
		s := grpc.NewServer()
		querypb.RegisterQueryNodeServer(s, svr)
		go func() {
			suite.Eventually(func() bool {
				return s.Serve(lis) == nil
			}, 10*time.Second, 100*time.Millisecond)
		}()
		suite.svrs = append(suite.svrs, s)
	}

	// check server ready to serve
	for _, lis := range suite.listeners {
		conn, err := grpc.Dial(lis.Addr().String(), grpc.WithBlock(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		suite.NoError(err)
		suite.NoError(conn.Close())
	}
}

func (suite *ClusterTestSuite) setupCluster() {
	suite.nodeManager = NewNodeManager()
	for i, lis := range suite.listeners {
		node := NewNodeInfo(ImmutableNodeInfo{
			NodeID:   int64(i),
			Address:  lis.Addr().String(),
			Hostname: "localhost",
		})
		suite.nodeManager.Add(node)
	}
	suite.cluster = NewCluster(suite.nodeManager, DefaultQueryNodeCreator)
}

func (suite *ClusterTestSuite) createTestServers() []querypb.QueryNodeServer {
	// create 2 mock servers with 1 always return error
	ret := make([]querypb.QueryNodeServer, 0, 2)
	ret = append(ret, suite.createDefaultMockServer())
	ret = append(ret, suite.createFailedMockServer())
	return ret
}

func (suite *ClusterTestSuite) createDefaultMockServer() querypb.QueryNodeServer {
	succStatus := merr.Success()
	svr := mocks.NewMockQueryNodeServer(suite.T())
	// TODO: register more mock methods
	svr.EXPECT().LoadSegments(
		mock.Anything,
		mock.AnythingOfType("*querypb.LoadSegmentsRequest"),
	).Maybe().Return(succStatus, nil)
	svr.EXPECT().WatchDmChannels(
		mock.Anything,
		mock.AnythingOfType("*querypb.WatchDmChannelsRequest"),
	).Maybe().Return(succStatus, nil)
	svr.EXPECT().UnsubDmChannel(
		mock.Anything,
		mock.AnythingOfType("*querypb.UnsubDmChannelRequest"),
	).Maybe().Return(succStatus, nil)
	svr.EXPECT().ReleaseSegments(
		mock.Anything,
		mock.AnythingOfType("*querypb.ReleaseSegmentsRequest"),
	).Maybe().Return(succStatus, nil)
	svr.EXPECT().LoadPartitions(
		mock.Anything,
		mock.AnythingOfType("*querypb.LoadPartitionsRequest"),
	).Maybe().Return(succStatus, nil)
	svr.EXPECT().ReleasePartitions(
		mock.Anything,
		mock.AnythingOfType("*querypb.ReleasePartitionsRequest"),
	).Maybe().Return(succStatus, nil)
	svr.EXPECT().GetDataDistribution(
		mock.Anything,
		mock.AnythingOfType("*querypb.GetDataDistributionRequest"),
	).Maybe().Return(&querypb.GetDataDistributionResponse{Status: succStatus}, nil)
	svr.EXPECT().GetMetrics(
		mock.Anything,
		mock.AnythingOfType("*milvuspb.GetMetricsRequest"),
	).Maybe().Return(&milvuspb.GetMetricsResponse{Status: succStatus}, nil)
	svr.EXPECT().SyncDistribution(
		mock.Anything,
		mock.AnythingOfType("*querypb.SyncDistributionRequest"),
	).Maybe().Return(succStatus, nil)
	svr.EXPECT().GetComponentStates(
		mock.Anything,
		mock.AnythingOfType("*milvuspb.GetComponentStatesRequest"),
	).Maybe().Return(&milvuspb.ComponentStates{
		State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy},
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)
	return svr
}

func (suite *ClusterTestSuite) createFailedMockServer() querypb.QueryNodeServer {
	failStatus := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}
	svr := mocks.NewMockQueryNodeServer(suite.T())
	// TODO: register more mock methods
	svr.EXPECT().LoadSegments(
		mock.Anything,
		mock.AnythingOfType("*querypb.LoadSegmentsRequest"),
	).Maybe().Return(failStatus, nil)
	svr.EXPECT().WatchDmChannels(
		mock.Anything,
		mock.AnythingOfType("*querypb.WatchDmChannelsRequest"),
	).Maybe().Return(failStatus, nil)
	svr.EXPECT().UnsubDmChannel(
		mock.Anything,
		mock.AnythingOfType("*querypb.UnsubDmChannelRequest"),
	).Maybe().Return(failStatus, nil)
	svr.EXPECT().ReleaseSegments(
		mock.Anything,
		mock.AnythingOfType("*querypb.ReleaseSegmentsRequest"),
	).Maybe().Return(failStatus, nil)
	svr.EXPECT().LoadPartitions(
		mock.Anything,
		mock.AnythingOfType("*querypb.LoadPartitionsRequest"),
	).Maybe().Return(failStatus, nil)
	svr.EXPECT().ReleasePartitions(
		mock.Anything,
		mock.AnythingOfType("*querypb.ReleasePartitionsRequest"),
	).Maybe().Return(failStatus, nil)
	svr.EXPECT().GetDataDistribution(
		mock.Anything,
		mock.AnythingOfType("*querypb.GetDataDistributionRequest"),
	).Maybe().Return(&querypb.GetDataDistributionResponse{Status: failStatus}, nil)
	svr.EXPECT().GetMetrics(
		mock.Anything,
		mock.AnythingOfType("*milvuspb.GetMetricsRequest"),
	).Maybe().Return(&milvuspb.GetMetricsResponse{Status: failStatus}, nil)
	svr.EXPECT().SyncDistribution(
		mock.Anything,
		mock.AnythingOfType("*querypb.SyncDistributionRequest"),
	).Maybe().Return(failStatus, nil)
	svr.EXPECT().GetComponentStates(
		mock.Anything,
		mock.AnythingOfType("*milvuspb.GetComponentStatesRequest"),
	).Maybe().Return(&milvuspb.ComponentStates{
		State:  &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Abnormal},
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)
	return svr
}

func (suite *ClusterTestSuite) TestLoadSegments() {
	ctx := context.TODO()
	status, err := suite.cluster.LoadSegments(ctx, 0, &querypb.LoadSegmentsRequest{
		Base:  &commonpb.MsgBase{},
		Infos: []*querypb.SegmentLoadInfo{{}},
	})

	suite.NoError(err)
	merr.Ok(status)

	status, err = suite.cluster.LoadSegments(ctx, 1, &querypb.LoadSegmentsRequest{
		Base:  &commonpb.MsgBase{},
		Infos: []*querypb.SegmentLoadInfo{{}},
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	suite.Equal("unexpected error", status.GetReason())

	_, err = suite.cluster.LoadSegments(ctx, 3, &querypb.LoadSegmentsRequest{
		Base:  &commonpb.MsgBase{},
		Infos: []*querypb.SegmentLoadInfo{{}},
	})
	suite.Error(err)
	suite.IsType(WrapErrNodeNotFound(3), err)
}

func (suite *ClusterTestSuite) TestWatchDmChannels() {
	ctx := context.TODO()
	status, err := suite.cluster.WatchDmChannels(ctx, 0, &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	merr.Ok(status)

	status, err = suite.cluster.WatchDmChannels(ctx, 1, &querypb.WatchDmChannelsRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	suite.Equal("unexpected error", status.GetReason())
}

func (suite *ClusterTestSuite) TestUnsubDmChannel() {
	ctx := context.TODO()
	status, err := suite.cluster.UnsubDmChannel(ctx, 0, &querypb.UnsubDmChannelRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	merr.Ok(status)

	status, err = suite.cluster.UnsubDmChannel(ctx, 1, &querypb.UnsubDmChannelRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	suite.Equal("unexpected error", status.GetReason())
}

func (suite *ClusterTestSuite) TestReleaseSegments() {
	ctx := context.TODO()
	status, err := suite.cluster.ReleaseSegments(ctx, 0, &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	merr.Ok(status)

	status, err = suite.cluster.ReleaseSegments(ctx, 1, &querypb.ReleaseSegmentsRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	suite.Equal("unexpected error", status.GetReason())
}

func (suite *ClusterTestSuite) TestLoadAndReleasePartitions() {
	ctx := context.TODO()
	status, err := suite.cluster.LoadPartitions(ctx, 0, &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	merr.Ok(status)

	status, err = suite.cluster.LoadPartitions(ctx, 1, &querypb.LoadPartitionsRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	suite.Equal("unexpected error", status.GetReason())

	status, err = suite.cluster.ReleasePartitions(ctx, 0, &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	merr.Ok(status)

	status, err = suite.cluster.ReleasePartitions(ctx, 1, &querypb.ReleasePartitionsRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	suite.Equal("unexpected error", status.GetReason())
}

func (suite *ClusterTestSuite) TestGetDataDistribution() {
	ctx := context.TODO()
	resp, err := suite.cluster.GetDataDistribution(ctx, 0, &querypb.GetDataDistributionRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	suite.Equal(merr.Success(), resp.GetStatus())

	resp, err = suite.cluster.GetDataDistribution(ctx, 1, &querypb.GetDataDistributionRequest{
		Base: &commonpb.MsgBase{},
	})

	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	suite.Equal("unexpected error", resp.GetStatus().GetReason())
}

func (suite *ClusterTestSuite) TestGetMetrics() {
	ctx := context.TODO()
	resp, err := suite.cluster.GetMetrics(ctx, 0, &milvuspb.GetMetricsRequest{})
	suite.NoError(err)
	suite.Equal(merr.Success(), resp.GetStatus())

	resp, err = suite.cluster.GetMetrics(ctx, 1, &milvuspb.GetMetricsRequest{})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, resp.GetStatus().GetErrorCode())
	suite.Equal("unexpected error", resp.GetStatus().GetReason())
}

func (suite *ClusterTestSuite) TestSyncDistribution() {
	ctx := context.TODO()
	status, err := suite.cluster.SyncDistribution(ctx, 0, &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	merr.Ok(status)

	status, err = suite.cluster.SyncDistribution(ctx, 1, &querypb.SyncDistributionRequest{
		Base: &commonpb.MsgBase{},
	})
	suite.NoError(err)
	suite.Equal(commonpb.ErrorCode_UnexpectedError, status.GetErrorCode())
	suite.Equal("unexpected error", status.GetReason())
}

func (suite *ClusterTestSuite) TestGetComponentStates() {
	ctx := context.TODO()
	status, err := suite.cluster.GetComponentStates(ctx, 0)
	suite.NoError(err)
	suite.Equal(status.State.GetStateCode(), commonpb.StateCode_Healthy)

	status, err = suite.cluster.GetComponentStates(ctx, 1)
	suite.NoError(err)
	suite.Equal(status.State.GetStateCode(), commonpb.StateCode_Abnormal)
}

func TestClusterSuite(t *testing.T) {
	suite.Run(t, new(ClusterTestSuite))
}
