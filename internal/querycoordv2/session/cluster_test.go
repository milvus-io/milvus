package session

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querycoordv2/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
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
	suite.setupServers()
}

func (suite *ClusterTestSuite) TearDownSuite() {
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
		conn, err := grpc.Dial(lis.Addr().String(), grpc.WithBlock(), grpc.WithInsecure())
		suite.NoError(err)
		suite.NoError(conn.Close())
	}
}

func (suite *ClusterTestSuite) setupCluster() {
	suite.nodeManager = NewNodeManager()
	for i, lis := range suite.listeners {
		node := NewNodeInfo(int64(i), lis.Addr().String())
		suite.nodeManager.Add(node)
	}
	suite.cluster = NewCluster(suite.nodeManager)
}

func (suite *ClusterTestSuite) createTestServers() []querypb.QueryNodeServer {
	// create 2 mock servers with 1 always return error
	ret := make([]querypb.QueryNodeServer, 0, 2)
	ret = append(ret, suite.createDefaultMockServer())
	ret = append(ret, suite.createFailedMockServer())
	return ret
}

func (suite *ClusterTestSuite) createDefaultMockServer() querypb.QueryNodeServer {
	succStatus := &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}
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
	return svr
}

func (suite *ClusterTestSuite) TestLoadSegments() {
	ctx := context.TODO()
	status, err := suite.cluster.LoadSegments(ctx, 0, &querypb.LoadSegmentsRequest{
		Infos: []*querypb.SegmentLoadInfo{{}},
	})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, status)

	status, err = suite.cluster.LoadSegments(ctx, 1, &querypb.LoadSegmentsRequest{
		Infos: []*querypb.SegmentLoadInfo{{}},
	})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}, status)

	_, err = suite.cluster.LoadSegments(ctx, 3, &querypb.LoadSegmentsRequest{
		Infos: []*querypb.SegmentLoadInfo{{}},
	})
	suite.Error(err)
	suite.IsType(WrapErrNodeNotFound(3), err)
}

func (suite *ClusterTestSuite) TestWatchDmChannels() {
	ctx := context.TODO()
	status, err := suite.cluster.WatchDmChannels(ctx, 0, &querypb.WatchDmChannelsRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, status)

	status, err = suite.cluster.WatchDmChannels(ctx, 1, &querypb.WatchDmChannelsRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}, status)
}

func (suite *ClusterTestSuite) TestUnsubDmChannel() {
	ctx := context.TODO()
	status, err := suite.cluster.UnsubDmChannel(ctx, 0, &querypb.UnsubDmChannelRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, status)

	status, err = suite.cluster.UnsubDmChannel(ctx, 1, &querypb.UnsubDmChannelRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}, status)
}

func (suite *ClusterTestSuite) TestReleaseSegments() {
	ctx := context.TODO()
	status, err := suite.cluster.ReleaseSegments(ctx, 0, &querypb.ReleaseSegmentsRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, status)

	status, err = suite.cluster.ReleaseSegments(ctx, 1, &querypb.ReleaseSegmentsRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}, status)
}

func (suite *ClusterTestSuite) TestGetDataDistribution() {
	ctx := context.TODO()
	resp, err := suite.cluster.GetDataDistribution(ctx, 0, &querypb.GetDataDistributionRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, resp.GetStatus())

	resp, err = suite.cluster.GetDataDistribution(ctx, 1, &querypb.GetDataDistributionRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}, resp.GetStatus())
}

func (suite *ClusterTestSuite) TestGetMetrics() {
	ctx := context.TODO()
	resp, err := suite.cluster.GetMetrics(ctx, 0, &milvuspb.GetMetricsRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, resp.GetStatus())

	resp, err = suite.cluster.GetMetrics(ctx, 1, &milvuspb.GetMetricsRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}, resp.GetStatus())
}

func (suite *ClusterTestSuite) TestSyncDistribution() {
	ctx := context.TODO()
	status, err := suite.cluster.SyncDistribution(ctx, 0, &querypb.SyncDistributionRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
		Reason:    "",
	}, status)

	status, err = suite.cluster.SyncDistribution(ctx, 1, &querypb.SyncDistributionRequest{})
	suite.NoError(err)
	suite.Equal(&commonpb.Status{
		ErrorCode: commonpb.ErrorCode_UnexpectedError,
		Reason:    "unexpected error",
	}, status)
}

func TestClusterSuite(t *testing.T) {
	suite.Run(t, new(ClusterTestSuite))
}
