package datacoord

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/datacoord/ddl"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

type splitRetirementPeer struct {
	milvuspb.UnimplementedMilvusServiceServer
	dc *Server
}

func (p *splitRetirementPeer) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return p.dc.GetMetrics(ctx, req)
}

func TestSplitRetirementAcrossCDCConnection(t *testing.T) {
	ctx := context.Background()
	remote := Params.CommonCfg.ClusterPrefix.GetValue()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	peer := &commonpb.MilvusCluster{ClusterId: remote, Pchannels: []string{"p1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: listener.Addr().String()}}
	cfg := &commonpb.ReplicateConfiguration{
		Clusters:             []*commonpb.MilvusCluster{{ClusterId: "primary", Pchannels: []string{"p0"}}, peer},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: "primary", TargetClusterId: remote}},
	}
	replicaCfg := proto.Clone(cfg).(*commonpb.ReplicateConfiguration)
	replicaCfg.Clusters[0], replicaCfg.Clusters[1] = replicaCfg.Clusters[1], replicaCfg.Clusters[0]
	replicaCfg.Clusters[0].ConnectionParam = nil
	assignment := &balancer.WatchChannelAssignmentsCallbackParam{ReplicateConfiguration: replicaCfg}
	b := &mock_balancer.MockBalancer{}
	balancerMock := mockey.Mock(balance.GetWithContext).Return(b, nil).Build()
	defer balancerMock.UnPatch()
	assignmentMock := mockey.Mock((*mock_balancer.MockBalancer).GetLatestChannelAssignment).Return(assignment, nil).Build()
	defer assignmentMock.UnPatch()
	store, _ := newSplitTestStore(t)
	server := grpc.NewServer()
	dc := &Server{splitStore: store, metricsRequest: metricsinfo.NewMetricsRequest()}
	dc.UpdateStateCode(commonpb.StateCode_Healthy)
	dc.registerMetricsRequest()
	milvuspb.RegisterMilvusServiceServer(server, &splitRetirementPeer{dc: dc})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	parent := &datapb.SplitShardTask{TaskId: 123456789012345678, CollectionId: 100, SourceVchannel: "p0_100v0"}
	check := func() error { return checkSplitPeers(ctx, cfg, "primary", parent) }
	require.ErrorIs(t, check(), merr.ErrServiceNotReady)
	record := splitTestRecord(parent.TaskId)
	record.Source = "p1_100v0"
	record.ReplicatedFrom = "primary"
	_, err = store.Record(ctx, record)
	require.NoError(t, err)
	require.ErrorIs(t, check(), merr.ErrServiceNotReady)
	require.NoError(t, store.MarkSourceAcked(ctx, parent.TaskId, 100, record.Source, []string{splitTestTarget1, splitTestTarget2}, nil, false))
	require.NoError(t, store.Advance(ctx, parent.TaskId, datapb.SplitShardTaskState_SplitShardTaskRetired))
	require.ErrorIs(t, check(), merr.ErrServiceNotReady)
	proof := &commonpb.ReplicateCheckpoint{ClusterId: "primary", Pchannel: "p0", MessageId: &commonpb.MessageID{Id: "17"}, TimeTick: 500}
	require.NoError(t, store.MarkSourceAcked(ctx, parent.TaskId, 100, record.Source, []string{splitTestTarget1, splitTestTarget2}, proof, true))
	require.NoError(t, check())
	cfg.Clusters[1].Pchannels = []string{"different"}
	require.ErrorIs(t, check(), merr.ErrServiceNotReady)
}

func TestSplitRetirementRequiresSameMemberAndTask(t *testing.T) {
	want := splitRetirement{Cluster: "west", Topology: "topology", CollectionID: 1, TaskID: 2, Source: "p_1v0", Retired: true}
	for _, change := range []func(*splitRetirement){
		func(s *splitRetirement) { s.Cluster = "east" }, func(s *splitRetirement) { s.Topology = "old" },
		func(s *splitRetirement) { s.CollectionID++ }, func(s *splitRetirement) { s.TaskID++ },
		func(s *splitRetirement) { s.Source = "other" }, func(s *splitRetirement) { s.Retired = false },
	} {
		got := want
		change(&got)
		payload, err := json.Marshal(got)
		require.NoError(t, err)
		require.ErrorIs(t, want.check(string(payload)), merr.ErrServiceNotReady)
	}
	require.Error(t, want.check("invalid"))
}

func TestSplitRetirementUnavailableTopology(t *testing.T) {
	ctx := context.Background()
	s := &Server{}
	parent := &datapb.SplitShardTask{TaskId: 1, CollectionId: 10, SourceVchannel: "p_10v0", State: datapb.SplitShardTaskState_SplitShardTaskRetired}
	var cfg *commonpb.ReplicateConfiguration
	var failure error
	mock := mockey.Mock(ddl.GetReplicationTopology).To(func(context.Context) (*commonpb.ReplicateConfiguration, error) { return cfg, failure }).Build()
	defer mock.UnPatch()
	parent.ReplicatedFromClusterId = "old-primary"
	require.ErrorIs(t, s.checkSplitParentRetired(ctx, parent), merr.ErrServiceNotReady)
	parent.ReplicatedFromClusterId = ""
	require.NoError(t, s.checkSplitParentRetired(ctx, parent))
	_, err := s.splitRetirementProgress(ctx, nil, gjson.Result{})
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	failure = context.DeadlineExceeded
	require.ErrorIs(t, s.checkSplitParentRetired(ctx, parent), failure)
	_, err = s.splitRetirementProgress(ctx, nil, gjson.Result{})
	require.ErrorIs(t, err, failure)
	failure = nil
	cfg = &commonpb.ReplicateConfiguration{Clusters: []*commonpb.MilvusCluster{{ClusterId: "a"}, {ClusterId: "b"}}}
	require.Error(t, s.checkSplitParentRetired(ctx, parent))
	cfg.CrossClusterTopology = []*commonpb.CrossClusterTopology{{SourceClusterId: "a", TargetClusterId: "b"}}
	require.Error(t, checkSplitPeers(ctx, cfg, "a", parent))
	parent.SourceVchannel = "invalid"
	require.Error(t, checkSplitPeers(ctx, cfg, "a", parent))
	_, err = splitTopologyID(&commonpb.ReplicateConfiguration{Clusters: []*commonpb.MilvusCluster{{ClusterId: string([]byte{0xfe})}}})
	require.Error(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, verifyPeerRetirement(cancelled, &commonpb.MilvusCluster{}, splitRetirement{}))
}

func TestSplitRetirementReadsLiveReplicationAssignment(t *testing.T) {
	ctx := t.Context()
	b := &mock_balancer.MockBalancer{}
	var balancerErr, assignmentErr error
	var assignment *balancer.WatchChannelAssignmentsCallbackParam
	get := mockey.Mock(balance.GetWithContext).To(func(context.Context) (balancer.Balancer, error) { return b, balancerErr }).Build()
	defer get.UnPatch()
	latest := mockey.Mock((*mock_balancer.MockBalancer).GetLatestChannelAssignment).To(func(*mock_balancer.MockBalancer) (*balancer.WatchChannelAssignmentsCallbackParam, error) {
		return assignment, assignmentErr
	}).Build()
	defer latest.UnPatch()
	_, active, err := ddl.GetReplicationRole(ctx)
	require.NoError(t, err)
	require.False(t, active)
	assignmentErr = merr.ErrServiceUnavailable
	_, err = ddl.GetReplicationTopology(ctx)
	require.ErrorIs(t, err, assignmentErr)
	balancerErr = context.DeadlineExceeded
	_, _, err = ddl.GetReplicationRole(ctx)
	require.ErrorIs(t, err, balancerErr)
}
