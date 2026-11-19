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
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type splitRetirementPeer struct {
	milvuspb.UnimplementedMilvusServiceServer
	dc *Server
}

func (p *splitRetirementPeer) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	result, err := p.dc.splitRetirementProgress(ctx, req, gjson.Parse(req.GetRequest()))
	return &milvuspb.GetMetricsResponse{Status: merr.Status(err), Response: result}, nil
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
	cfgMock := mockey.Mock(ddl.GetReplicationTopology).Return(replicaCfg, nil).Build()
	defer cfgMock.UnPatch()
	store, _ := newSplitTestStore(t)
	server := grpc.NewServer()
	milvuspb.RegisterMilvusServiceServer(server, &splitRetirementPeer{dc: &Server{splitStore: store}})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	parent := &datapb.SplitShardTask{TaskId: 123456789012345678, CollectionId: 100, SourceVchannel: "p0_100v0"}
	check := func() error { return checkSplitPeers(ctx, cfg, "primary", parent) }
	require.ErrorIs(t, check(), merr.ErrServiceNotReady)
	record := splitTestRecord(parent.TaskId)
	record.Source = "p1_100v0"
	_, err = store.Record(ctx, record)
	require.NoError(t, err)
	require.ErrorIs(t, check(), merr.ErrServiceNotReady)
	require.NoError(t, store.MarkSourceAcked(ctx, parent.TaskId, 100, record.Source, []string{splitTestTarget1, splitTestTarget2}))
	require.NoError(t, store.Advance(ctx, parent.TaskId, datapb.SplitShardTaskState_SplitShardTaskRetired))
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
	parent := &datapb.SplitShardTask{TaskId: 1, CollectionId: 10, SourceVchannel: "p_10v0"}
	var cfg *commonpb.ReplicateConfiguration
	var failure error
	mock := mockey.Mock(ddl.GetReplicationTopology).To(func(context.Context) (*commonpb.ReplicateConfiguration, error) { return cfg, failure }).Build()
	defer mock.UnPatch()
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
