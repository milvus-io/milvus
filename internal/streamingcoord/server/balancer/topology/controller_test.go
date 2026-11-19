package topology

import (
	"context"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestRecoverTopologyFromEtcd(t *testing.T) {
	paramtable.Init()
	client, err := etcd.GetRemoteEtcdClient(paramtable.Get().EtcdCfg.Endpoints.GetAsStrings())
	require.NoError(t, err)
	defer client.Close()
	kv := etcdkv.NewEtcdKV(client, path.Join("test-wal-replica", uuid.NewString()))
	defer func() { require.NoError(t, kv.RemoveWithPrefix(context.Background(), streamingcoord.MetaPrefix)) }()
	catalog := streamingcoord.NewReplicaTopologyCatalog(kv)
	var seq uint64
	allocate := func(context.Context) (uint64, error) { seq++; return seq, nil }
	c, err := Recover(t.Context(), catalog, allocate)
	require.NoError(t, err)
	nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {ResourceGroup: "a"}, 2: {ResourceGroup: "b"}}
	writers := map[string]int64{"p": 1, "q": 1}
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, nil, nil))
	initial := c.Request(nil, nodes, nil)
	require.Len(t, initial.Topologies, 2)
	require.Empty(t, initial.ConfirmedQueryMemberships)
	delayed, err := Recover(t.Context(), catalog, allocate)
	require.NoError(t, err)
	require.True(t, proto.Equal(initial, delayed.Request(nil, nodes, nil)))
	writerMember := initial.Topologies[0].Members[1]
	require.EqualValues(t, 2, writerMember.NodeId)
	writers["p"] = 2
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, nil, nil))
	require.True(t, proto.Equal(initial, c.Request(nil, nodes, nil)), "live writer must be discoverable before handoff")
	advertised := map[string]types.PChannelInfoAssigned{"p": {Node: types.StreamingNodeInfo{ServerID: 1}, Channel: types.PChannelInfo{WALReplicaID: initial.Topologies[0].WriterReplicaId, Term: initial.Topologies[0].WriterTerm}}}
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, nil, advertised))
	require.Error(t, delayed.Plan(t.Context(), writers, nodes, nodes, nil, advertised))
	require.NoError(t, delayed.Plan(t.Context(), writers, nodes, nodes, nil, advertised))
	latest := c.Request(nil, nodes, nil)
	require.True(t, proto.Equal(latest, delayed.Request(nil, nodes, nil)))
	require.Equal(t, writerMember.WalReplicaId, latest.Topologies[0].WriterReplicaId)
	require.Equal(t, initial.Topologies[0].WriterTerm+1, latest.Topologies[0].WriterTerm)
	require.Equal(t, initial.Topologies[1].Revision, latest.Topologies[1].Revision)
	revision := latest.Topologies[0].Revision
	reports := map[int64]*streamingpb.SyncWALReplicasResponse{1: {ObservedRevision: revision}, 2: {ObservedRevision: revision, Replicas: []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: writerMember.WalReplicaId, State: streamingpb.LocalWALReplica_READY}, {Pchannel: "q", WalReplicaId: 100}}}}
	require.Equal(t, []string{"p"}, c.Request(nil, nodes, reports).ConfirmedQueryMemberships)
	reports[2].Replicas = nil
	require.Equal(t, []string{"p", "q"}, c.Request(nil, nodes, reports).ConfirmedQueryMemberships)
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	require.True(t, proto.Equal(latest, c.Request(nil, nodes, nil)))
	// RO failures cannot change the writer term. An old response from the
	// designated writer is likewise insufficient.
	node := latest.Topologies[0].Members[0]
	reports[node.NodeId].Replicas = []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: node.WalReplicaId, State: streamingpb.LocalWALReplica_UNRECOVERABLE}}
	reports[2].Replicas = []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: writerMember.WalReplicaId, State: streamingpb.LocalWALReplica_UNRECOVERABLE}}
	reports[2].ObservedRevision--
	writers["p"] = 1
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	require.True(t, proto.Equal(latest, c.Request(nil, nodes, nil)))
	reports[2].ObservedRevision = revision
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	renewed := c.Request(nil, nodes, nil)
	require.Greater(t, renewed.Topologies[0].Revision, revision)
	require.Equal(t, writerMember.WalReplicaId, renewed.Topologies[0].WriterReplicaId)
	require.Equal(t, latest.Topologies[0].WriterTerm+1, renewed.Topologies[0].WriterTerm)
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	require.True(t, proto.Equal(renewed, c.Request(nil, nodes, nil)))
}
