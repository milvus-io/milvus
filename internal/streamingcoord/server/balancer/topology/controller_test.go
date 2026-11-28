package topology

import (
	"context"
	"path"
	"testing"
	"testing/synctest"
	"time"

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

func setupTopologyCatalog(t *testing.T) Catalog {
	t.Helper()
	paramtable.Init()
	client, err := etcd.GetRemoteEtcdClient(paramtable.Get().EtcdCfg.Endpoints.GetAsStrings())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	kv := etcdkv.NewEtcdKV(client, path.Join("test-wal-replica", uuid.NewString()))
	t.Cleanup(func() { require.NoError(t, kv.RemoveWithPrefix(context.Background(), streamingcoord.MetaPrefix)) })
	return streamingcoord.NewReplicaTopologyCatalog(kv)
}

func TestRecoverTopologyFromEtcd(t *testing.T) {
	catalog := setupTopologyCatalog(t)
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
	offline, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = Recover(offline, catalog, allocate)
	require.Error(t, err)
	require.True(t, proto.Equal(initial, delayed.Request(nil, nodes, nil)))
	writerMember := initial.Topologies[0].Members[1]
	require.EqualValues(t, 2, writerMember.NodeId)
	writers["p"] = 2
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, nil, nil))
	require.True(t, proto.Equal(initial, c.Request(nil, nodes, nil)), "live writer must be discoverable before handoff")
	advertised := map[string]types.PChannelInfoAssigned{"p": {Node: types.StreamingNodeInfo{ServerID: 1}, Channel: types.PChannelInfo{WALReplicaID: initial.Topologies[0].WriterReplicaId, Term: initial.Topologies[0].WriterTerm}}}
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, nil, advertised))
	require.Error(t, delayed.Plan(t.Context(), writers, nodes, nodes, nil, advertised))
	require.Nil(t, delayed.Request(nil, nodes, nil), "failed CAS invalidates the old broadcast")
	require.Error(t, delayed.Plan(offline, writers, nodes, nodes, nil, advertised))
	require.Nil(t, delayed.Request(nil, nodes, nil), "cannot broadcast until catalog reload succeeds")
	require.NoError(t, delayed.Plan(t.Context(), writers, nodes, nodes, nil, advertised))
	latest := c.Request(nil, nodes, nil)
	require.True(t, proto.Equal(latest, delayed.Request(nil, nodes, nil)))
	require.Equal(t, writerMember.WalReplicaId, latest.Topologies[0].WriterReplicaId)
	require.Equal(t, initial.Topologies[0].WriterTerm+1, latest.Topologies[0].WriterTerm)
	require.Equal(t, initial.Topologies[1].Revision, latest.Topologies[1].Revision)
	revision := latest.Topologies[0].Revision
	reports := map[int64]*streamingpb.SyncWALReplicasResponse{1: {ObservedRevision: revision}, 2: {ObservedRevision: revision, Replicas: []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: writerMember.WalReplicaId, State: streamingpb.LocalWALReplica_RW, WriterTerm: proto.Int64(latest.Topologies[0].WriterTerm)}, {Pchannel: "q", WalReplicaId: 100}}}}
	require.Equal(t, []string{"p"}, c.Request(nil, nodes, reports).ConfirmedQueryMemberships)
	reports[2].Replicas = nil
	require.Equal(t, []string{"p", "q"}, c.Request(nil, nodes, reports).ConfirmedQueryMemberships)
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	require.True(t, proto.Equal(latest, c.Request(nil, nodes, nil)))
	// An old response from the designated writer cannot authorize reassignment.
	node := latest.Topologies[0].Members[0]
	reports[node.NodeId].Replicas = []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: node.WalReplicaId, State: streamingpb.LocalWALReplica_RO}}
	reports[2].Replicas = []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: writerMember.WalReplicaId, State: streamingpb.LocalWALReplica_RO}}
	reports[2].ObservedRevision--
	writers["p"] = 1
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	require.True(t, proto.Equal(latest, c.Request(nil, nodes, nil)))
	reports[2].ObservedRevision = revision
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	renewed := c.Request(nil, nodes, nil)
	require.Greater(t, renewed.Topologies[0].Revision, revision)
	require.Equal(t, node.WalReplicaId, renewed.Topologies[0].WriterReplicaId, "new policy selects node 1 after the terminal reader report")
	require.Equal(t, latest.Topologies[0].WriterTerm+1, renewed.Topologies[0].WriterTerm)
	require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	require.True(t, proto.Equal(renewed, c.Request(nil, nodes, nil)))
}

func TestPlanReplicaOutcomes(t *testing.T) {
	catalog := setupTopologyCatalog(t)
	var seq uint64
	allocate := func(context.Context) (uint64, error) { seq++; return seq, nil }
	c, err := Recover(t.Context(), catalog, allocate)
	require.NoError(t, err)
	nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {ResourceGroup: "a"}, 2: {ResourceGroup: "b"}}
	writers := map[string]int64{"p": 1}
	reports := map[int64]*streamingpb.SyncWALReplicasResponse{}
	step := func() *streamingpb.WALReplicaTopology {
		t.Helper()
		require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
		return c.desired["p"]
	}
	initial := step()
	readerID := initial.Members[1].WalReplicaId
	reports[1] = &streamingpb.SyncWALReplicasResponse{ObservedRevision: initial.Revision, Replicas: []*streamingpb.LocalWALReplica{
		{Pchannel: "p", WalReplicaId: readerID, State: streamingpb.LocalWALReplica_CLOSED},
		{Pchannel: "q", WalReplicaId: initial.WriterReplicaId, State: streamingpb.LocalWALReplica_CLOSED},
	}}
	require.Same(t, initial, step(), "wrong reporting node or PChannel cannot close a member")

	reports[1].Replicas = []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: initial.WriterReplicaId, State: streamingpb.LocalWALReplica_RO}}
	reports[2] = &streamingpb.SyncWALReplicasResponse{ObservedRevision: initial.Revision, Replicas: []*streamingpb.LocalWALReplica{
		{Pchannel: "p", WalReplicaId: readerID, State: streamingpb.LocalWALReplica_CLOSED},
	}}
	c.allocate = func(context.Context) (uint64, error) { return 0, context.DeadlineExceeded }
	require.ErrorIs(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil), context.DeadlineExceeded)
	require.Same(t, initial, c.Request(nil, nodes, nil).Topologies[0], "an allocation timeout cannot alter the published authority")
	c.allocate = allocate
	renewed := step()
	require.Greater(t, renewed.Revision, initial.Revision)
	require.Equal(t, initial.WriterReplicaId, renewed.WriterReplicaId)
	require.Equal(t, initial.WriterTerm+1, renewed.WriterTerm)
	require.NotEqual(t, readerID, renewed.Members[1].WalReplicaId)
	require.Same(t, renewed, step(), "duplicate old report cannot repeat the allocation")

	reports[1].ObservedRevision = renewed.Revision
	reports[1].Replicas[0].State = streamingpb.LocalWALReplica_CLOSED
	c.retryAfter["p"] = time.Now().Add(time.Hour)
	require.Same(t, renewed, step(), "repeated open failures are rate limited")
	c.retryAfter["p"] = time.Now().Add(-time.Second)
	replaced := step()
	require.NotEqual(t, renewed.WriterReplicaId, replaced.WriterReplicaId)
	require.Equal(t, renewed.WriterTerm+1, replaced.WriterTerm)
	require.Equal(t, renewed.Members[1], replaced.Members[1])

	// A subsequent failure of the reader alone preserves the active writer.
	reports[2].ObservedRevision = replaced.Revision
	reports[2].Replicas[0].WalReplicaId = replaced.Members[1].WalReplicaId
	c.retryAfter["p"] = time.Now().Add(-time.Second)
	repaired := step()
	require.Equal(t, replaced.WriterReplicaId, repaired.WriterReplicaId)
	require.Equal(t, replaced.WriterTerm, repaired.WriterTerm)
	require.NotEqual(t, replaced.Members[1].WalReplicaId, repaired.Members[1].WalReplicaId)
}

func TestRetrySyncUntilAllReplicasConverge(t *testing.T) {
	paramtable.Init()
	interval := &paramtable.Get().StreamingCfg.WALBalancerBackoffMaxInterval
	old := interval.SwapTempValue("100ms")
	defer interval.SwapTempValue(old)
	synctest.Test(t, func(t *testing.T) {
		c := &Controller{desired: map[string]*streamingpb.WALReplicaTopology{"p": {
			Pchannel: "p", Revision: 10, WriterReplicaId: 1, WriterTerm: 7,
			Members: []*streamingpb.WALReplicaPlacement{{NodeId: 1, WalReplicaId: 1}, {NodeId: 2, WalReplicaId: 2}},
		}}}
		live := map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {}, 2: {}}
		reports := map[int64]*streamingpb.SyncWALReplicasResponse{}
		sync := func() <-chan time.Time {
			c.Request(nil, live, reports)
			return c.RetryTimer()
		}
		first := sync()
		time.Sleep(50 * time.Millisecond)
		require.NotNil(t, first, "missing control streams need a retry")
		reports[1] = &streamingpb.SyncWALReplicasResponse{ObservedRevision: 10, Replicas: []*streamingpb.LocalWALReplica{
			{Pchannel: "p", WalReplicaId: 1, State: streamingpb.LocalWALReplica_PREPARING},
		}}
		require.Equal(t, first, sync(), "balance events must not postpone an existing retry")
		select {
		case <-first:
		case <-time.After(time.Second):
			t.Fatal("retry of an idle channel did not fire")
		}
		require.NotEqual(t, first, sync(), "while a member is preparing each timeout schedules the next retry")
		rw := reports[1].Replicas[0]
		rw.State = streamingpb.LocalWALReplica_RW
		rw.WriterTerm = proto.Int64(7)
		reports[2] = &streamingpb.SyncWALReplicasResponse{ObservedRevision: 9}
		require.NotNil(t, sync(), "stale inventory")
		reports[2].ObservedRevision = 10
		require.NotNil(t, sync(), "missing member in a current inventory")
		reports[2].Replicas = []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: 2, State: streamingpb.LocalWALReplica_RO}}
		rw.WriterTerm = proto.Int64(6)
		require.NotNil(t, sync(), "old writer term")
		rw.WriterTerm = proto.Int64(7)
		reports[2].Replicas = append(reports[2].Replicas, &streamingpb.LocalWALReplica{Pchannel: "p", WalReplicaId: 3})
		require.NotNil(t, sync(), "retiring instance on an otherwise ready node")
		reports[2].Replicas = reports[2].Replicas[:1]
		require.Nil(t, sync(), "ready topology has no retry timer")
	})
}

func TestPlanReopenWaitsForCompleteDrainedInventory(t *testing.T) {
	catalog := setupTopologyCatalog(t)
	var seq uint64
	allocate := func(context.Context) (uint64, error) { seq++; return seq, nil }
	nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{1: {ResourceGroup: "primary"}, 2: {ResourceGroup: "other"}}
	c, err := Recover(t.Context(), catalog, allocate)
	require.NoError(t, err)
	writers := map[string]int64{"p": 1, "q": 1}
	reports := map[int64]*streamingpb.SyncWALReplicasResponse{}
	plan := func() {
		t.Helper()
		require.NoError(t, c.Plan(t.Context(), writers, nodes, nodes, reports, nil))
	}
	plan()
	old := c.desired["p"]
	writers["p"] = 0
	plan()
	closed := c.desired["p"]
	require.Empty(t, closed.Members)
	require.Equal(t, old.WriterTerm, closed.WriterTerm)
	c, err = Recover(t.Context(), catalog, allocate)
	require.NoError(t, err)
	writers["p"] = 1
	reports[1] = &streamingpb.SyncWALReplicasResponse{ObservedRevision: closed.Revision}
	reports[2] = &streamingpb.SyncWALReplicasResponse{ObservedRevision: closed.Revision - 1}
	plan()
	require.True(t, proto.Equal(closed, c.desired["p"]), "live nodes must have observed the empty topology")
	reports[2].ObservedRevision = closed.Revision
	reports[2].Replicas = []*streamingpb.LocalWALReplica{{Pchannel: "p", WalReplicaId: old.Members[1].WalReplicaId, State: streamingpb.LocalWALReplica_PREPARING}}
	plan()
	require.True(t, proto.Equal(closed, c.desired["p"]), "draining IDs may still hold resources")
	delete(nodes, 2)
	plan()
	require.Len(t, c.desired["p"].Members, 1)
	require.Greater(t, c.desired["p"].Revision, closed.Revision)
	require.Greater(t, c.desired["p"].WriterTerm, closed.WriterTerm)
	require.NotEqual(t, old.WriterReplicaId, c.desired["p"].WriterReplicaId)
}
