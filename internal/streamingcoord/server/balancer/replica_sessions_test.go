package balancer_test

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/client/manager"
	coretypes "github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type balanceIDs struct {
	idalloc.Allocator
	next atomic.Uint64
}

func (s *balanceIDs) Allocate(context.Context) (uint64, error) { return s.next.Add(1), nil }

type balanceNodes struct {
	manager.ManagerClient
	mu     sync.Mutex
	groups map[int64]string
	ads    map[int64][]*streamingpb.WALAdvertisement
	notify chan struct{}
}

func (n *balanceNodes) WatchNodeChanged(context.Context) (<-chan struct{}, error) {
	return n.notify, nil
}

func (n *balanceNodes) GetAllStreamingNodes(context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	nodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{}
	for id, rg := range n.groups {
		nodes[id] = &types.StreamingNodeInfoWithResourceGroup{StreamingNodeInfo: types.StreamingNodeInfo{ServerID: id}, ResourceGroup: rg}
	}
	return nodes, nil
}

func (n *balanceNodes) CollectAllStatus(ctx context.Context, rg string) (map[int64]*types.StreamingNodeStatus, error) {
	nodes, _ := n.GetAllStreamingNodes(ctx)
	selected := map[int64]*types.StreamingNodeStatus{}
	for id, node := range nodes {
		if node.ResourceGroup == rg {
			selected[id] = &types.StreamingNodeStatus{StreamingNodeInfo: node.StreamingNodeInfo}
		}
	}
	if len(selected) == 0 {
		for id, node := range nodes {
			selected[id] = &types.StreamingNodeStatus{StreamingNodeInfo: node.StreamingNodeInfo}
		}
	}
	return selected, nil
}

func (n *balanceNodes) sessions() map[string]*sessionutil.Session {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[string]*sessionutil.Session{}
	for id, group := range n.groups {
		out[fmt.Sprint(id)] = &sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: id, ServerLabels: map[string]string{sessionutil.LabelResourceGroup: group}, WALReplicas: n.ads[id]}}
	}
	return out
}

type balanceStream struct {
	streamingpb.StreamingNodeWALReplicaService_SyncWALReplicasClient
	ctx    context.Context
	id     int64
	nodes  *balanceNodes
	report chan *streamingpb.SyncWALReplicasResponse
}

func (s *balanceStream) Send(req *streamingpb.SyncWALReplicasRequest) error {
	response := &streamingpb.SyncWALReplicasResponse{}
	var ads []*streamingpb.WALAdvertisement
	for _, t := range req.Topologies {
		response.ObservedRevision = max(response.ObservedRevision, t.Revision)
		for _, m := range t.Members {
			if m.NodeId != s.id {
				continue
			}
			a := &streamingpb.WALAdvertisement{Pchannel: t.Pchannel, WalReplicaId: m.WalReplicaId}
			if m.WalReplicaId == t.WriterReplicaId {
				a.WriterTerm = proto.Int64(t.WriterTerm)
				a.IsCchannel = t.Pchannel == req.Cchannel.GetPchannel()
			}
			ads = append(ads, a)
			response.Replicas = append(response.Replicas, &streamingpb.LocalWALReplica{Pchannel: t.Pchannel, WalReplicaId: m.WalReplicaId, State: streamingpb.LocalWALReplica_READY})
		}
	}
	s.nodes.mu.Lock()
	s.nodes.ads[s.id] = ads
	s.nodes.mu.Unlock()
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.report <- response:
		return nil
	}
}

func (s *balanceStream) Recv() (*streamingpb.SyncWALReplicasResponse, error) {
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case r := <-s.report:
		return r, nil
	}
}

type registeredSession struct{ *balanceTestSession }

func (s *registeredSession) GetRegisteredRevision() int64 { return 1 }

func TestBalanceReplicatedWALSessions(t *testing.T) {
	mockey.PatchConvey("balance consumes session routes and persists only desired topology", t, func() {
		setupBalancerConfigSource(t)
		cfg := &paramtable.Get().StreamingCfg
		old := cfg.PrimaryResourceGroup.SwapTempValue("primary")
		defer cfg.PrimaryResourceGroup.SwapTempValue(old)
		channel.ResetStaticPChannelStatsManager()
		channel.RecoverPChannelStatsManager(nil)
		client, _ := kvfactory.GetEtcdAndPath()
		meta := etcdkv.NewEtcdKV(client, path.Join("test-wal-balancer", fmt.Sprint(time.Now().UnixNano())))
		defer func() { require.NoError(t, meta.RemoveWithPrefix(context.Background(), catalogkv.MetaPrefix)) }()
		catalog := catalogkv.NewCataLog(meta)
		require.NoError(t, catalog.SaveVersion(t.Context(), &streamingpb.StreamingVersion{Version: channel.StreamingVersion300}))
		topologies := catalogkv.NewReplicaTopologyCatalog(meta)
		nodes := &balanceNodes{groups: map[int64]string{1: "primary", 2: "reader"}, ads: map[int64][]*streamingpb.WALAdvertisement{}, notify: make(chan struct{})}
		mockey.Mock(idalloc.NewIDAllocator).Return(&balanceIDs{}).Build()
		mockey.Mock((*manager.WALReplicaClient).Sync).To(func(_ *manager.WALReplicaClient, ctx context.Context, id int64) (streamingpb.StreamingNodeWALReplicaService_SyncWALReplicasClient, error) {
			return &balanceStream{ctx: ctx, id: id, nodes: nodes, report: make(chan *streamingpb.SyncWALReplicasResponse, 1)}, nil
		}).Build()
		resource.InitForTest(resource.OptETCD(client), resource.OptSession(&registeredSession{&balanceTestSession{list: nodes.sessions}}), resource.OptStreamingCatalog(catalog), resource.OptTopologyCatalog(topologies), resource.OptMixCoordClient(syncutil.NewFuture[coretypes.MixCoordClient]()), resource.OptStreamingManagerClient(nodes))
		provider := newStaticChannelProvider("p")
		b, err := balancer.RecoverBalancer(t.Context(), provider)
		require.NoError(t, err)
		defer b.Close()
		wait := func(name string, id int64) *streamingpb.WALReplicaTopology {
			var result *streamingpb.WALReplicaTopology
			require.Eventually(t, func() bool {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				if b.Trigger(ctx) != nil {
					return false
				}
				r, e := b.GetLatestChannelAssignment()
				if e != nil {
					return false
				}
				for _, w := range r.Relations {
					if w.Channel.Name == name && w.Node.ServerID == id {
						topos, e := topologies.List(ctx)
						if e != nil {
							return false
						}
						result = topos[name]
						return result != nil && w.Channel.WALReplicaID == result.WriterReplicaId && w.Channel.Term == result.WriterTerm
					}
				}
				return false
			}, 12*time.Second, 20*time.Millisecond)
			return result
		}
		first := wait("p", 1)
		require.Len(t, first.Members, 2)
		provider.ch <- []string{"q"}
		wait("q", 1)
		cfg.PrimaryResourceGroup.SwapTempValue("reader")
		second := wait("p", 2)
		require.Greater(t, second.WriterTerm, first.WriterTerm)
		require.Equal(t, first.Members, second.Members)
		nodes.mu.Lock()
		delete(nodes.groups, 2)
		nodes.mu.Unlock()
		third := wait("p", 1)
		require.Greater(t, third.WriterTerm, second.WriterTerm)
		require.Equal(t, first.WriterReplicaId, third.WriterReplicaId)
		close(provider.ch)
	})
}
