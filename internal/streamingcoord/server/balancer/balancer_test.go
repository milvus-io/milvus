package balancer_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_manager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	_ "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/policy"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	coretypes "github.com/milvus-io/milvus/internal/types"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBalancerFreezeNodeInOtherResourceGroup(t *testing.T) {
	// Regression test for #53176: a frozen node that is still in the session
	// (GetAllStreamingNodes) but invisible to CollectAllStatus (because it
	// belongs to a non-primary resource group) must NOT be silently unfrozen
	// during the freeze cleanup in fetchStreamingNodeStatus.
	setupBalancerConfigSource(t)
	oldRootPath := paramtable.Get().EtcdCfg.RootPath.SwapTempValue(fmt.Sprintf("freeze-other-rg-%d", time.Now().UnixNano()))
	oldMetaSubPath := paramtable.Get().EtcdCfg.MetaSubPath.SwapTempValue("meta")
	oldExpectedStreamingNodeNum := paramtable.Get().StreamingCfg.WALBalancerExpectedInitialStreamingNodeNum.SwapTempValue("0")
	defer paramtable.Get().EtcdCfg.RootPath.SwapTempValue(oldRootPath)
	defer paramtable.Get().EtcdCfg.MetaSubPath.SwapTempValue(oldMetaSubPath)
	defer paramtable.Get().StreamingCfg.WALBalancerExpectedInitialStreamingNodeNum.SwapTempValue(oldExpectedStreamingNodeNum)
	etcdClient, _ := kvfactory.GetEtcdAndPath()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})

	// Signal every balance round through this channel so the test can wait
	// deterministically for the round that runs the freeze cleanup.
	collected := make(chan struct{}, 64)
	// session view: node 3 is alive but belongs to another resource group.
	// Guarded by a mutex: the balancer goroutine reads it via the mock
	// closure while the test mutates it to simulate node 3 leaving the session.
	var allNodesMu sync.RWMutex
	allNodes := map[int64]*types.StreamingNodeInfoWithResourceGroup{
		1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}, ResourceGroup: "rg-primary"},
		2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"}, ResourceGroup: "rg-primary"},
		3: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 3, Address: "localhost:3"}, ResourceGroup: "rg-other"},
	}
	streamingNodeManager := mock_manager.NewMockManagerClient(t)
	streamingNodeManager.EXPECT().WatchNodeChanged(mock.Anything).Return(make(chan struct{}), nil)
	streamingNodeManager.EXPECT().Assign(mock.Anything, mock.Anything).Return(nil).Maybe()
	streamingNodeManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil).Maybe()
	// No .Maybe(): the freeze cleanup MUST call GetAllStreamingNodes every round,
	// otherwise this test would silently pass on the buggy RG-filtered cleanup.
	streamingNodeManager.EXPECT().GetAllStreamingNodes(mock.Anything).RunAndReturn(func(ctx context.Context) (map[int64]*types.StreamingNodeInfoWithResourceGroup, error) {
		allNodesMu.RLock()
		defer allNodesMu.RUnlock()
		result := make(map[int64]*types.StreamingNodeInfoWithResourceGroup, len(allNodes))
		for id, node := range allNodes {
			result[id] = node
		}
		return result, nil
	})
	streamingNodeManager.EXPECT().CollectAllStatus(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rgName string) (map[int64]*types.StreamingNodeStatus, error) {
		select {
		case collected <- struct{}{}:
		default:
		}
		// node 3 is NOT in the primary resource group status view.
		return map[int64]*types.StreamingNodeStatus{
			1: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 1, Address: "localhost:1"}},
			2: {StreamingNodeInfo: types.StreamingNodeInfo{ServerID: 2, Address: "localhost:2"}},
		}, nil
	})

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(
		resource.OptETCD(etcdClient),
		resource.OptStreamingCatalog(catalog),
		resource.OptStreamingManagerClient(streamingNodeManager),
		resource.OptSession(&balanceTestSession{SessionInterface: s}),
		resource.OptTopologyCatalog(catalogkv.NewReplicaTopologyCatalog(etcdkv.NewEtcdKV(etcdClient, paramtable.Get().EtcdCfg.MetaRootPath.GetValue()))),
		resource.OptMixCoordClient(syncutil.NewFuture[coretypes.MixCoordClient]()),
	)
	catalog.EXPECT().GetCChannel(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveCChannel(mock.Anything, mock.Anything).Return(nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Return(nil).Maybe()
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	ctx := context.Background()
	b, err := balancer.RecoverBalancer(ctx, newStaticChannelProvider("initial-channel"))
	assert.NoError(t, err)
	assert.NotNil(t, b)
	defer b.Close()

	// waitForBalanceRound requests a balance round and blocks until it has
	// completed. The first Trigger's future is set inside apply() before the
	// round runs, so the collected signal only proves a round started; the
	// second Trigger's future is set only when the execute loop returns to
	// the select, which is after the in-flight round finished. That makes
	// every assertion after this helper ordered after the cleanup ran.
	waitForBalanceRound := func() {
		assert.NoError(t, b.Trigger(ctx)) // request a round
		select {
		case <-collected: // that (or a later) round has started
		case <-time.After(30 * time.Second):
			t.Fatal("no balance round observed")
		}
		assert.NoError(t, b.Trigger(ctx)) // returns only after the in-flight round completed
	}
	// Drain any signals left by rounds before the freeze request was applied.
	drainCollected := func() {
		for {
			select {
			case <-collected:
			default:
				return
			}
		}
	}
	waitForBalanceRound()

	// freeze node 3, which is in session but not in the primary RG status view.
	resp, err := b.UpdateBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{AllowRebalance: true},
		Nodes: &streamingpb.WALBalancePolicyNodes{
			FreezeNodeIds: []int64{3},
		},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{3}, resp.FreezeNodeIds)

	// Force one full balance round after the freeze: the cleanup in
	// fetchStreamingNodeStatus must run and must NOT unfreeze node 3.
	drainCollected()
	waitForBalanceRound()

	// node 3 must still be frozen: excluded from available nodes and still
	// tracked in the freeze set.
	nodes, err := b.GetAvailableStreamingNodes(ctx)
	assert.NoError(t, err)
	assert.NotContains(t, nodes, int64(3))
	resp, err = b.UpdateBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{AllowRebalance: true},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []int64{3}, resp.FreezeNodeIds)

	// The freeze must still be released when the node genuinely leaves the
	// session, otherwise freezeNodes accumulates dead IDs.
	allNodesMu.Lock()
	delete(allNodes, 3)
	allNodesMu.Unlock()
	drainCollected()
	waitForBalanceRound()
	resp, err = b.UpdateBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{
		Config: &streamingpb.WALBalancePolicyConfig{AllowRebalance: true},
	})
	assert.NoError(t, err)
	assert.Empty(t, resp.FreezeNodeIds)
}

func TestBalancerWaitUntilSchemaDropReady(t *testing.T) {
	paramtable.Init()
	oldRootPath := paramtable.Get().EtcdCfg.RootPath.SwapTempValue(fmt.Sprintf("schema-drop-ready-%d", time.Now().UnixNano()))
	oldMetaSubPath := paramtable.Get().EtcdCfg.MetaSubPath.SwapTempValue("meta")
	oldExpectedStreamingNodeNum := paramtable.Get().StreamingCfg.WALBalancerExpectedInitialStreamingNodeNum.SwapTempValue("0")
	defer paramtable.Get().EtcdCfg.RootPath.SwapTempValue(oldRootPath)
	defer paramtable.Get().EtcdCfg.MetaSubPath.SwapTempValue(oldMetaSubPath)
	defer paramtable.Get().StreamingCfg.WALBalancerExpectedInitialStreamingNodeNum.SwapTempValue(oldExpectedStreamingNodeNum)
	metaRoot := paramtable.Get().EtcdCfg.MetaRootPath.GetValue()

	etcdClient, _ := kvfactory.GetEtcdAndPath()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})

	streamingNodeManager := mock_manager.NewMockManagerClient(t)
	streamingNodeManager.EXPECT().WatchNodeChanged(mock.Anything).Return(make(chan struct{}), nil).Maybe()
	streamingNodeManager.EXPECT().Assign(mock.Anything, mock.Anything).Return(nil).Maybe()
	streamingNodeManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil).Maybe()
	streamingNodeManager.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{}, nil).Maybe()
	streamingNodeManager.EXPECT().CollectAllStatus(mock.Anything, mock.Anything).Return(map[int64]*types.StreamingNodeStatus{}, nil).Maybe()

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(
		resource.OptETCD(etcdClient),
		resource.OptStreamingCatalog(catalog),
		resource.OptStreamingManagerClient(streamingNodeManager),
		resource.OptSession(&balanceTestSession{SessionInterface: s}),
		resource.OptTopologyCatalog(catalogkv.NewReplicaTopologyCatalog(etcdkv.NewEtcdKV(etcdClient, paramtable.Get().EtcdCfg.MetaRootPath.GetValue()))),
		resource.OptMixCoordClient(syncutil.NewFuture[coretypes.MixCoordClient]()),
	)
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{Pchannel: "schema-drop-ready-channel"}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(nil, nil)
	savedVersions := make(chan int64, 4)
	catalog.EXPECT().SaveVersion(mock.Anything, mock.Anything).Run(func(_ context.Context, version *streamingpb.StreamingVersion) {
		savedVersions <- version.GetVersion()
	}).Return(nil).Maybe()
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	ctx := context.Background()
	b, err := balancer.RecoverBalancer(ctx, newStaticChannelProvider())
	if !assert.NoError(t, err) {
		return
	}
	defer b.Close()

	waitCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	readyProxyKey := path.Join(metaRoot, sessionutil.DefaultServiceRoot, typeutil.ProxyRole+"-ready")
	putProxySession(t, waitCtx, readyProxyKey, "3.0.0-beta")
	defer resource.Resource().ETCD().Delete(context.Background(), readyProxyKey)

	legacyProxyKey := path.Join(metaRoot, sessionutil.DefaultServiceRoot, typeutil.ProxyRole+"-legacy")
	putProxySession(t, waitCtx, legacyProxyKey, "2.6.6")
	defer resource.Resource().ETCD().Delete(context.Background(), legacyProxyKey)

	cancelCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	err = b.WaitUntilSchemaDropReady(cancelCtx)
	cancel()
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	waitDone := make(chan error, 1)
	go func() {
		waitDone <- b.WaitUntilSchemaDropReady(context.Background())
	}()
	select {
	case err := <-waitDone:
		assert.NoError(t, err)
		assert.Fail(t, "schema drop readiness should wait for legacy Proxy sessions")
	case <-time.After(100 * time.Millisecond):
	}

	_, err = resource.Resource().ETCD().Delete(context.Background(), legacyProxyKey)
	assert.NoError(t, err)
	select {
	case err := <-waitDone:
		assert.NoError(t, err)
	case <-time.After(3 * time.Second):
		assert.Fail(t, "schema drop readiness did not unblock after legacy Proxy session disappeared")
	}
	assertSavedStreamingVersion(t, savedVersions, channel.StreamingVersion300)
}

func TestBalancerWaitUntilSchemaDropReadySkipsAfterPersistedVersion(t *testing.T) {
	paramtable.Init()
	oldRootPath := paramtable.Get().EtcdCfg.RootPath.SwapTempValue(fmt.Sprintf("schema-drop-ready-skip-%d", time.Now().UnixNano()))
	oldMetaSubPath := paramtable.Get().EtcdCfg.MetaSubPath.SwapTempValue("meta")
	oldExpectedStreamingNodeNum := paramtable.Get().StreamingCfg.WALBalancerExpectedInitialStreamingNodeNum.SwapTempValue("0")
	defer paramtable.Get().EtcdCfg.RootPath.SwapTempValue(oldRootPath)
	defer paramtable.Get().EtcdCfg.MetaSubPath.SwapTempValue(oldMetaSubPath)
	defer paramtable.Get().StreamingCfg.WALBalancerExpectedInitialStreamingNodeNum.SwapTempValue(oldExpectedStreamingNodeNum)
	metaRoot := paramtable.Get().EtcdCfg.MetaRootPath.GetValue()

	etcdClient, _ := kvfactory.GetEtcdAndPath()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})

	streamingNodeManager := mock_manager.NewMockManagerClient(t)
	streamingNodeManager.EXPECT().WatchNodeChanged(mock.Anything).Return(make(chan struct{}), nil).Maybe()
	streamingNodeManager.EXPECT().Assign(mock.Anything, mock.Anything).Return(nil).Maybe()
	streamingNodeManager.EXPECT().Remove(mock.Anything, mock.Anything).Return(nil).Maybe()
	streamingNodeManager.EXPECT().GetAllStreamingNodes(mock.Anything).Return(map[int64]*types.StreamingNodeInfoWithResourceGroup{}, nil).Maybe()
	streamingNodeManager.EXPECT().CollectAllStatus(mock.Anything, mock.Anything).Return(map[int64]*types.StreamingNodeStatus{}, nil).Maybe()

	catalog := mock_metastore.NewMockStreamingCoordCataLog(t)
	s := sessionutil.NewMockSession(t)
	s.EXPECT().GetRegisteredRevision().Return(int64(1))
	resource.InitForTest(
		resource.OptETCD(etcdClient),
		resource.OptStreamingCatalog(catalog),
		resource.OptStreamingManagerClient(streamingNodeManager),
		resource.OptSession(&balanceTestSession{SessionInterface: s}),
		resource.OptTopologyCatalog(catalogkv.NewReplicaTopologyCatalog(etcdkv.NewEtcdKV(etcdClient, paramtable.Get().EtcdCfg.MetaRootPath.GetValue()))),
		resource.OptMixCoordClient(syncutil.NewFuture[coretypes.MixCoordClient]()),
	)
	catalog.EXPECT().GetCChannel(mock.Anything).Return(&streamingpb.CChannelMeta{Pchannel: "schema-drop-ready-skip-channel"}, nil)
	catalog.EXPECT().GetVersion(mock.Anything).Return(&streamingpb.StreamingVersion{Version: channel.StreamingVersion300}, nil)
	catalog.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, nil)

	ctx := context.Background()
	legacyProxyKey := path.Join(metaRoot, sessionutil.DefaultServiceRoot, typeutil.ProxyRole+"-legacy")
	putProxySession(t, ctx, legacyProxyKey, "2.6.6")
	defer resource.Resource().ETCD().Delete(context.Background(), legacyProxyKey)

	b, err := balancer.RecoverBalancer(ctx, newStaticChannelProvider())
	if !assert.NoError(t, err) {
		return
	}
	defer b.Close()

	waitCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	assert.NoError(t, b.WaitUntilSchemaDropReady(waitCtx))
}

func putProxySession(t *testing.T, ctx context.Context, key string, version string) {
	t.Helper()

	raw := sessionutil.SessionRaw{Version: version, ServerID: 1}
	data, err := json.Marshal(raw)
	assert.NoError(t, err)
	_, err = resource.Resource().ETCD().Put(ctx, key, string(data))
	assert.NoError(t, err)
}

func assertSavedStreamingVersion(t *testing.T, savedVersions <-chan int64, expected int64) {
	t.Helper()

	for {
		select {
		case version := <-savedVersions:
			if version == expected {
				return
			}
		default:
			assert.Failf(t, "streaming version was not saved", "expected version %d", expected)
			return
		}
	}
}

// staticChannelProvider is a test helper implementing balancer.ChannelProvider with static channels.
type staticChannelProvider struct {
	channels []string
	ch       chan []string
}

func newStaticChannelProvider(channels ...string) *staticChannelProvider {
	return &staticChannelProvider{
		channels: channels,
		ch:       make(chan []string),
	}
}

func (p *staticChannelProvider) GetInitialChannels() []string {
	return p.channels
}

func (p *staticChannelProvider) NewIncomingChannels() <-chan []string {
	return p.ch
}

func (p *staticChannelProvider) Close() {}

// balanceTestSession supplies the linearizable Session inventory to the
// control loop. Gate-only tests use an empty inventory.
type balanceTestSession struct {
	sessionutil.SessionInterface
	list func() map[string]*sessionutil.Session
}

func (s *balanceTestSession) GetSessions(context.Context, string) (map[string]*sessionutil.Session, int64, error) {
	if s.list == nil {
		return nil, 1, nil
	}
	return s.list(), 1, nil
}
