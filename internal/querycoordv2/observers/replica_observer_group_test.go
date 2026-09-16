// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package observers

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const observerGroupConfig = `[{"id":"g","collectionIds":["1","2","3"]}]`

type observerRowSource struct {
	mu       sync.Mutex
	segments map[int64][]*datapb.SegmentInfo
	calls    map[int64]int
	err      error
	onFetch  func()
}

func newGroupObserver(t *testing.T, groups string) (*ReplicaObserver, *observerRowSource) {
	t.Helper()
	paramtable.Init()
	key := paramtable.Get().QueryCoordCfg.CollectionGroups.Key
	require.NoError(t, paramtable.Get().Save(key, groups))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	cfg := &paramtable.Get().EtcdCfg
	client, err := etcd.GetEtcdClient(cfg.UseEmbedEtcd.GetAsBool(), cfg.EtcdUseSSL.GetAsBool(), cfg.Endpoints.GetAsStrings(), cfg.EtcdTLSCert.GetValue(), cfg.EtcdTLSKey.GetValue(), cfg.EtcdTLSCACert.GetValue(), cfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	store := etcdkv.NewEtcdKV(client, "test-group-observer-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")); store.Close() })
	nodes := session.NewNodeManager()
	metadata := meta.NewMeta(params.RandomIncrementIDAllocator(), querycoord.NewCatalog(store), nodes)
	for id := int64(1); id <= 4; id++ {
		nodes.Add(session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: id, Address: "localhost", Hostname: "localhost"}))
		metadata.HandleNodeUp(context.Background(), id)
	}
	source := &observerRowSource{segments: map[int64][]*datapb.SegmentInfo{
		1: {{ID: 1, NumOfRows: 800}}, 2: {{ID: 2, NumOfRows: 400}}, 3: {{ID: 3, NumOfRows: 400}},
	}, calls: make(map[int64]int)}
	patch := mockey.Mock((*meta.CoordinatorBroker).GetRecoveryInfoV2).To(func(_ *meta.CoordinatorBroker, _ context.Context, id int64, _ ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
		source.mu.Lock()
		defer source.mu.Unlock()
		source.calls[id]++
		if source.onFetch != nil {
			source.onFetch()
		}
		return nil, source.segments[id], source.err
	}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	observer := NewReplicaObserver(metadata, meta.NewDistributionManager(nodes), nil, meta.NewCoordinatorBroker(nil))
	t.Cleanup(observer.Stop)
	return observer, source
}

func spawnObserved(t *testing.T, ob *ReplicaObserver, id int64) int64 {
	t.Helper()
	replicas, err := ob.meta.Spawn(context.Background(), id, map[string]int{meta.DefaultResourceGroupName: 1}, nil, commonpb.LoadPriority_HIGH)
	require.NoError(t, err)
	return replicas[0].GetID()
}

func TestCollectionGroupConfigAndBatching(t *testing.T) {
	for _, raw := range []string{"{", `[{"id":""}]`, `[{"id":"g"},{"id":"g"}]`, `[{"id":"g","collectionIds":["0"]}]`, `[{"id":"g","collectionIds":["9223372036854775808"]}]`, `[{"id":"a","collectionIds":["1"]},{"id":"b","collectionIds":["1"]}]`} {
		_, err := parseCollectionGroups(raw)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
	groups, err := parseCollectionGroups(observerGroupConfig)
	require.NoError(t, err)
	ob := &ReplicaObserver{collectionGroups: groups}
	require.Equal(t, [][]int64{{1, 2, 3}, {4}, {5}}, ob.collectionBatches([]int64{1, 2, 3, 4, 5}))
	groups, err = parseCollectionGroups("[]")
	require.NoError(t, err)
	require.Empty(t, groups)
}

func TestReplicaObserverOneCallPerBatchAndCachedRows(t *testing.T) {
	ctx := context.Background()
	ob, source := newGroupObserver(t, observerGroupConfig)
	for id := int64(1); id <= 4; id++ {
		spawnObserved(t, ob, id)
	}
	var batches [][]int64
	patch := mockey.Mock((*meta.ReplicaManager).RecoverNodesInCollections).To(func(_ *meta.ReplicaManager, _ context.Context, ids []int64, rgs map[string]*meta.ResourceGroup, rows map[int64]meta.CollectionRowCount) error {
		batches = append(batches, append([]int64(nil), ids...))
		require.Len(t, rgs[meta.DefaultResourceGroupName].GetNodes(), 4)
		if len(ids) > 1 {
			require.Equal(t, int64(800), rows[1].Rows)
			for _, id := range ids[1:] {
				require.Equal(t, int64(400), rows[id].Rows)
			}
			return merr.WrapErrServiceUnavailableMsg("test allocation failure")
		}
		require.Empty(t, rows)
		return nil
	}).Build()
	defer patch.UnPatch()
	require.NoError(t, ob.Init())
	ob.checkNodesInReplica(ctx)
	require.Equal(t, [][]int64{{1, 2, 3}, {4}}, batches)
	ob.checkNodesInReplica(ctx)
	require.Equal(t, map[int64]int{1: 1, 2: 1, 3: 1}, source.calls)
	require.NoError(t, ob.meta.ReplicaManager.RemoveCollection(ctx, 2))
	ob.checkNodesInReplica(ctx)
	require.NotContains(t, ob.rowStats, int64(2))
	require.NoError(t, paramtable.Get().Save(paramtable.Get().QueryCoordCfg.CollectionGroups.Key, "[]"))
	batches = nil
	ob.checkNodesInReplica(ctx)
	require.Equal(t, [][]int64{{1}, {3}, {4}}, batches)
	require.Equal(t, map[int64]int{1: 1, 2: 1, 3: 1}, source.calls)
}

func TestReplicaObserverDynamicGroupsAndRollback(t *testing.T) {
	ctx := context.Background()
	ob, _ := newGroupObserver(t, "[]")
	ids := []int64{spawnObserved(t, ob, 1), spawnObserved(t, ob, 2), spawnObserved(t, ob, 3)}
	ob.checkNodesInReplica(ctx)
	for _, id := range ids {
		require.Equal(t, 4, ob.meta.Get(ctx, id).RWNodesCount())
	}
	key := paramtable.Get().QueryCoordCfg.CollectionGroups.Key
	require.NoError(t, paramtable.Get().Save(key, observerGroupConfig))
	ob.checkNodesInReplica(ctx)
	for i, quota := range []int{2, 1, 1} {
		require.Equal(t, quota, ob.meta.Get(ctx, ids[i]).RWNodesCount())
	}
	require.NoError(t, paramtable.Get().Save(key, "{"))
	ob.checkNodesInReplica(ctx)
	for i, quota := range []int{2, 1, 1} {
		require.Equal(t, quota, ob.meta.Get(ctx, ids[i]).RWNodesCount())
	}
	require.NoError(t, paramtable.Get().Save(key, `[{"id":"other","collectionIds":["2","3"]}]`))
	ob.checkNodesInReplica(ctx)
	for i, quota := range []int{4, 2, 2} {
		require.Equal(t, quota, ob.meta.Get(ctx, ids[i]).RWNodesCount())
	}
	require.NoError(t, paramtable.Get().Save(key, "[]"))
	ob.checkNodesInReplica(ctx)
	for _, id := range ids {
		require.Equal(t, 4, ob.meta.Get(ctx, id).RWNodesCount())
		require.Zero(t, ob.meta.Get(ctx, id).RONodesCount())
	}
}

func TestReplicaObserverConfigChangesDuringRowFetch(t *testing.T) {
	ctx := context.Background()
	ob, source := newGroupObserver(t, observerGroupConfig)
	for id := int64(1); id <= 3; id++ {
		spawnObserved(t, ob, id)
	}
	source.onFetch = func() {
		require.NoError(t, paramtable.Get().Save(paramtable.Get().QueryCoordCfg.CollectionGroups.Key, "[]"))
	}
	calls := 0
	patch := mockey.Mock((*meta.ReplicaManager).RecoverNodesInCollections).To(func(_ *meta.ReplicaManager, _ context.Context, ids []int64, _ map[string]*meta.ResourceGroup, _ map[int64]meta.CollectionRowCount) error {
		calls++
		require.Len(t, ids, 1)
		return nil
	}).Build()
	defer patch.UnPatch()
	ob.checkNodesInReplica(ctx)
	require.Zero(t, calls)
	select {
	case <-ob.meta.ReplicaRecoveryRequested():
	default:
		t.Fatal("configuration change should request another cycle")
	}
	ob.checkNodesInReplica(ctx)
	require.Equal(t, 3, calls)
}

func TestReplicaObserverRowCacheFailures(t *testing.T) {
	ctx := context.Background()
	ob, source := newGroupObserver(t, observerGroupConfig)
	source.segments[1] = []*datapb.SegmentInfo{{ID: 1, NumOfRows: 10}, {ID: 1, NumOfRows: 10}, {ID: 2, NumOfRows: 20}}
	require.Equal(t, meta.CollectionRowCount{Rows: 30, Valid: true, Fresh: true}, ob.getCollectionRows(ctx, 1))
	require.Equal(t, int64(30), ob.getCollectionRows(ctx, 1).Rows)
	require.Equal(t, 1, source.calls[1])
	expire := func() {
		for id, cache := range ob.rowStats {
			cache.retryAt = time.Time{}
			ob.rowStats[id] = cache
		}
	}
	source.err = merr.WrapErrServiceUnavailableMsg("DataCoord unavailable")
	expire()
	require.Equal(t, meta.CollectionRowCount{Rows: 30, Valid: true}, ob.getCollectionRows(ctx, 1))
	require.False(t, ob.getCollectionRows(ctx, 4).Valid)
	source.err = nil
	for _, segments := range [][]*datapb.SegmentInfo{{{ID: 1, NumOfRows: -1}}, {{ID: 1, NumOfRows: math.MaxInt64}, {ID: 2, NumOfRows: 1}}} {
		source.segments[1] = segments
		expire()
		require.Equal(t, meta.CollectionRowCount{Rows: 30, Valid: true}, ob.getCollectionRows(ctx, 1))
	}
	source.segments[1] = nil
	expire()
	require.Equal(t, meta.CollectionRowCount{Valid: true, Fresh: true}, ob.getCollectionRows(ctx, 1))
	ob.broker = nil
	expire()
	require.False(t, ob.getCollectionRows(ctx, 1).Fresh)
}

func TestReplicaObserverWakeupBeforeCollectionMetadata(t *testing.T) {
	ctx := context.Background()
	ob, _ := newGroupObserver(t, `[{"id":"g","collectionIds":["1","2"]}]`)
	key := paramtable.Get().QueryCoordCfg.CheckNodeInReplicaInterval.Key
	require.NoError(t, paramtable.Get().Save(key, "60"))
	defer paramtable.Get().Reset(key)
	require.NoError(t, ob.Init())
	ob.Start()
	// No CollectionManager entry exists yet. The load path must still wake the
	// observer and allocate nodes without waiting for the 60-second timer.
	for id := int64(1); id <= 2; id++ {
		reps, err := utils.SpawnReplicasWithReplicaConfig(ctx, ob.meta, meta.SpawnWithReplicaConfigParams{CollectionID: id, Configs: []*messagespb.LoadReplicaConfig{{ReplicaId: 100 + id, ResourceGroupName: meta.DefaultResourceGroupName}}})
		require.NoError(t, err)
		require.Len(t, reps, 1)
		require.Eventually(t, func() bool { return ob.meta.Get(ctx, 100+id).RWNodesCount() > 0 }, 5*time.Second, 10*time.Millisecond)
	}
	require.Eventually(t, func() bool {
		return ob.meta.Get(ctx, 101).RWNodesCount() == 3 && ob.meta.Get(ctx, 102).RWNodesCount() == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Empty(t, ob.meta.GetAll(ctx))
	ob.Stop()
}
