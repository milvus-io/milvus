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

package meta

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const groupTestConfig = `[{"id":"g","collectionIds":["1","2","3","4"]}]`

type groupTestSource struct {
	mu       sync.Mutex
	segments map[int64][]*datapb.SegmentInfo
	calls    map[int64]int
	err      error
}

func newGroupTestManager(t *testing.T, config string, rows map[int64]int64) (*ReplicaManager, *groupTestSource) {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().EtcdCfg
	client, err := etcd.GetEtcdClient(cfg.UseEmbedEtcd.GetAsBool(), cfg.EtcdUseSSL.GetAsBool(),
		cfg.Endpoints.GetAsStrings(), cfg.EtcdTLSCert.GetValue(), cfg.EtcdTLSKey.GetValue(),
		cfg.EtcdTLSCACert.GetValue(), cfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	store := etcdkv.NewEtcdKV(client, "test-replica-group-"+uuid.NewString())
	t.Cleanup(func() {
		require.NoError(t, store.RemoveWithPrefix(context.Background(), ""))
		store.Close()
	})
	mgr := NewReplicaManager(params.RandomIncrementIDAllocator(), querycoord.NewCatalog(store))
	source := &groupTestSource{segments: make(map[int64][]*datapb.SegmentInfo), calls: make(map[int64]int)}
	for id, count := range rows {
		source.segments[id] = []*datapb.SegmentInfo{{ID: id, NumOfRows: count}}
	}
	patch := mockey.Mock((*CoordinatorBroker).GetRecoveryInfoV2).To(
		func(_ *CoordinatorBroker, _ context.Context, id int64, _ ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
			source.mu.Lock()
			defer source.mu.Unlock()
			source.calls[id]++
			return nil, source.segments[id], source.err
		}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	broker := NewCoordinatorBroker(nil)
	require.NoError(t, mgr.InitCollectionGroups(config, broker.GetRecoveryInfoV2))
	return mgr, source
}

func groupTestSpawn(t *testing.T, mgr *ReplicaManager, collectionID int64, count int) []*Replica {
	t.Helper()
	replicas, err := mgr.Spawn(context.Background(), collectionID, map[string]int{"rg": count}, []string{"ch"}, commonpb.LoadPriority_HIGH)
	require.NoError(t, err)
	return replicas
}

func groupTestRG(nodes ...int64) map[string]*ResourceGroup {
	return map[string]*ResourceGroup{"rg": newTestResourceGroup("rg", typeutil.NewUniqueSet(nodes...))}
}

func expireGroupRows(m *ReplicaManager) {
	m.rowStatsMu.Lock()
	defer m.rowStatsMu.Unlock()
	for id, stats := range m.rowStats {
		stats.retryAt = time.Time{}
		m.rowStats[id] = stats
	}
}

func TestReplicaGroupConfiguration(t *testing.T) {
	for _, config := range []string{
		"{", `[{"id":""}]`, `[{"id":"g"},{"id":"g"}]`,
		`[{"id":"g","collectionIds":["0"]}]`, `[{"id":"g","collectionIds":["9223372036854775808"]}]`,
		`[{"id":"a","collectionIds":["1"]},{"id":"b","collectionIds":["1"]}]`,
	} {
		mgr := NewReplicaManager(nil, nil)
		require.ErrorIs(t, mgr.InitCollectionGroups(config, nil), merr.ErrParameterInvalid)
	}
	mgr := NewReplicaManager(nil, nil)
	require.NoError(t, mgr.InitCollectionGroups(groupTestConfig, nil))
	require.Equal(t, "g", mgr.collectionGroups[1])
	require.NoError(t, mgr.InitCollectionGroups("[]", nil))
	require.Empty(t, mgr.collectionGroups)
}

func TestReplicaGroupRecoveryUsesCurrentConfiguration(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, "[]", nil)
	old := groupTestSpawn(t, mgr, 1, 1)[0]
	// Every recovered replica follows the current configuration, regardless of creation time.
	next := NewReplicaManager(params.RandomIncrementIDAllocator(), mgr.catalog)
	require.NoError(t, next.InitCollectionGroups(groupTestConfig, mgr.recoveryInfo))
	require.NoError(t, next.Recover(ctx, []int64{1}))
	require.Equal(t, "g", next.Get(ctx, old.GetID()).GetCollectionGroupID())
	id, err := next.AllocateReplicaID(ctx)
	require.NoError(t, err)
	reps, err := next.SpawnWithReplicaConfig(ctx, SpawnWithReplicaConfigParams{
		CollectionID: 1, Channels: []string{"ch"}, Configs: []*messagespb.LoadReplicaConfig{
			{ReplicaId: old.GetID(), ResourceGroupName: "rg"}, {ReplicaId: id, ResourceGroupName: "rg"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "g", reps[0].GetCollectionGroupID())
	require.Equal(t, "g", reps[1].GetCollectionGroupID())
	restarted := NewReplicaManager(params.RandomIncrementIDAllocator(), mgr.catalog)
	require.NoError(t, restarted.InitCollectionGroups("[]", mgr.recoveryInfo))
	require.NoError(t, restarted.Recover(ctx, []int64{1}))
	require.Empty(t, restarted.Get(ctx, id).GetCollectionGroupID())
	require.Empty(t, restarted.Get(ctx, old.GetID()).GetCollectionGroupID())
	require.Empty(t, groupTestSpawn(t, restarted, 2, 1)[0].GetCollectionGroupID())
	require.NoError(t, restarted.RemoveCollection(ctx, 1))
	require.Empty(t, groupTestSpawn(t, restarted, 1, 1)[0].GetCollectionGroupID())
}

func TestReplicaGroupWeightedRecoveryAndCache(t *testing.T) {
	ctx := context.Background()
	mgr, source := newGroupTestManager(t, groupTestConfig, map[int64]int64{1: 800, 2: 400, 3: 400})
	ids := []int64{groupTestSpawn(t, mgr, 1, 1)[0].GetID(), groupTestSpawn(t, mgr, 2, 1)[0].GetID(), groupTestSpawn(t, mgr, 3, 1)[0].GetID()}
	rg := groupTestRG(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
	require.Equal(t, 8, mgr.Get(ctx, ids[0]).RWNodesCount())
	require.Equal(t, 4, mgr.Get(ctx, ids[1]).RWNodesCount())
	require.Equal(t, 4, mgr.Get(ctx, ids[2]).RWNodesCount())
	before := make([]*querypb.Replica, 0)
	for _, id := range ids {
		before = append(before, proto.Clone(mgr.Get(ctx, id).replicaPB).(*querypb.Replica))
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, rg))
	}
	for i, id := range ids {
		require.True(t, proto.Equal(before[i], mgr.Get(ctx, id).replicaPB))
	}
	require.Equal(t, map[int64]int{1: 1, 2: 1, 3: 1}, source.calls)
	source.segments[2] = []*datapb.SegmentInfo{{ID: 2, NumOfRows: 800}}
	expireGroupRows(mgr)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 2, rg))
	require.Greater(t, mgr.Get(ctx, ids[1]).RWNodesCount(), 4)
	require.Greater(t, mgr.Get(ctx, ids[0]).RONodesCount(), 0)
	// Existing observer drains RO; the same recovery entry converges afterwards.
	for _, id := range ids {
		r := mgr.Get(ctx, id)
		require.NoError(t, mgr.RemoveNode(ctx, r.GetCollectionID(), id, r.GetRONodes()...))
	}
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
}

func TestReplicaGroupSharingAndReplicaIsolation(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, groupTestConfig, map[int64]int64{1: 100, 2: 50, 3: 50})
	a := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	b := groupTestSpawn(t, mgr, 2, 1)[0].GetID()
	c := groupTestSpawn(t, mgr, 3, 1)[0].GetID()
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(1, 2)))
	require.Equal(t, 1, mgr.Get(ctx, a).RWNodesCount())
	require.Equal(t, mgr.Get(ctx, b).GetRWNodes(), mgr.Get(ctx, c).GetRWNodes())
	require.NotEqual(t, mgr.Get(ctx, a).GetRWNodes(), mgr.Get(ctx, b).GetRWNodes())
	extra := groupTestSpawn(t, mgr, 1, 2)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(1, 2)))
	seen := typeutil.NewUniqueSet()
	for _, r := range mgr.GetByCollection(ctx, 1) {
		for _, node := range append(append([]int64(nil), r.GetRWNodes()...), r.GetRONodes()...) {
			require.False(t, seen.Contain(node))
			seen.Insert(node)
		}
	}
	require.Less(t, mgr.Get(ctx, extra[0].GetID()).RWNodesCount()+mgr.Get(ctx, extra[1].GetID()).RWNodesCount(), 2)
}

func TestReplicaGroupExistingReplicasAndDrain(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, "[]", map[int64]int64{1: 900, 2: 100})
	old := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	rg := groupTestRG(1, 2, 3, 4, 5, 6, 7, 8)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
	require.NoError(t, mgr.UpdateCollectionGroups(ctx, groupTestConfig))
	added := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	groupTestSpawn(t, mgr, 2, 1)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
	require.Equal(t, 4, mgr.Get(ctx, old).RWNodesCount())
	require.Equal(t, 4, mgr.Get(ctx, old).RONodesCount())
	require.Zero(t, mgr.Get(ctx, added).RWNodesCount())
	require.NoError(t, mgr.RemoveNode(ctx, 1, old, mgr.Get(ctx, old).GetRONodes()...))
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 2, rg))
	require.Equal(t, 4, mgr.Get(ctx, old).RWNodesCount())
	require.Equal(t, 4, mgr.Get(ctx, added).RWNodesCount())
	for _, node := range mgr.Get(ctx, added).GetRWNodes() {
		require.False(t, mgr.Get(ctx, old).Contains(node))
	}
}

func TestReplicaGroupRowFailureAndRecovery(t *testing.T) {
	ctx := context.Background()
	mgr, source := newGroupTestManager(t, groupTestConfig, map[int64]int64{1: 10})
	r := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	source.err = merr.WrapErrServiceUnavailableMsg("test outage")
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(1, 2)))
	require.Zero(t, mgr.Get(ctx, r).RWNodesCount())
	require.False(t, mgr.rowStats[1].valid)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(1, 2)))
	require.Equal(t, 1, source.calls[1])
	source.err = nil
	source.segments[1] = []*datapb.SegmentInfo{{ID: 1, NumOfRows: 10}, {ID: 1, NumOfRows: 10}, {ID: 2, NumOfRows: 20}}
	expireGroupRows(mgr)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(1, 2)))
	require.Equal(t, int64(30), mgr.rowStats[1].rows)
	source.err = merr.WrapErrServiceUnavailableMsg("outage again")
	expireGroupRows(mgr)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(2, 3)))
	require.ElementsMatch(t, []int64{2, 3}, mgr.Get(ctx, r).GetRWNodes())
	require.ElementsMatch(t, []int64{1}, mgr.Get(ctx, r).GetRONodes())
	require.Equal(t, int64(30), mgr.rowStats[1].rows)
	source.err = nil
	source.segments[1] = []*datapb.SegmentInfo{{ID: 1, NumOfRows: math.MaxInt64}, {ID: 2, NumOfRows: 1}}
	expireGroupRows(mgr)
	require.Error(t, mgr.getCollectionRows(ctx, 1).err)
	require.Equal(t, int64(30), mgr.rowStats[1].rows)
	source.segments[1] = nil
	expireGroupRows(mgr)
	require.Equal(t, int64(0), mgr.getCollectionRows(ctx, 1).rows)
	require.True(t, mgr.rowStats[1].valid)
}

func TestReplicaGroupStaleObserverPut(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, groupTestConfig, map[int64]int64{1: 10})
	r := groupTestSpawn(t, mgr, 1, 1)[0]
	stale := r.CopyForWrite()
	stale.TryEnableChannelExclusiveMode("ch")
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(1, 2, 3, 4)))
	expected := mgr.Get(ctx, r.GetID()).GetRWNodes()
	snapshot := stale.IntoReplica()
	noWrite := mockey.Mock(querycoord.Catalog.SaveReplica).Return(merr.WrapErrServiceUnavailableMsg("unexpected duplicate write")).Build()
	require.NoError(t, mgr.Put(ctx, snapshot))
	noWrite.UnPatch()
	require.ElementsMatch(t, expected, mgr.Get(ctx, r.GetID()).GetRWNodes())
	require.Equal(t, "g", mgr.Get(ctx, r.GetID()).GetCollectionGroupID())
	require.NoError(t, mgr.RemoveCollection(ctx, 1))
	require.Error(t, mgr.Put(ctx, snapshot))
}

func TestReplicaGroupQuotas(t *testing.T) {
	for _, tc := range []struct {
		rows  []int64
		nodes int
		want  []int
	}{
		{[]int64{800, 400, 400}, 16, []int{8, 4, 4}},
		{[]int64{100, 50, 50}, 2, []int{1, 1, 1}},
		{[]int64{math.MaxInt64, math.MaxInt64}, 10, []int{5, 5}},
		{[]int64{0, 0}, 3, []int{2, 1}},
		{[]int64{100, 0}, 4, []int{4, 1}},
	} {
		members := make([]*groupReplicaAssignment, 0)
		for i, rows := range tc.rows {
			r := NewReplicaWithPriority(&querypb.Replica{ID: int64(i + 1)}, commonpb.LoadPriority_HIGH)
			members = append(members, &groupReplicaAssignment{replica: r, rows: rows})
		}
		assignGroupQuotas(members, tc.nodes)
		for i, m := range members {
			require.Equal(t, tc.want[i], m.quota)
		}
	}
	assignGroupQuotas(nil, 10)
}

func TestReplicaGroupPlannerProperties(t *testing.T) {
	paramtable.Init()
	random := rand.New(rand.NewSource(53463))
	for trial := 0; trial < 100; trial++ {
		nodes := typeutil.NewUniqueSet()
		for n := 1; n <= 1+random.Intn(12); n++ {
			nodes.Insert(int64(n))
		}
		all := make(map[int64][]*Replica)
		stats := make(map[int64]collectionRowStats)
		id := int64(0)
		for c := int64(1); c <= int64(1+random.Intn(8)); c++ {
			stats[c] = collectionRowStats{valid: true, rows: random.Int63n(1000)}
			for r := 0; r < 1+random.Intn(4); r++ {
				id++
				replica := NewReplicaWithPriority(&querypb.Replica{ID: id, CollectionID: c, ResourceGroup: "rg"}, commonpb.LoadPriority_HIGH)
				replica.collectionGroupID = "g"
				all[c] = append(all[c], replica)
			}
		}
		key := replicaGroupKey{"g", "rg"}
		planned := planReplicaGroup(key, all, nodes, stats, nil)
		again := planReplicaGroup(key, all, nodes, stats, nil)
		for _, replicas := range all {
			occupied := typeutil.NewUniqueSet()
			for _, r := range replicas {
				require.Equal(t, planned[r.GetID()], again[r.GetID()])
				for node := range planned[r.GetID()] {
					require.True(t, nodes.Contain(node))
					require.False(t, occupied.Contain(node))
					occupied.Insert(node)
				}
			}
		}
	}
}

func TestReplicaGroupAugmentMatching(t *testing.T) {
	makeMember := func(id int64, available ...int64) *groupReplicaAssignment {
		return &groupReplicaAssignment{
			replica: NewReplicaWithPriority(&querypb.Replica{ID: id, CollectionID: 1}, commonpb.LoadPriority_HIGH),
			rows:    10, quota: 1, available: typeutil.NewUniqueSet(available...), desired: typeutil.NewUniqueSet(),
		}
	}
	a, b := makeMember(1, 1, 2), makeMember(2, 1)
	projected := make(map[int64]float64)
	claimed := map[int64]typeutil.UniqueSet{1: typeutil.NewUniqueSet()}
	require.True(t, placeGroupNode(a, []*groupReplicaAssignment{a, b}, projected, claimed, typeutil.NewUniqueSet()))
	require.True(t, placeGroupNode(b, []*groupReplicaAssignment{a, b}, projected, claimed, typeutil.NewUniqueSet()))
	require.Equal(t, typeutil.NewUniqueSet(2), a.desired)
	require.Equal(t, typeutil.NewUniqueSet(1), b.desired)
}

func TestReplicaGroupPartialPersistenceRecovery(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, groupTestConfig, map[int64]int64{1: 100, 2: 100, 3: 100})
	for _, id := range []int64{1, 2, 3} {
		groupTestSpawn(t, mgr, id, 1)
	}
	writes := 0
	var original func(querycoord.Catalog, context.Context, ...*querypb.Replica) error
	patch := mockey.Mock(querycoord.Catalog.SaveReplica).Origin(&original).To(
		func(c querycoord.Catalog, ctx context.Context, replicas ...*querypb.Replica) error {
			writes++
			if writes == 2 {
				return merr.WrapErrServiceUnavailableMsg("test second write failed")
			}
			return original(c, ctx, replicas...)
		}).Build()
	rg := groupTestRG(1, 2, 3)
	require.Error(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
	patch.UnPatch()
	restarted := NewReplicaManager(params.RandomIncrementIDAllocator(), mgr.catalog)
	require.NoError(t, restarted.InitCollectionGroups(groupTestConfig, mgr.recoveryInfo))
	require.NoError(t, restarted.Recover(ctx, []int64{1, 2, 3}))
	require.NoError(t, restarted.RecoverNodesInCollection(ctx, 2, rg))
	for _, id := range []int64{1, 2, 3} {
		require.Equal(t, 1, restarted.GetByCollection(ctx, id)[0].RWNodesCount())
		require.Equal(t, "g", restarted.GetByCollection(ctx, id)[0].GetCollectionGroupID())
	}
}

func TestReplicaGroupWaitRGAndDisabledStopping(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, groupTestConfig, map[int64]int64{1: 100, 2: 100})
	reps, err := mgr.Spawn(ctx, 1, map[string]int{"rg": 1}, []string{"ch"}, commonpb.LoadPriority_HIGH, WithNeedWaitRGReady())
	require.NoError(t, err)
	id := reps[0].GetID()
	rg := groupTestRG(1, 2)
	rg["rg"].cfg = newResourceGroupConfig(4, 4)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
	require.Zero(t, mgr.Get(ctx, id).RWNodesCount())
	rg = groupTestRG(1, 2, 3, 4)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
	require.Equal(t, 4, mgr.Get(ctx, id).RWNodesCount())
	key := paramtable.Get().QueryCoordCfg.EnableStoppingBalance.Key
	old := paramtable.Get().QueryCoordCfg.EnableStoppingBalance.GetValue()
	require.NoError(t, paramtable.Get().Save(key, "false"))
	t.Cleanup(func() { require.NoError(t, paramtable.Get().Save(key, old)) })
	groupTestSpawn(t, mgr, 2, 1)
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 2, rg))
	require.Equal(t, 4, mgr.Get(ctx, id).RWNodesCount())
	require.Zero(t, mgr.Get(ctx, id).RONodesCount())
}

func TestReplicaGroupConcurrentRecoverAndCache(t *testing.T) {
	ctx := context.Background()
	mgr, source := newGroupTestManager(t, groupTestConfig, map[int64]int64{1: 10, 2: 20})
	for _, id := range []int64{1, 2} {
		groupTestSpawn(t, mgr, id, 2)
	}
	rg := groupTestRG(1, 2, 3, 4)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := mgr.RecoverNodesInCollection(ctx, 1, rg)
			if err != nil {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			}
		}()
	}
	wg.Wait()
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 2, rg))
	source.mu.Lock()
	require.Equal(t, map[int64]int{1: 1, 2: 1}, source.calls)
	source.mu.Unlock()
	for _, id := range []int64{1, 2} {
		seen := typeutil.NewUniqueSet()
		for _, r := range mgr.GetByCollection(ctx, id) {
			for _, node := range r.GetNodes() {
				require.False(t, seen.Contain(node))
				seen.Insert(node)
			}
		}
	}
	require.NoError(t, mgr.RemoveCollection(ctx, 1))
	require.NotContains(t, mgr.rowStats, int64(1))
}

func TestReplicaGroupRecoveryInvalidInputs(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, groupTestConfig, nil)
	require.Error(t, mgr.RecoverNodesInCollection(ctx, 999, groupTestRG(1)))
	groupTestSpawn(t, mgr, 1, 1)
	require.Error(t, mgr.RecoverNodesInCollection(ctx, 1, map[string]*ResourceGroup{
		"rg":    newTestResourceGroup("rg", typeutil.NewUniqueSet(1)),
		"other": newTestResourceGroup("other", typeutil.NewUniqueSet(1)),
	}))
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, map[string]*ResourceGroup{"rg": nil}))
	mgr.recoveryInfo = nil
	expireGroupRows(mgr)
	require.Error(t, mgr.getCollectionRows(ctx, 1).err)
}

// Configuration changes must alter all existing replicas without rewriting their
// persisted node ownership, and must not be undone by a stale observer snapshot.
func TestReplicaGroupDynamicConfiguration(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, "[]", map[int64]int64{1: 200, 2: 100, 3: 100})
	ids := []int64{groupTestSpawn(t, mgr, 1, 1)[0].GetID(), groupTestSpawn(t, mgr, 2, 1)[0].GetID(), groupTestSpawn(t, mgr, 3, 1)[0].GetID()}
	beforeGrouping := mgr.Get(ctx, ids[0]).CopyForWrite().IntoReplica()
	rg := groupTestRG(1, 2, 3, 4)
	for _, c := range []int64{1, 2, 3} {
		require.NoError(t, mgr.RecoverNodesInCollection(ctx, c, rg))
	}
	before, err := mgr.catalog.GetReplicas(ctx)
	require.NoError(t, err)
	require.NoError(t, mgr.UpdateCollectionGroups(ctx, groupTestConfig))
	after, err := mgr.catalog.GetReplicas(ctx)
	require.NoError(t, err)
	for i := range before {
		require.True(t, proto.Equal(before[i], after[i]))
	}
	for _, id := range ids {
		require.Equal(t, "g", mgr.Get(ctx, id).GetCollectionGroupID())
	}
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
	for i, quota := range []int{2, 1, 1} {
		require.Equal(t, quota, mgr.Get(ctx, ids[i]).RWNodesCount())
	}
	version := mgr.groupVersion
	require.NoError(t, mgr.UpdateCollectionGroups(ctx, groupTestConfig))
	require.Equal(t, version, mgr.groupVersion)
	require.Error(t, mgr.UpdateCollectionGroups(ctx, "{"))
	require.Equal(t, version, mgr.groupVersion)
	stale := mgr.Get(ctx, ids[0]).CopyForWrite().IntoReplica()
	// Move an existing collection to another group; removing the other members
	// restores per-collection allocation on the next normal observer cycle.
	require.NoError(t, mgr.UpdateCollectionGroups(ctx, `[{"id":"other","collectionIds":["1"]}]`))
	require.Equal(t, "other", mgr.Get(ctx, ids[0]).GetCollectionGroupID())
	require.Empty(t, mgr.Get(ctx, ids[1]).GetCollectionGroupID())
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, rg))
	for _, id := range ids {
		require.Equal(t, 4, mgr.Get(ctx, id).RWNodesCount())
		require.Zero(t, mgr.Get(ctx, id).RONodesCount())
	}
	require.NoError(t, mgr.UpdateCollectionGroups(ctx, "[]"))
	require.NoError(t, mgr.Put(ctx, stale))
	// An observer may still hold an ungrouped snapshot from before enablement.
	// A full enable/disable cycle must not make that snapshot current again.
	require.NoError(t, mgr.Put(ctx, beforeGrouping))
	require.Empty(t, mgr.Get(ctx, ids[0]).GetCollectionGroupID())
	require.Equal(t, 4, mgr.Get(ctx, ids[0]).RWNodesCount())
	// Re-enabling applies to the same replica IDs, without release/reload.
	require.NoError(t, mgr.UpdateCollectionGroups(ctx, groupTestConfig))
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, rg))
	require.Equal(t, 2, mgr.Get(ctx, ids[0]).RWNodesCount())
}

func TestReplicaGroupConfigChangesDuringRowFetch(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, groupTestConfig, nil)
	id := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	started, resume := make(chan struct{}), make(chan struct{})
	mgr.recoveryInfo = func(context.Context, int64, ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
		close(started)
		<-resume
		return nil, []*datapb.SegmentInfo{{ID: 1, NumOfRows: 100}}, nil
	}
	result := make(chan error, 1)
	go func() { result <- mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(1, 2)) }()
	<-started
	// Disabling does not wait for DataCoord, and invalidates the in-flight plan.
	require.NoError(t, mgr.UpdateCollectionGroups(ctx, "[]"))
	close(resume)
	require.ErrorIs(t, <-result, merr.ErrServiceUnavailable)
	require.Empty(t, mgr.Get(ctx, id).GetCollectionGroupID())
	require.Zero(t, mgr.Get(ctx, id).RWNodesCount())
	require.NoError(t, mgr.RecoverNodesInCollection(ctx, 1, groupTestRG(1, 2)))
	require.Equal(t, 2, mgr.Get(ctx, id).RWNodesCount())
}

func TestReplicaGroupRefreshCurrentConfig(t *testing.T) {
	ctx := context.Background()
	mgr, _ := newGroupTestManager(t, "[]", nil)
	id := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	cfg := paramtable.Get()
	key := cfg.QueryCoordCfg.CollectionGroups.Key
	t.Cleanup(func() { cfg.Reset(key) })
	require.NoError(t, cfg.Save(key, groupTestConfig))
	require.NoError(t, mgr.RefreshCollectionGroups(ctx))
	require.Equal(t, "g", mgr.Get(ctx, id).GetCollectionGroupID())
	require.NoError(t, cfg.Save(key, "[]"))
	require.NoError(t, mgr.RefreshCollectionGroups(ctx))
	require.Empty(t, mgr.Get(ctx, id).GetCollectionGroupID())
}
