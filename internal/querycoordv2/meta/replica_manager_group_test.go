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

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func newGroupTestManager(t *testing.T) *ReplicaManager {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().EtcdCfg
	client, err := etcd.GetEtcdClient(cfg.UseEmbedEtcd.GetAsBool(), cfg.EtcdUseSSL.GetAsBool(),
		cfg.Endpoints.GetAsStrings(), cfg.EtcdTLSCert.GetValue(), cfg.EtcdTLSKey.GetValue(),
		cfg.EtcdTLSCACert.GetValue(), cfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	store := etcdkv.NewEtcdKV(client, "test-replica-batch-"+uuid.NewString())
	t.Cleanup(func() { require.NoError(t, store.RemoveWithPrefix(context.Background(), "")); store.Close() })
	return NewReplicaManager(params.RandomIncrementIDAllocator(), querycoord.NewCatalog(store))
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

func groupRows(values ...int64) map[int64]CollectionRowCount {
	result := make(map[int64]CollectionRowCount)
	for i, rows := range values {
		result[int64(i+1)] = CollectionRowCount{Rows: rows, Valid: true, Fresh: true}
	}
	return result
}

func TestReplicaBatchWeightedAllocationAndSingletonRollback(t *testing.T) {
	ctx := context.Background()
	mgr := newGroupTestManager(t)
	ids := []int64{groupTestSpawn(t, mgr, 1, 1)[0].GetID(), groupTestSpawn(t, mgr, 2, 1)[0].GetID(), groupTestSpawn(t, mgr, 3, 1)[0].GetID()}
	rg := groupTestRG(1, 2, 3, 4, 5, 6, 7, 8)
	rows := groupRows(800, 400, 400)
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, rg, rows))
	before := make([]*querypb.Replica, 0)
	for i, quota := range []int{4, 2, 2} {
		require.Equal(t, quota, mgr.Get(ctx, ids[i]).RWNodesCount())
		before = append(before, proto.Clone(mgr.Get(ctx, ids[i]).replicaPB).(*querypb.Replica))
	}
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, rg, rows))
	for i, id := range ids {
		require.True(t, proto.Equal(before[i], mgr.Get(ctx, id).replicaPB))
	}
	// The manager obeys the supplied batch, even if config lists other members.
	key := paramtable.Get().QueryCoordCfg.CollectionGroups.Key
	require.NoError(t, paramtable.Get().Save(key, `[{"id":"ignored","collectionIds":["1","2","3"]}]`))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1}, rg, nil))
	require.Equal(t, 8, mgr.Get(ctx, ids[0]).RWNodesCount())
	require.Equal(t, 2, mgr.Get(ctx, ids[1]).RWNodesCount())
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{2}, rg, nil))
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{3}, rg, nil))
	for _, id := range ids {
		require.Equal(t, 8, mgr.Get(ctx, id).RWNodesCount())
	}
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, rg, rows))
	for i, quota := range []int{4, 2, 2} {
		require.Equal(t, quota, mgr.Get(ctx, ids[i]).RWNodesCount())
	}
}

func TestReplicaBatchSharingAndDrain(t *testing.T) {
	ctx := context.Background()
	mgr := newGroupTestManager(t)
	first := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	groupTestSpawn(t, mgr, 2, 1)
	groupTestSpawn(t, mgr, 3, 1)
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, groupTestRG(1, 2), groupRows(100, 50, 50)))
	for _, c := range []int64{1, 2, 3} {
		require.Equal(t, 1, mgr.GetByCollection(ctx, c)[0].RWNodesCount())
	}
	// Same-collection replicas remain isolated, including draining nodes.
	extra := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1}, groupTestRG(1, 2, 3, 4), nil))
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, groupTestRG(1, 2), groupRows(100, 50, 50)))
	require.NoError(t, mgr.RemoveNode(ctx, 1, first, mgr.Get(ctx, first).GetRONodes()...))
	require.NoError(t, mgr.RemoveNode(ctx, 1, extra, mgr.Get(ctx, extra).GetRONodes()...))
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, groupTestRG(1, 2), groupRows(100, 50, 50)))
	seen := typeutil.NewUniqueSet()
	for _, r := range mgr.GetByCollection(ctx, 1) {
		for _, node := range r.GetNodes() {
			require.False(t, seen.Contain(node))
			seen.Insert(node)
		}
	}
}

func TestReplicaBatchMissingStatisticsAndRepair(t *testing.T) {
	ctx := context.Background()
	mgr := newGroupTestManager(t)
	a := groupTestSpawn(t, mgr, 1, 1)[0].GetID()
	b := groupTestSpawn(t, mgr, 2, 1)[0].GetID()
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2}, groupTestRG(1, 2), nil))
	require.Zero(t, mgr.Get(ctx, a).RWNodesCount())
	require.Zero(t, mgr.Get(ctx, b).RWNodesCount())
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2}, groupTestRG(1, 2), groupRows(100, 100)))
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2}, groupTestRG(3, 4), nil))
	for _, id := range []int64{a, b} {
		require.Equal(t, 1, mgr.Get(ctx, id).RWNodesCount())
		require.Contains(t, []int64{3, 4}, mgr.Get(ctx, id).GetRWNodes()[0])
	}
}

func TestReplicaBatchLatestChannelRegistration(t *testing.T) {
	ctx := context.Background()
	mgr := newGroupTestManager(t)
	r := groupTestSpawn(t, mgr, 1, 1)[0]
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1}, groupTestRG(1, 2, 3, 4), nil))
	expected := append([]int64(nil), mgr.Get(ctx, r.GetID()).GetRWNodes()...)
	require.NoError(t, mgr.RegisterReplicaChannels(ctx, 1, r.GetID(), []string{"ch"}))
	require.ElementsMatch(t, expected, mgr.Get(ctx, r.GetID()).GetRWNodes())
	noWrite := mockey.Mock(querycoord.Catalog.SaveReplica).Return(merr.WrapErrServiceUnavailableMsg("unexpected duplicate write")).Build()
	require.NoError(t, mgr.RegisterReplicaChannels(ctx, 1, r.GetID(), []string{"ch"}))
	noWrite.UnPatch()
	require.NoError(t, mgr.RemoveCollection(ctx, 1))
	require.ErrorIs(t, mgr.RegisterReplicaChannels(ctx, 1, r.GetID(), []string{"ch"}), merr.ErrReplicaNotFound)
}

func TestReplicaBatchPartialPersistenceRecovery(t *testing.T) {
	ctx := context.Background()
	mgr := newGroupTestManager(t)
	for _, id := range []int64{1, 2, 3} {
		groupTestSpawn(t, mgr, id, 1)
	}
	writes := 0
	var original func(querycoord.Catalog, context.Context, ...*querypb.Replica) error
	patch := mockey.Mock(querycoord.Catalog.SaveReplica).Origin(&original).To(func(c querycoord.Catalog, ctx context.Context, reps ...*querypb.Replica) error {
		writes++
		if writes == 2 {
			return merr.WrapErrServiceUnavailableMsg("second write failed")
		}
		return original(c, ctx, reps...)
	}).Build()
	rg := groupTestRG(1, 2, 3)
	require.Error(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, rg, groupRows(100, 100, 100)))
	patch.UnPatch()
	restarted := NewReplicaManager(params.RandomIncrementIDAllocator(), mgr.catalog)
	require.NoError(t, restarted.Recover(ctx, []int64{1, 2, 3}))
	require.NoError(t, restarted.RecoverNodesInCollections(ctx, []int64{1, 2, 3}, rg, groupRows(100, 100, 100)))
	for _, id := range []int64{1, 2, 3} {
		require.Equal(t, 1, restarted.GetByCollection(ctx, id)[0].RWNodesCount())
	}
}

func TestReplicaBatchWaitRGAndDisabledStopping(t *testing.T) {
	ctx := context.Background()
	mgr := newGroupTestManager(t)
	reps, err := mgr.Spawn(ctx, 1, map[string]int{"rg": 1}, nil, commonpb.LoadPriority_HIGH, WithNeedWaitRGReady())
	require.NoError(t, err)
	rg := groupTestRG(1, 2)
	rg["rg"].cfg = newResourceGroupConfig(4, 4)
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1}, rg, nil))
	require.Zero(t, mgr.Get(ctx, reps[0].GetID()).RWNodesCount())
	rg = groupTestRG(1, 2, 3, 4)
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1}, rg, nil))
	require.Equal(t, 4, mgr.Get(ctx, reps[0].GetID()).RWNodesCount())
	key := paramtable.Get().QueryCoordCfg.EnableStoppingBalance.Key
	require.NoError(t, paramtable.Get().Save(key, "false"))
	t.Cleanup(func() { paramtable.Get().Reset(key) })
	groupTestSpawn(t, mgr, 2, 1)
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1, 2}, rg, groupRows(100, 100)))
	require.Equal(t, 4, mgr.Get(ctx, reps[0].GetID()).RWNodesCount())
}

func TestReplicaBatchConcurrentAllocation(t *testing.T) {
	ctx := context.Background()
	mgr := newGroupTestManager(t)
	groupTestSpawn(t, mgr, 1, 2)
	groupTestSpawn(t, mgr, 2, 2)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{2, 1}, groupTestRG(1, 2, 3, 4), groupRows(10, 20)))
		}()
	}
	wg.Wait()
	for _, id := range []int64{1, 2} {
		seen := typeutil.NewUniqueSet()
		for _, r := range mgr.GetByCollection(ctx, id) {
			for _, n := range r.GetNodes() {
				require.False(t, seen.Contain(n))
				seen.Insert(n)
			}
		}
	}
}

func TestReplicaBatchInvalidInput(t *testing.T) {
	ctx := context.Background()
	mgr := newGroupTestManager(t)
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, nil, nil, nil))
	require.Error(t, mgr.RecoverNodesInCollections(ctx, []int64{999}, groupTestRG(1), nil))
	groupTestSpawn(t, mgr, 1, 1)
	require.Error(t, mgr.RecoverNodesInCollections(ctx, []int64{1}, nil, nil))
	require.NoError(t, mgr.RecoverNodesInCollections(ctx, []int64{1}, map[string]*ResourceGroup{"rg": nil}, nil))
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
		stats := make(map[int64]CollectionRowCount)
		id := int64(0)
		for c := int64(1); c <= int64(1+random.Intn(8)); c++ {
			stats[c] = CollectionRowCount{Valid: true, Fresh: true, Rows: random.Int63n(1000)}
			for r := 0; r < 1+random.Intn(4); r++ {
				id++
				replica := NewReplicaWithPriority(&querypb.Replica{ID: id, CollectionID: c, ResourceGroup: "rg"}, commonpb.LoadPriority_HIGH)
				all[c] = append(all[c], replica)
			}
		}
		key := "rg"
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
