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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func groupReplica(id, collection int64, rg string, rw ...int64) *Replica {
	return NewReplica(&querypb.Replica{ID: id, CollectionID: collection, ResourceGroup: rg, Nodes: rw})
}

func TestCollectionGroupConfig(t *testing.T) {
	for _, tc := range []struct {
		name, config   string
		allow          []string
		valid, enabled bool
	}{
		{"empty", `{}`, nil, true, false},
		{"disabled", `{"g":[1,2]}`, nil, true, false},
		{"exact", `{"g":[2,1]}`, []string{"rg"}, true, true},
		{"all", `{"g":[1,2]}`, []string{"*"}, true, true},
		{"miss", `{"g":[1,2]}`, []string{"r"}, true, false},
		{"invalid json", `oops`, nil, false, false},
		{"duplicate", `{"g":[1],"h":[1]}`, nil, false, false},
		{"duplicate within", `{"g":[1,1]}`, nil, false, false},
		{"empty name", `{" ":[1]}`, nil, false, false},
		{"empty members", `{"g":[]}`, nil, false, false},
		{"invalid id", `{"g":[0]}`, nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, err := newReplicaPlacementPolicy(tc.config, tc.allow)
			if !tc.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.enabled, a.matches("rg"))
			require.False(t, a.enabled(nil))
			if len(a.groups) > 0 {
				require.Equal(t, []int64{1, 2}, a.groups[1].members)
			}
		})
	}
}

func TestCollectionGroupAssignment(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas []*Replica
		nodes    []int64
		rows     map[int64]int64
		counts   []int
	}{
		{"weighted", []*Replica{groupReplica(1, 10, "rg"), groupReplica(2, 20, "rg")}, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, map[int64]int64{10: 900, 20: 100}, []int{9, 1}},
		{"different replica counts", []*Replica{groupReplica(1, 10, "rg"), groupReplica(2, 10, "rg"), groupReplica(3, 20, "rg")}, []int64{1, 2, 3, 4, 5, 6}, map[int64]int64{10: 100, 20: 100}, []int{2, 2, 2}},
		{"oversubscribed", []*Replica{groupReplica(1, 10, "rg"), groupReplica(2, 10, "rg"), groupReplica(3, 20, "rg"), groupReplica(4, 30, "rg")}, []int64{1, 2}, map[int64]int64{10: 100, 20: 10, 30: 20}, []int{1, 1, 1, 1}},
		{"empty data", []*Replica{groupReplica(1, 10, "rg"), groupReplica(2, 20, "rg")}, []int64{1, 2, 3}, nil, []int{2, 1}},
		{"no nodes", []*Replica{groupReplica(1, 10, "rg")}, nil, nil, []int{0}},
		{"same collection capacity", []*Replica{groupReplica(1, 10, "rg"), groupReplica(2, 10, "rg")}, []int64{1}, nil, []int{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := assignCollectionGroup(tc.replicas, tc.nodes, tc.rows)
			owners := make(map[int64]typeutil.Set[int64])
			for i, r := range tc.replicas {
				require.Len(t, plan[r.GetID()], tc.counts[i])
				if owners[r.GetCollectionID()] == nil {
					owners[r.GetCollectionID()] = typeutil.NewSet[int64]()
				}
				for _, node := range plan[r.GetID()] {
					require.False(t, owners[r.GetCollectionID()].Contain(node))
					owners[r.GetCollectionID()].Insert(node)
				}
			}
			for range 30 {
				require.Equal(t, plan, assignCollectionGroup(tc.replicas, tc.nodes, tc.rows))
			}
		})
	}
	// Enable on existing replicas that both own the entire RG: spread their remaining nodes.
	rs := []*Replica{groupReplica(1, 10, "rg", 1, 2, 3, 4), groupReplica(2, 20, "rg", 1, 2, 3, 4)}
	plan := assignCollectionGroup(rs, []int64{1, 2, 3, 4}, map[int64]int64{10: 1, 20: 1})
	require.Equal(t, map[int64][]int64{1: {1, 2}, 2: {3, 4}}, plan)
	// Restart recomputation retains every legal RW when the input and quotas are unchanged.
	for _, r := range rs {
		r.replicaPB.Nodes = plan[r.GetID()]
	}
	require.Equal(t, plan, assignCollectionGroup(rs, []int64{1, 2, 3, 4}, map[int64]int64{10: 1, 20: 1}))
}

func TestCollectionGroupTransitions(t *testing.T) {
	a := groupReplica(1, 10, "new", 3, 1)
	b := groupReplica(2, 10, "old", 4)
	b.replicaPB.RoNodes = []int64{2}
	b = NewReplica(b.replicaPB)
	siblings := []*Replica{a, b}
	updated := applyCollectionGroupPlan(a, siblings, []int64{1, 2, 3}, map[int64][]int64{1: {2}})
	require.Equal(t, []int64{1}, updated.GetRWNodes()) // last serving RW retained
	require.Equal(t, []int64{3, 1}, a.GetRWNodes())    // immutable source, even when sorted
	require.ElementsMatch(t, []int64{3}, updated.GetRONodes())
	require.Nil(t, applyCollectionGroupPlan(updated, []*Replica{updated, b}, []int64{1, 2, 3}, map[int64][]int64{1: {2}}))
	// Only after the old RG's RO has drained can its node enter this replica.
	b = groupReplica(2, 10, "old", 4)
	next := applyCollectionGroupPlan(updated, []*Replica{updated, b}, []int64{1, 2, 3}, map[int64][]int64{1: {2}})
	require.ElementsMatch(t, []int64{2}, next.GetRWNodes())
	require.ElementsMatch(t, []int64{1, 3}, next.GetRONodes())
	// Fault recovery obtains a free node even if the preferred node is blocked.
	b = groupReplica(2, 10, "old", 2)
	next = applyCollectionGroupPlan(a, []*Replica{a, b}, []int64{2, 5}, map[int64][]int64{1: {2}})
	require.Equal(t, []int64{5}, next.GetRWNodes())
	require.ElementsMatch(t, []int64{1, 3}, next.GetRONodes())
	// RO can be promoted back when it belongs to this replica.
	require.Equal(t, []int64{3}, applyCollectionGroupPlan(updated, []*Replica{updated}, []int64{3}, map[int64][]int64{1: {3}}).GetRWNodes())
}

type groupTestState struct {
	failAfterSave bool
	duringRows    func()
	duringSave    func()
	m             *Meta
	writes, calls int
	failWrite     int
	failRows      bool
	rows          map[int64]int64
	level         datapb.SegmentLevel
	persisted     map[int64]*querypb.Replica
}

// Runtime mocks copy protobuf values inside callbacks, never retain stack pointers.
func newGroupTestState(t *testing.T, allow []string) *groupTestState {
	paramtable.Init()
	catalog := querycoord.NewCatalog(nil)
	s := &groupTestState{m: NewMeta(nil, catalog, nil), rows: map[int64]int64{10: 300, 20: 100}, level: datapb.SegmentLevel_L1, persisted: make(map[int64]*querypb.Replica)}
	cfg := &paramtable.Get().QueryCoordCfg
	for _, key := range []string{cfg.ReplicaPlacementCollectionGroups.Key, cfg.ReplicaPlacementResourceGroupAllowlist.Key} {
		t.Cleanup(func() { paramtable.Get().Reset(key) })
	}
	require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementCollectionGroups.Key, `{"g":[10,20]}`))
	require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, strings.Join(allow, ",")))
	s.m.Broker = &CoordinatorBroker{}
	s.m.groups["rg"] = newTestResourceGroup("rg", typeutil.NewSet[int64](1, 2, 3, 4))
	s.m.groups["other"] = newTestResourceGroup("other", typeutil.NewSet[int64](5, 6))
	for _, id := range []int64{10, 20} {
		s.m.collectionPartitions[id] = typeutil.NewSet[int64](id + 1)
		s.m.collections[id] = &Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: id}}
		r := groupReplica(id, id, "rg", 1, 2, 3, 4)
		s.m.putReplicasInMemory(id, r)
		s.persisted[id] = proto.Clone(r.replicaPB).(*querypb.Replica)
	}
	mockey.Mock(querycoord.Catalog.SaveReplica).To(func(_ querycoord.Catalog, _ context.Context, rs ...*querypb.Replica) error {
		s.writes++
		if s.writes == s.failWrite && !s.failAfterSave {
			return merr.WrapErrServiceUnavailable("injected save failure")
		}
		for _, r := range rs {
			s.persisted[r.GetID()] = proto.Clone(r).(*querypb.Replica)
		}
		if s.duringSave != nil {
			f := s.duringSave
			s.duringSave = nil
			f()
		}
		if s.writes == s.failWrite {
			return merr.WrapErrServiceUnavailable("injected lost save response")
		}
		return nil
	}).Build()
	mockey.Mock((*CoordinatorBroker).GetRecoveryInfoV2).To(func(_ *CoordinatorBroker, _ context.Context, id int64, parts ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
		s.calls++
		if s.duringRows != nil {
			f := s.duringRows
			s.duringRows = nil
			f()
		}
		if s.failRows {
			return nil, nil, merr.WrapErrServiceUnavailable("injected rows failure")
		}
		require.Equal(t, []int64{id + 1}, parts)
		return nil, []*datapb.SegmentInfo{
			{ID: id, PartitionID: id + 1, NumOfRows: s.rows[id] - 1, State: commonpb.SegmentState_Dropped, Level: s.level},
			{ID: id + 100, PartitionID: common.AllPartitionsID, NumOfRows: 1},
			{ID: id + 200, PartitionID: 999, NumOfRows: 1000000, Level: datapb.SegmentLevel_L1},
			{ID: id + 300, PartitionID: common.AllPartitionsID, NumOfRows: 1000000, Level: datapb.SegmentLevel_L0},
			{ID: id + 400, PartitionID: 999, NumOfRows: 2000000, Level: datapb.SegmentLevel_L2},
		}, nil
	}).Build()
	return s
}

func (s *groupTestState) recover(id int64) error {
	rgs, _ := s.m.GetResourceGroups(context.Background(), s.m.GetResourceGroupByCollection(context.Background(), id).Collect())
	return s.m.RecoverNodesInCollection(context.Background(), id, rgs)
}

func TestCollectionGroupRecovery(t *testing.T) {
	mockey.PatchConvey("stable, concurrent, row and RG changes", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		require.NoError(t, s.recover(20))
		require.Equal(t, 2, s.calls)
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 1)
		require.Equal(t, map[int64]int64{10: 300, 20: 100}, s.m.placementPolicy.groups[10].rows)
		writes := s.writes
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) { defer wg.Done(); require.NoError(t, s.recover([]int64{10, 20}[i%2])) }(i)
		}
		wg.Wait()
		require.Equal(t, writes, s.writes)
		require.Equal(t, 2, s.calls)
		s.rows[10], s.rows[20] = 100, 300
		s.m.placementPolicy.groups[10].refreshed = time.Time{}
		require.NoError(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 1)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 3)
		// Failure retains the complete snapshot, including when an RG node is lost.
		s.failRows = true
		s.m.placementPolicy.groups[10].refreshed = time.Time{}
		s.m.groups["rg"].nodes = typeutil.NewSet[int64](7, 8, 9, 10)
		require.Error(t, s.recover(20))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 1)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 3)
		for _, r := range s.m.GetByResourceGroup(context.Background(), "rg") {
			for _, n := range r.GetRWNodes() {
				require.GreaterOrEqual(t, n, int64(7))
			}
		}
	})
}

func TestCollectionGroupRowLevels(t *testing.T) {
	for _, tc := range []struct {
		name  string
		level datapb.SegmentLevel
		rows  int64
	}{
		{"L1", datapb.SegmentLevel_L1, 17},
		{"legacy L1", datapb.SegmentLevel_Legacy, 17},
		{"L0", datapb.SegmentLevel_L0, 0},
		{"L2", datapb.SegmentLevel_L2, 17},
		{"future data level", datapb.SegmentLevel(99), 17},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockey.PatchConvey("count non-L0 recovery rows in loaded partitions", t, func() {
				g := &collectionGroup{members: []int64{10}}
				mockey.Mock((*CoordinatorBroker).GetRecoveryInfoV2).To(func(_ *CoordinatorBroker, _ context.Context, id int64, parts ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
					require.Equal(t, int64(10), id)
					require.Equal(t, []int64{11}, parts)
					return nil, []*datapb.SegmentInfo{
						{ID: 1, PartitionID: 11, NumOfRows: 10, State: commonpb.SegmentState_Flushed, Level: tc.level},
						{ID: 2, PartitionID: 11, NumOfRows: 7, State: commonpb.SegmentState_Dropped, Level: tc.level},
						{ID: 3, PartitionID: 12, NumOfRows: 1000, State: commonpb.SegmentState_Flushed, Level: tc.level},
					}, nil
				}).Build()
				require.NoError(t, g.refreshRows(context.Background(), &CoordinatorBroker{}, map[int64][]int64{10: {11}}))
				require.Equal(t, tc.rows, g.rows[10])
			})
		})
	}
}

func TestCollectionGroupClusteringCompactedRows(t *testing.T) {
	for _, initialLevel := range []datapb.SegmentLevel{datapb.SegmentLevel_L1, datapb.SegmentLevel_L2} {
		mockey.PatchConvey("L2 data retains proportional capacity through recovery", t, func() {
			s := newGroupTestState(t, []string{"rg"})
			s.level = initialLevel
			require.NoError(t, s.recover(10))
			require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
			require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 1)
			before := s.m.Get(context.Background(), 10).GetRWNodes()
			writes := s.writes
			g := s.m.placementPolicy.groups[10]
			s.level = datapb.SegmentLevel_L2
			g.refreshed = time.Time{}
			require.NoError(t, s.recover(20))
			require.Equal(t, s.rows, g.rows)
			require.Equal(t, writes, s.writes)
			require.Equal(t, before, s.m.Get(context.Background(), 10).GetRWNodes())
			// A new L2 row snapshot must still drive a changed placement plan.
			s.rows[10], s.rows[20] = 100, 300
			g.refreshed = time.Time{}
			require.NoError(t, s.recover(10))
			require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 1)
			require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 3)
		})
	}
}

func TestCollectionGroupDisabled(t *testing.T) {
	for _, allow := range [][]string{nil, {"miss"}} {
		mockey.PatchConvey("disabled and unmatched keep old allocation without DC", t, func() {
			s := newGroupTestState(t, allow)
			require.NoError(t, s.recover(10))
			require.NoError(t, s.recover(20))
			require.Zero(t, s.calls)
			require.Zero(t, s.writes)
		})
	}
	mockey.PatchConvey("wildcard; unlisted collection still ordinary", t, func() {
		s := newGroupTestState(t, []string{"*"})
		require.NoError(t, s.recover(10))
		require.Equal(t, 2, s.calls)
		s.m.putReplicasInMemory(30, groupReplica(30, 30, "rg"))
		require.NoError(t, s.recover(30))
		require.Equal(t, 2, s.calls)
		require.Len(t, s.m.Get(context.Background(), 30).GetRWNodes(), 4)
	})
}

func TestCollectionGroupPartialWriteAndRestart(t *testing.T) {
	mockey.PatchConvey("retry resumes successful writes and restart keeps layout", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.failWrite = 2
		require.Error(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 4)
		require.NoError(t, s.recover(20))
		require.Equal(t, 3, s.writes)
		before := make(map[int64][]int64)
		for id, r := range s.persisted {
			before[id] = slices.Clone(r.GetNodes())
		}
		// Rebuild the COW replica index and plan cache from durable protobufs.
		restarted := NewReplicaManager(nil, s.m.ReplicaManager.catalog)
		s.m.placementPolicy = nil
		for id, r := range s.persisted {
			restarted.putReplicasInMemory(id, NewReplica(proto.Clone(r).(*querypb.Replica)))
		}
		s.m.ReplicaManager = restarted
		require.NoError(t, s.recover(20))
		for id, nodes := range before {
			require.ElementsMatch(t, nodes, s.m.Get(context.Background(), id).GetRWNodes())
		}
	})
}

func TestCollectionGroupInitialFailureAndMixedRG(t *testing.T) {
	mockey.PatchConvey("initial row failure still repairs failed nodes; other RG remains normal", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.failRows = true
		s.m.putReplicasInMemory(10, groupReplica(11, 10, "other"))
		s.m.groups["rg"].nodes = typeutil.NewSet[int64](7, 8)
		require.Error(t, s.recover(10))
		require.NotEmpty(t, s.m.Get(context.Background(), 10).GetRWNodes())
		require.ElementsMatch(t, []int64{5, 6}, s.m.Get(context.Background(), 11).GetRWNodes())
	})
}

func TestCollectionGroupLiveConfiguration(t *testing.T) {
	mockey.PatchConvey("recovery reads current config and keeps cached rows", t, func() {
		s := newGroupTestState(t, nil)
		cfg := &paramtable.Get().QueryCoordCfg
		require.NoError(t, s.recover(10))
		require.Zero(t, s.calls)
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, "rg"))
		require.NoError(t, s.recover(10))
		require.Equal(t, 2, s.calls)
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, "*"))
		require.NoError(t, s.recover(20))
		require.Equal(t, 2, s.calls) // changing the whitelist doesn't discard row cache
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementCollectionGroups.Key, `{"g":[20]}`))
		require.NoError(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 4)
		require.Equal(t, 2, s.calls) // removed member uses legacy recovery
		require.NoError(t, s.recover(20))
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 4)
		require.Equal(t, 3, s.calls)
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, ""))
		require.NoError(t, s.recover(20))
		require.Equal(t, 3, s.calls)
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementCollectionGroups.Key, `invalid`))
		require.NoError(t, s.recover(20)) // disabled config doesn't interfere with legacy recovery
		require.Equal(t, 3, s.calls)
	})
}

func TestCollectionGroupConfigurationDuringRecovery(t *testing.T) {
	mockey.PatchConvey("config changed during RPC rejects old plan; subsequent recovery reads it", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		cfg := &paramtable.Get().QueryCoordCfg
		s.duringRows = func() {
			require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, ""))
		}
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		require.Zero(t, s.writes)
		calls := s.calls
		require.NoError(t, s.recover(20))
		require.Equal(t, calls, s.calls)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 4)
	})
	mockey.PatchConvey("concurrent trigger crosses a config change without committing the old plan", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		entered, release := make(chan struct{}), make(chan struct{})
		s.duringRows = func() { close(entered); <-release }
		first, second := make(chan error, 1), make(chan error, 1)
		go func() { first <- s.recover(10) }()
		<-entered
		cfg := &paramtable.Get().QueryCoordCfg
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, ""))
		go func() { second <- s.recover(20) }()
		close(release)
		require.ErrorIs(t, <-first, merr.ErrServiceUnavailable)
		require.NoError(t, <-second)
		require.Zero(t, s.writes)
		require.Equal(t, 2, s.calls)
	})

	mockey.PatchConvey("uncertain write settles before config removes the old group", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		cfg := &paramtable.Get().QueryCoordCfg
		s.failWrite, s.failAfterSave = 1, true
		require.Error(t, s.recover(10))
		old := s.m.placementPolicy
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementCollectionGroups.Key, `{}`))
		s.failWrite = 2
		require.Error(t, s.recover(20))
		require.Same(t, old, s.m.placementPolicy)
		require.NoError(t, s.recover(20))
		require.Nil(t, old.groups[10].pending)
		require.Empty(t, s.m.placementPolicy.groups)
		require.NoError(t, s.recover(10))
		require.Equal(t, 2, s.calls)
		for _, id := range []int64{10, 20} {
			require.Len(t, s.m.Get(context.Background(), id).GetRWNodes(), 4)
			require.True(t, proto.Equal(s.persisted[id], s.m.Get(context.Background(), id).replicaPB))
		}
	})
	mockey.PatchConvey("invalid active config preserves ordinary recovery and can be corrected", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		cfg := &paramtable.Get().QueryCoordCfg
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementCollectionGroups.Key, `invalid`))
		require.NoError(t, s.recover(10))
		require.Zero(t, s.calls)
		require.Zero(t, s.writes)
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementCollectionGroups.Key, `{"g":[10,20]}`))
		require.NoError(t, s.recover(10))
		require.Equal(t, 2, s.calls)
	})
}

func TestCollectionGroupScopeValidation(t *testing.T) {
	mockey.PatchConvey("partition scope change during RPC rejects stale row snapshot", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.duringRows = func() {
			s.m.CollectionManager.rwmutex.Lock()
			s.m.collectionPartitions[10].Insert(12)
			s.m.CollectionManager.rwmutex.Unlock()
		}
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		require.Zero(t, s.writes)
	})
	mockey.PatchConvey("replica joins during refresh; new collection must join the row snapshot", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.m.coll2Replicas.Remove(20)
		s.duringRows = func() {
			s.m.collLock.Lock(20)
			s.m.putReplicasInMemory(20, groupReplica(21, 20, "rg"))
			s.m.collLock.Unlock(20)
		}
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		require.ElementsMatch(t, []int64{1, 2, 3, 4}, s.m.Get(context.Background(), 21).GetRWNodes())
	})
	mockey.PatchConvey("unknown load scope does not request DC or shrink survivors", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		delete(s.m.collectionPartitions, 20)
		require.Error(t, s.recover(10))
		require.Zero(t, s.calls)
		require.Zero(t, s.writes)
	})
	mockey.PatchConvey("missing RG and wait-until-RG-ready", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		r := s.m.Get(context.Background(), 10).CopyForWrite()
		r.SetWaitRGReadyAt(time.Now())
		s.m.putReplicasInMemory(10, r.IntoReplica())
		s.m.groups["rg"].cfg.Requests.NodeNum = 5
		require.NoError(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 4)
		s.m.groups["rg"].cfg.Requests.NodeNum = 4
		require.NoError(t, s.recover(20))
		require.False(t, s.m.Get(context.Background(), 10).NeedWaitRGReady())
		delete(s.m.groups, "rg")
		require.ErrorIs(t, s.recover(10), merr.ErrResourceGroupNotFound)
	})
}

func TestCollectionGroupSharingAfterNodeGrowth(t *testing.T) {
	replicas := []*Replica{groupReplica(1, 10, "rg", 1), groupReplica(2, 20, "rg", 1), groupReplica(3, 30, "rg", 1)}
	plan := assignCollectionGroup(replicas, []int64{1, 2}, map[int64]int64{10: 100, 20: 10, 30: 10})
	require.Equal(t, []int64{1, 2}, plan[1])
	require.Equal(t, []int64{1}, plan[2])
	require.Equal(t, []int64{2}, plan[3])
}

func TestCollectionGroupDoesNotBlockPendingMigration(t *testing.T) {
	a, b := groupReplica(1, 10, "rg"), groupReplica(2, 10, "rg", 1)
	plan := map[int64][]int64{1: {1}, 2: {2}}
	// Node 2 is free but reserved for the old owner of node 1. A must wait.
	require.Nil(t, applyCollectionGroupPlan(a, []*Replica{a, b}, []int64{1, 2}, plan))
	b = applyCollectionGroupPlan(b, []*Replica{a, b}, []int64{1, 2}, plan)
	require.Equal(t, []int64{2}, b.GetRWNodes())
	require.Equal(t, []int64{1}, b.GetRONodes())
	require.Nil(t, applyCollectionGroupPlan(a, []*Replica{a, b}, []int64{1, 2}, plan))
	mutable := b.CopyForWrite()
	mutable.RemoveNode(1)
	b = mutable.IntoReplica()
	a = applyCollectionGroupPlan(a, []*Replica{a, b}, []int64{1, 2}, plan)
	require.Equal(t, []int64{1}, a.GetRWNodes())
}

func TestCollectionGroupUncertainWrite(t *testing.T) {
	mockey.PatchConvey("lost catalog response is replayed before changed inputs", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.failWrite = 1
		s.failAfterSave = true
		require.Error(t, s.recover(10))
		require.Len(t, s.persisted[10].GetNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 4)
		group := s.m.placementPolicy.groups[10]
		require.NotNil(t, group.pending)
		s.failWrite = 2
		require.Error(t, s.recover(20))
		require.NotNil(t, group.pending)
		// A memory-only flag must survive the pending full protobuf replay.
		current := s.m.Get(context.Background(), 10).CopyForWrite()
		current.SetQueryInvisible(true)
		s.m.putReplicasInMemory(10, current.IntoReplica())
		s.rows[10], s.rows[20] = 100, 300
		group.refreshed = time.Time{}
		require.NoError(t, s.recover(10))
		require.Nil(t, group.pending)
		require.False(t, s.m.Get(context.Background(), 10).IsQueryVisible())
		require.Len(t, s.persisted[10].GetNodes(), 1)
	})
	mockey.PatchConvey("a later full replica write supersedes the uncertain write", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.failWrite = 1
		require.Error(t, s.recover(10))
		current := s.m.Get(context.Background(), 10).CopyForWrite()
		current.AddRONode(4)
		require.NoError(t, s.m.Put(context.Background(), current.IntoReplica()))
		require.NoError(t, s.recover(10))
		require.Nil(t, s.m.placementPolicy.groups[10].pending)
	})
}

func TestCollectionGroupSmallCollectionMinimumsPreserveWeightedQuota(t *testing.T) {
	replicas := []*Replica{groupReplica(1, 10, "rg")}
	rows := map[int64]int64{10: 900}
	for id := int64(2); id <= 7; id++ {
		replicas = append(replicas, groupReplica(id, id*10, "rg"))
		rows[id*10] = 10
	}
	plan := assignCollectionGroup(replicas, []int64{1, 2, 3, 4, 5}, rows)
	require.Len(t, plan[1], 5)
	for _, r := range replicas[1:] {
		require.Len(t, plan[r.GetID()], 1)
	}
}

func TestCollectionGroupInvalidConfigRecovery(t *testing.T) {
	for _, invalid := range []string{`invalid`, `{"g":[]}`, `{"g":[10,10]}`} {
		mockey.PatchConvey("invalid config does not stop any collection's recovery: "+invalid, t, func() {
			s := newGroupTestState(t, []string{"rg"})
			require.NoError(t, s.recover(10))
			s.m.putReplicasInMemory(30, groupReplica(30, 30, "other", 5))
			s.m.putReplicasInMemory(40, groupReplica(40, 40, "rg", 1))
			calls := s.calls
			cfg := &paramtable.Get().QueryCoordCfg
			require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementCollectionGroups.Key, invalid))
			s.m.groups["rg"].nodes = typeutil.NewSet[int64](2, 3, 4, 7)
			for _, id := range []int64{30, 40, 10, 20} {
				require.NoError(t, s.recover(id))
			}
			require.Equal(t, calls, s.calls)
			require.ElementsMatch(t, []int64{5, 6}, s.m.Get(context.Background(), 30).GetRWNodes())
			for _, id := range []int64{10, 20, 40} {
				r := s.m.Get(context.Background(), id)
				require.ElementsMatch(t, []int64{2, 3, 4, 7}, r.GetRWNodes())
				require.Contains(t, r.GetRONodes(), int64(1))
			}
			policy := s.m.placementPolicy
			require.NoError(t, s.recover(10))
			require.Same(t, policy, s.m.placementPolicy)
			require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementCollectionGroups.Key, `{"g":[10,20]}`))
			require.NoError(t, s.recover(10))
			require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		})
	}
	mockey.PatchConvey("invalid config cannot bypass an uncertain write", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.failWrite, s.failAfterSave = 1, true
		require.Error(t, s.recover(10))
		old := s.m.placementPolicy
		require.NoError(t, paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ReplicaPlacementCollectionGroups.Key, `invalid`))
		s.failWrite = 2
		require.Error(t, s.recover(20))
		require.Same(t, old, s.m.placementPolicy)
		require.NotNil(t, old.groups[10].pending)
		require.NoError(t, s.recover(20))
		require.Nil(t, old.groups[10].pending)
		require.Empty(t, s.m.placementPolicy.groups)
	})
}

func TestCollectionGroupFirstLoadCapacity(t *testing.T) {
	for _, count := range []int{1, 2} {
		mockey.PatchConvey("unknown scope uses ordinary per-collection capacity", t, func() {
			s := newGroupTestState(t, []string{"rg"})
			s.m.coll2Replicas.Remove(10)
			delete(s.m.collectionPartitions, 10)
			for i := 0; i < count; i++ {
				s.m.putReplicasInMemory(10, groupReplica(int64(10+i), 10, "rg"))
			}
			require.Error(t, s.recover(10)) // unknown rows do not prevent node assignment
			require.Zero(t, s.calls)
			used := typeutil.NewSet[int64]()
			for _, r := range s.m.GetByCollection(context.Background(), 10) {
				require.Len(t, r.GetRWNodes(), 4/count)
				for _, n := range r.GetRWNodes() {
					require.False(t, used.Contain(n))
					used.Insert(n)
				}
			}
			s.m.collectionPartitions[10] = typeutil.NewSet[int64](11)
			require.NoError(t, s.recover(10))
			require.Equal(t, 2, s.calls)
		})
	}
	mockey.PatchConvey("scope change during rows still repairs a failed node", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.duringRows = func() {
			s.m.collectionPartitions[10].Insert(12)
			s.m.groups["rg"].nodes = typeutil.NewSet[int64](5, 6, 7, 8)
		}
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		for _, id := range []int64{10, 20} {
			require.ElementsMatch(t, []int64{5, 6, 7, 8}, s.m.Get(context.Background(), id).GetRWNodes())
		}
	})
}

func TestCollectionGroupRowHysteresis(t *testing.T) {
	mockey.PatchConvey("near equal rows retain a remainder seat through refresh and restart", t, func() {
		s := newGroupTestState(t, []string{"rg"})
		s.m.groups["rg"].nodes = typeutil.NewSet[int64](1, 2, 3)
		s.rows[10], s.rows[20] = 1001, 1000
		require.NoError(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 2)
		writes := s.writes
		for i := 0; i < 10; i++ {
			s.rows[10], s.rows[20] = int64(1000+i%2), int64(1001-i%2)
			s.m.placementPolicy.groups[10].refreshed = time.Time{}
			require.NoError(t, s.recover(20))
		}
		require.Equal(t, writes, s.writes)
		s.rows[10], s.rows[20] = 1000, 1001
		s.m.placementPolicy = nil // restart: no cache, current durable replica counts remain
		require.NoError(t, s.recover(20))
		require.Equal(t, writes, s.writes)
		// Gradual growth is compared with the accepted plan, not the last poll.
		for n := int64(1010); n <= 1500; n += 10 {
			s.rows[20] = n
			s.m.placementPolicy.groups[10].refreshed = time.Time{}
			require.NoError(t, s.recover(20))
		}
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 1)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 2)
		// Node changes bypass the row deadband.
		s.m.groups["rg"].nodes.Insert(4)
		require.NoError(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 2)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 2)
	})
}

func TestCollectionGroupPersistenceDoesNotLockMetadata(t *testing.T) {
	for _, change := range []string{"none", "nodes", "scope", "config", "rg config", "drop rg"} {
		mockey.PatchConvey("metadata may progress during a catalog write: "+change, t, func() {
			s := newGroupTestState(t, []string{"rg"})
			s.duringSave = func() {
				require.True(t, s.m.CollectionManager.rwmutex.TryLock(), "catalog write must not hold collection metadata lock")
				if change == "scope" {
					s.m.collectionPartitions[10].Insert(12)
				}
				s.m.CollectionManager.rwmutex.Unlock()
				require.True(t, s.m.ResourceManager.rwmutex.TryLock(), "catalog write must not hold resource metadata lock")
				switch change {
				case "nodes":
					s.m.groups["rg"].nodes.Remove(1)
				case "rg config":
					s.m.groups["rg"].cfg.Requests.NodeNum++
				case "drop rg":
					delete(s.m.groups, "rg")
				}
				s.m.ResourceManager.rwmutex.Unlock()
				if change == "config" {
					require.NoError(t, paramtable.Get().Save(paramtable.Get().QueryCoordCfg.ReplicaPlacementResourceGroupAllowlist.Key, ""))
				}
			}
			err := s.recover(10)
			if change == "none" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.Equal(t, 1, s.writes, "abort before another write from the stale snapshot")
			if change == "nodes" {
				require.NoError(t, s.recover(20))
				for _, r := range s.m.GetByResourceGroup(context.Background(), "rg") {
					require.NotContains(t, r.GetRWNodes(), int64(1))
				}
			}
		})
	}
}

func TestCollectionGroupBlockedTargetsRetainCapacity(t *testing.T) {
	a := groupReplica(1, 10, "rg", 1, 2, 3, 4)
	b := groupReplica(2, 10, "rg", 5, 6, 7, 8)
	plan := map[int64][]int64{1: {5, 6}, 2: {7, 8}}
	a = applyCollectionGroupPlan(a, []*Replica{a, b}, []int64{1, 2, 3, 4, 5, 6, 7, 8}, plan)
	require.ElementsMatch(t, []int64{1, 2}, a.GetRWNodes())
	b = applyCollectionGroupPlan(b, []*Replica{a, b}, []int64{1, 2, 3, 4, 5, 6, 7, 8}, plan)
	require.ElementsMatch(t, []int64{5, 6}, b.GetRONodes())
	mutable := b.CopyForWrite()
	mutable.RemoveNode(5, 6)
	b = mutable.IntoReplica()
	a = applyCollectionGroupPlan(a, []*Replica{a, b}, []int64{1, 2, 3, 4, 5, 6, 7, 8}, plan)
	require.ElementsMatch(t, []int64{5, 6}, a.GetRWNodes())
}
