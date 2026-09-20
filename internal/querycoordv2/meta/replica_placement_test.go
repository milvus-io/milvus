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

func placementReplica(id, collection int64, rg string, rw ...int64) *Replica {
	return NewReplica(&querypb.Replica{ID: id, CollectionID: collection, ResourceGroup: rg, Nodes: rw})
}

func TestReplicaPlacementConfig(t *testing.T) {
	for _, tc := range []struct {
		raw     string
		enabled bool
	}{{"", false}, {"rg", true}, {"*", true}, {"r", false}, {" rg ,other ", true}, {" , ", false}, {"rg*", false}} {
		require.Equal(t, tc.enabled, newReplicaPlacementPolicy(tc.raw).matches("rg"))
	}
}

func TestReplicaPlacementAssignment(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas []*Replica
		nodes    []int64
		rows     map[int64]int64
		counts   []int
	}{
		{"weighted", []*Replica{placementReplica(1, 10, "rg"), placementReplica(2, 20, "rg")}, []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, map[int64]int64{10: 900, 20: 100}, []int{9, 1}},
		{"different replica counts", []*Replica{placementReplica(1, 10, "rg"), placementReplica(2, 10, "rg"), placementReplica(3, 20, "rg")}, []int64{1, 2, 3, 4, 5, 6}, map[int64]int64{10: 100, 20: 100}, []int{2, 2, 2}},
		{"oversubscribed", []*Replica{placementReplica(1, 10, "rg"), placementReplica(2, 10, "rg"), placementReplica(3, 20, "rg"), placementReplica(4, 30, "rg")}, []int64{1, 2}, map[int64]int64{10: 100, 20: 10, 30: 20}, []int{1, 1, 1, 1}},
		{"empty data", []*Replica{placementReplica(1, 10, "rg"), placementReplica(2, 20, "rg")}, []int64{1, 2, 3}, nil, []int{2, 1}},
		{"no nodes", []*Replica{placementReplica(1, 10, "rg")}, nil, nil, []int{0}},
		{"same collection capacity", []*Replica{placementReplica(1, 10, "rg"), placementReplica(2, 10, "rg")}, []int64{1}, nil, []int{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan := assignReplicaPlacement(tc.replicas, tc.nodes, tc.rows)
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
				require.Equal(t, plan, assignReplicaPlacement(tc.replicas, tc.nodes, tc.rows))
			}
		})
	}
	// Enable on existing replicas that both own the entire RG: spread their remaining nodes.
	rs := []*Replica{placementReplica(1, 10, "rg", 1, 2, 3, 4), placementReplica(2, 20, "rg", 1, 2, 3, 4)}
	plan := assignReplicaPlacement(rs, []int64{1, 2, 3, 4}, map[int64]int64{10: 1, 20: 1})
	require.Equal(t, map[int64][]int64{1: {1, 2}, 2: {3, 4}}, plan)
	// Restart recomputation retains every legal RW when the input and quotas are unchanged.
	for _, r := range rs {
		r.replicaPB.Nodes = plan[r.GetID()]
	}
	require.Equal(t, plan, assignReplicaPlacement(rs, []int64{1, 2, 3, 4}, map[int64]int64{10: 1, 20: 1}))
}

func TestReplicaPlacementTransitions(t *testing.T) {
	a := placementReplica(1, 10, "new", 3, 1)
	b := placementReplica(2, 10, "old", 4)
	b.replicaPB.RoNodes = []int64{2}
	b = NewReplica(b.replicaPB)
	siblings := []*Replica{a, b}
	updated := applyReplicaPlacementPlan(a, siblings, []int64{1, 2, 3}, map[int64][]int64{1: {2}})
	require.Equal(t, []int64{1}, updated.GetRWNodes()) // last serving RW retained
	require.Equal(t, []int64{3, 1}, a.GetRWNodes())    // immutable source, even when sorted
	require.ElementsMatch(t, []int64{3}, updated.GetRONodes())
	require.Nil(t, applyReplicaPlacementPlan(updated, []*Replica{updated, b}, []int64{1, 2, 3}, map[int64][]int64{1: {2}}))
	// Only after the old RG's RO has drained can its node enter this replica.
	b = placementReplica(2, 10, "old", 4)
	next := applyReplicaPlacementPlan(updated, []*Replica{updated, b}, []int64{1, 2, 3}, map[int64][]int64{1: {2}})
	require.ElementsMatch(t, []int64{2}, next.GetRWNodes())
	require.ElementsMatch(t, []int64{1, 3}, next.GetRONodes())
	// Fault recovery obtains a free node even if the preferred node is blocked.
	b = placementReplica(2, 10, "old", 2)
	next = applyReplicaPlacementPlan(a, []*Replica{a, b}, []int64{2, 5}, map[int64][]int64{1: {2}})
	require.Equal(t, []int64{5}, next.GetRWNodes())
	require.ElementsMatch(t, []int64{1, 3}, next.GetRONodes())
	// RO can be promoted back when it belongs to this replica.
	require.Equal(t, []int64{3}, applyReplicaPlacementPlan(updated, []*Replica{updated}, []int64{3}, map[int64][]int64{1: {3}}).GetRWNodes())
}

type placementTestState struct {
	failAfterSave bool
	duringRows    func()
	duringSave    func()
	m             *Meta
	writes, calls int
	failWrite     int
	failRows      bool
	failRowsFor   int64
	rows          map[int64]int64
	level         datapb.SegmentLevel
	shards        map[int64]int
	persisted     map[int64]*querypb.Replica
}

// Runtime mocks copy protobuf values inside callbacks, never retain stack pointers.
func newPlacementTestState(t *testing.T, allow []string) *placementTestState {
	paramtable.Init()
	catalog := querycoord.NewCatalog(nil)
	s := &placementTestState{m: NewMeta(nil, catalog, nil), rows: map[int64]int64{10: 300, 20: 100}, level: datapb.SegmentLevel_L1, persisted: make(map[int64]*querypb.Replica)}
	cfg := &paramtable.Get().QueryCoordCfg
	for _, key := range []string{cfg.ReplicaPlacementResourceGroupAllowlist.Key} {
		t.Cleanup(func() { paramtable.Get().Reset(key) })
	}
	require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, strings.Join(allow, ",")))
	s.m.Broker = &CoordinatorBroker{}
	s.m.groups["rg"] = newTestResourceGroup("rg", typeutil.NewSet[int64](1, 2, 3, 4))
	s.m.groups["other"] = newTestResourceGroup("other", typeutil.NewSet[int64](5, 6))
	for _, id := range []int64{10, 20} {
		s.m.collectionPartitions[id] = typeutil.NewSet[int64](id + 1)
		s.m.collections[id] = &Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: id}}
		r := placementReplica(id, id, "rg", 1, 2, 3, 4)
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
		if s.failRows || s.failRowsFor == id {
			return nil, nil, merr.WrapErrServiceUnavailable("injected rows failure")
		}
		require.Equal(t, []int64{id + 1}, parts)
		count, ok := s.shards[id]
		if !ok {
			count = 1
		}
		channels := make([]*datapb.VchannelInfo, count)
		for i := range channels {
			channels[i] = &datapb.VchannelInfo{ChannelName: "v"}
		}
		return channels, []*datapb.SegmentInfo{
			{ID: id, PartitionID: id + 1, NumOfRows: s.rows[id] - 1, State: commonpb.SegmentState_Dropped, Level: s.level},
			{ID: id + 100, PartitionID: common.AllPartitionsID, NumOfRows: 1},
			{ID: id + 200, PartitionID: 999, NumOfRows: 1000000, Level: datapb.SegmentLevel_L1},
			{ID: id + 300, PartitionID: common.AllPartitionsID, NumOfRows: 1000000, Level: datapb.SegmentLevel_L0},
			{ID: id + 400, PartitionID: 999, NumOfRows: 2000000, Level: datapb.SegmentLevel_L2},
		}, nil
	}).Build()
	return s
}

func (s *placementTestState) recover(id int64) error {
	rgs, _ := s.m.GetResourceGroups(context.Background(), s.m.GetResourceGroupByCollection(context.Background(), id).Collect())
	return s.m.RecoverNodesInCollection(context.Background(), id, rgs)
}

func TestReplicaPlacementRecovery(t *testing.T) {
	mockey.PatchConvey("stable, concurrent, row and RG changes", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		require.NoError(t, s.recover(20))
		require.Equal(t, 2, s.calls)
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 1)
		require.Equal(t, map[int64]int64{10: 300, 20: 100}, s.m.placementPolicy.groups["rg"].rows)
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
		s.m.placementPolicy.groups["rg"].refreshed = time.Time{}
		require.NoError(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 1)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 3)
		// Failure retains the complete snapshot, including when an RG node is lost.
		s.failRows = true
		s.m.placementPolicy.groups["rg"].refreshed = time.Time{}
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

func TestReplicaPlacementRowLevels(t *testing.T) {
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
				g := &replicaPlacement{}
				mockey.Mock((*CoordinatorBroker).GetRecoveryInfoV2).To(func(_ *CoordinatorBroker, _ context.Context, id int64, parts ...int64) ([]*datapb.VchannelInfo, []*datapb.SegmentInfo, error) {
					require.Equal(t, int64(10), id)
					require.Equal(t, []int64{11}, parts)
					return []*datapb.VchannelInfo{{ChannelName: "v1"}}, []*datapb.SegmentInfo{
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

func TestReplicaPlacementClusteringCompactedRows(t *testing.T) {
	for _, initialLevel := range []datapb.SegmentLevel{datapb.SegmentLevel_L1, datapb.SegmentLevel_L2} {
		mockey.PatchConvey("L2 data retains proportional capacity through recovery", t, func() {
			s := newPlacementTestState(t, []string{"rg"})
			s.level = initialLevel
			require.NoError(t, s.recover(10))
			require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
			require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 1)
			before := s.m.Get(context.Background(), 10).GetRWNodes()
			writes := s.writes
			g := s.m.placementPolicy.groups["rg"]
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

func TestReplicaPlacementDisabled(t *testing.T) {
	for _, allow := range [][]string{nil, {"miss"}} {
		mockey.PatchConvey("disabled and unmatched keep old allocation without DC", t, func() {
			s := newPlacementTestState(t, allow)
			require.NoError(t, s.recover(10))
			require.NoError(t, s.recover(20))
			require.Zero(t, s.calls)
			require.Zero(t, s.writes)
		})
	}
	mockey.PatchConvey("wildcard automatically includes newly loaded single-shard collections", t, func() {
		s := newPlacementTestState(t, []string{"*"})
		require.NoError(t, s.recover(10))
		require.Equal(t, 2, s.calls)
		s.m.putReplicasInMemory(30, placementReplica(30, 30, "rg"))
		require.Error(t, s.recover(30))
		require.Equal(t, 2, s.calls)
		require.Len(t, s.m.Get(context.Background(), 30).GetRWNodes(), 4)
	})
}

func TestReplicaPlacementPartialWriteAndRestart(t *testing.T) {
	mockey.PatchConvey("retry resumes successful writes and restart keeps layout", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		s.failWrite = 2
		require.Error(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 4)
		require.NoError(t, s.recover(20))
		require.Equal(t, 4, s.writes)
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

func TestReplicaPlacementInitialFailureAndMixedRG(t *testing.T) {
	mockey.PatchConvey("initial row failure still repairs failed nodes; other RG remains normal", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		s.failRows = true
		s.m.putReplicasInMemory(10, placementReplica(11, 10, "other"))
		s.m.groups["rg"].nodes = typeutil.NewSet[int64](7, 8)
		require.Error(t, s.recover(10))
		require.NotEmpty(t, s.m.Get(context.Background(), 10).GetRWNodes())
		require.ElementsMatch(t, []int64{5, 6}, s.m.Get(context.Background(), 11).GetRWNodes())
	})
}

func TestReplicaPlacementLiveConfiguration(t *testing.T) {
	mockey.PatchConvey("each trigger reads allowlist; unchanged RG retains cache", t, func() {
		s := newPlacementTestState(t, nil)
		cfg := &paramtable.Get().QueryCoordCfg
		require.NoError(t, s.recover(10))
		require.Zero(t, s.calls)
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, "rg"))
		require.NoError(t, s.recover(10))
		require.Equal(t, 2, s.calls)
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, "*"))
		require.NoError(t, s.recover(20))
		require.Equal(t, 2, s.calls)
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, ""))
		require.NoError(t, s.recover(10))
		require.NoError(t, s.recover(20))
		require.Equal(t, 2, s.calls)
		for _, id := range []int64{10, 20} {
			require.Len(t, s.m.Get(context.Background(), id).GetRWNodes(), 4)
		}
	})
}

func TestReplicaPlacementConfigurationDuringRecovery(t *testing.T) {
	mockey.PatchConvey("config changed during RPC rejects old plan; subsequent recovery reads it", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
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
		s := newPlacementTestState(t, []string{"rg"})
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
		s := newPlacementTestState(t, []string{"rg"})
		cfg := &paramtable.Get().QueryCoordCfg
		s.failWrite, s.failAfterSave = 1, true
		require.Error(t, s.recover(10))
		old := s.m.placementPolicy
		require.NoError(t, paramtable.Get().Save(cfg.ReplicaPlacementResourceGroupAllowlist.Key, ""))
		s.failWrite = 2
		require.Error(t, s.recover(20))
		require.Same(t, old, s.m.placementPolicy)
		require.NoError(t, s.recover(20))
		require.Nil(t, old.groups["rg"].pending)
		require.Empty(t, s.m.placementPolicy.groups)
		require.NoError(t, s.recover(10))
		require.Equal(t, 2, s.calls)
		for _, id := range []int64{10, 20} {
			require.Len(t, s.m.Get(context.Background(), id).GetRWNodes(), 4)
			require.True(t, proto.Equal(s.persisted[id], s.m.Get(context.Background(), id).replicaPB))
		}
	})
}

func TestReplicaPlacementScopeValidation(t *testing.T) {
	mockey.PatchConvey("partition scope change during RPC rejects stale row snapshot", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		s.duringRows = func() {
			s.m.CollectionManager.rwmutex.Lock()
			s.m.collectionPartitions[10].Insert(12)
			s.m.CollectionManager.rwmutex.Unlock()
		}
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		require.Zero(t, s.writes)
	})
	mockey.PatchConvey("replica joins during refresh; new collection must join the row snapshot", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		s.m.coll2Replicas.Remove(20)
		s.duringRows = func() {
			s.m.collLock.Lock(20)
			s.m.putReplicasInMemory(20, placementReplica(21, 20, "rg"))
			s.m.collLock.Unlock(20)
		}
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		require.ElementsMatch(t, []int64{1, 2, 3, 4}, s.m.Get(context.Background(), 21).GetRWNodes())
	})
	mockey.PatchConvey("unknown load scope does not request DC or shrink survivors", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		delete(s.m.collectionPartitions, 20)
		require.Error(t, s.recover(10))
		require.Zero(t, s.calls)
		require.Zero(t, s.writes)
	})
	mockey.PatchConvey("missing RG and wait-until-RG-ready", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
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

func TestReplicaPlacementSharingAfterNodeGrowth(t *testing.T) {
	replicas := []*Replica{placementReplica(1, 10, "rg", 1), placementReplica(2, 20, "rg", 1), placementReplica(3, 30, "rg", 1)}
	plan := assignReplicaPlacement(replicas, []int64{1, 2}, map[int64]int64{10: 100, 20: 10, 30: 10})
	require.Equal(t, []int64{1, 2}, plan[1])
	require.Equal(t, []int64{1}, plan[2])
	require.Equal(t, []int64{2}, plan[3])
}

func TestReplicaPlacementDoesNotBlockPendingMigration(t *testing.T) {
	a, b := placementReplica(1, 10, "rg"), placementReplica(2, 10, "rg", 1)
	plan := map[int64][]int64{1: {1}, 2: {2}}
	// Node 2 is free but reserved for the old owner of node 1. A must wait.
	require.Nil(t, applyReplicaPlacementPlan(a, []*Replica{a, b}, []int64{1, 2}, plan))
	b = applyReplicaPlacementPlan(b, []*Replica{a, b}, []int64{1, 2}, plan)
	require.Equal(t, []int64{2}, b.GetRWNodes())
	require.Equal(t, []int64{1}, b.GetRONodes())
	require.Nil(t, applyReplicaPlacementPlan(a, []*Replica{a, b}, []int64{1, 2}, plan))
	mutable := b.CopyForWrite()
	mutable.RemoveNode(1)
	b = mutable.IntoReplica()
	a = applyReplicaPlacementPlan(a, []*Replica{a, b}, []int64{1, 2}, plan)
	require.Equal(t, []int64{1}, a.GetRWNodes())
}

func TestReplicaPlacementUncertainWrite(t *testing.T) {
	mockey.PatchConvey("lost catalog response is replayed before changed inputs", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		s.failWrite = 1
		s.failAfterSave = true
		require.Error(t, s.recover(10))
		require.Len(t, s.persisted[10].GetNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 4)
		group := s.m.placementPolicy.groups["rg"]
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
		s := newPlacementTestState(t, []string{"rg"})
		s.failWrite = 1
		require.Error(t, s.recover(10))
		current := s.m.Get(context.Background(), 10).CopyForWrite()
		current.AddRONode(4)
		require.NoError(t, s.m.Put(context.Background(), current.IntoReplica()))
		require.NoError(t, s.recover(10))
		require.Nil(t, s.m.placementPolicy.groups["rg"].pending)
	})
}

func TestReplicaPlacementSmallCollectionMinimumsPreserveWeightedQuota(t *testing.T) {
	replicas := []*Replica{placementReplica(1, 10, "rg")}
	rows := map[int64]int64{10: 900}
	for id := int64(2); id <= 7; id++ {
		replicas = append(replicas, placementReplica(id, id*10, "rg"))
		rows[id*10] = 10
	}
	plan := assignReplicaPlacement(replicas, []int64{1, 2, 3, 4, 5}, rows)
	require.Len(t, plan[1], 5)
	for _, r := range replicas[1:] {
		require.Len(t, plan[r.GetID()], 1)
	}
}

func TestReplicaPlacementFirstLoadCapacity(t *testing.T) {
	for _, count := range []int{1, 2} {
		mockey.PatchConvey("unknown scope uses ordinary per-collection capacity", t, func() {
			s := newPlacementTestState(t, []string{"rg"})
			s.m.coll2Replicas.Remove(10)
			delete(s.m.collectionPartitions, 10)
			for i := 0; i < count; i++ {
				s.m.putReplicasInMemory(10, placementReplica(int64(10+i), 10, "rg"))
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
		s := newPlacementTestState(t, []string{"rg"})
		s.duringRows = func() {
			s.m.collectionPartitions[10].Insert(12)
			s.m.groups["rg"].nodes = typeutil.NewSet[int64](5, 6, 7, 8)
		}
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		for _, id := range []int64{10, 20} {
			require.NotEmpty(t, s.m.Get(context.Background(), id).GetRWNodes())
			for _, node := range s.m.Get(context.Background(), id).GetRWNodes() {
				require.Contains(t, []int64{5, 6, 7, 8}, node)
			}
		}
	})
}

func TestReplicaPlacementRowHysteresis(t *testing.T) {
	mockey.PatchConvey("near equal rows retain a remainder seat through refresh and restart", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		s.m.groups["rg"].nodes = typeutil.NewSet[int64](1, 2, 3)
		s.rows[10], s.rows[20] = 1001, 1000
		require.NoError(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 2)
		writes := s.writes
		for i := 0; i < 10; i++ {
			s.rows[10], s.rows[20] = int64(1000+i%2), int64(1001-i%2)
			s.m.placementPolicy.groups["rg"].refreshed = time.Time{}
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
			s.m.placementPolicy.groups["rg"].refreshed = time.Time{}
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

func TestReplicaPlacementPersistenceDoesNotLockMetadata(t *testing.T) {
	for _, change := range []string{"none", "nodes", "scope", "config", "rg config", "drop rg"} {
		mockey.PatchConvey("metadata may progress during a catalog write: "+change, t, func() {
			s := newPlacementTestState(t, []string{"rg"})
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

func TestReplicaPlacementBlockedTargetsRetainCapacity(t *testing.T) {
	a := placementReplica(1, 10, "rg", 1, 2, 3, 4)
	b := placementReplica(2, 10, "rg", 5, 6, 7, 8)
	plan := map[int64][]int64{1: {5, 6}, 2: {7, 8}}
	a = applyReplicaPlacementPlan(a, []*Replica{a, b}, []int64{1, 2, 3, 4, 5, 6, 7, 8}, plan)
	require.ElementsMatch(t, []int64{1, 2}, a.GetRWNodes())
	b = applyReplicaPlacementPlan(b, []*Replica{a, b}, []int64{1, 2, 3, 4, 5, 6, 7, 8}, plan)
	require.ElementsMatch(t, []int64{5, 6}, b.GetRONodes())
	mutable := b.CopyForWrite()
	mutable.RemoveNode(5, 6)
	b = mutable.IntoReplica()
	a = applyReplicaPlacementPlan(a, []*Replica{a, b}, []int64{1, 2, 3, 4, 5, 6, 7, 8}, plan)
	require.ElementsMatch(t, []int64{5, 6}, a.GetRWNodes())
}

func TestReplicaPlacementAutomaticMembers(t *testing.T) {
	mockey.PatchConvey("unknown joining/releasing member never widens existing placement", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		require.NoError(t, s.recover(10))
		before := map[int64][]int64{10: slices.Clone(s.m.Get(context.Background(), 10).GetRWNodes()), 20: slices.Clone(s.m.Get(context.Background(), 20).GetRWNodes())}
		s.m.putReplicasInMemory(30, placementReplica(30, 30, "rg"))
		require.Error(t, s.recover(30))
		require.Len(t, s.m.Get(context.Background(), 30).GetRWNodes(), 4)
		for id, nodes := range before {
			require.ElementsMatch(t, nodes, s.m.Get(context.Background(), id).GetRWNodes())
		}
		require.Equal(t, 2, s.calls)
		s.m.collectionPartitions[30] = typeutil.NewSet[int64](31)
		s.rows[30] = 400
		require.NoError(t, s.recover(20))
		require.Equal(t, 5, s.calls)
		require.Len(t, s.m.Get(context.Background(), 30).GetRWNodes(), 2)
		before[10] = slices.Clone(s.m.Get(context.Background(), 10).GetRWNodes())
		before[20] = slices.Clone(s.m.Get(context.Background(), 20).GetRWNodes())
		delete(s.m.collectionPartitions, 30)
		require.Error(t, s.recover(10))
		for id, nodes := range before {
			require.ElementsMatch(t, nodes, s.m.Get(context.Background(), id).GetRWNodes())
		}
		s.m.coll2Replicas.Remove(30)
		require.NoError(t, s.recover(20))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 1)
	})
	mockey.PatchConvey("multi-shard excluded; empty channels cannot masquerade as single-shard", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		s.shards = map[int64]int{20: 2}
		require.NoError(t, s.recover(10))
		require.NoError(t, s.recover(20))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 4)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 4)
		require.NotContains(t, s.m.placementPolicy.groups["rg"].rows, int64(20))
		s.m.placementPolicy = nil
		s.shards[20] = 0
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		require.Zero(t, s.writes)
	})
	mockey.PatchConvey("new ready member joining during RPC participates only in a fresh snapshot", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		require.NoError(t, s.recover(10))
		before := slices.Clone(s.m.Get(context.Background(), 20).GetRWNodes())
		s.m.placementPolicy.groups["rg"].refreshed = time.Time{}
		s.duringRows = func() {
			s.m.putReplicasInMemory(30, placementReplica(30, 30, "rg"))
			s.m.collectionPartitions[30] = typeutil.NewSet[int64](31)
			s.rows[30] = 400
		}
		require.ErrorIs(t, s.recover(10), merr.ErrServiceUnavailable)
		require.ElementsMatch(t, before, s.m.Get(context.Background(), 20).GetRWNodes())
		require.NoError(t, s.recover(30))
		require.Len(t, s.m.Get(context.Background(), 30).GetRWNodes(), 2)
	})
}

func TestReplicaPlacementCrossRG(t *testing.T) {
	mockey.PatchConvey("same collection weights are independent per RG; RO still blocks ownership", t, func() {
		s := newPlacementTestState(t, []string{"*"})
		s.m.putReplicasInMemory(10, placementReplica(11, 10, "other", 5, 6))
		s.m.putReplicasInMemory(20, placementReplica(21, 20, "other", 5, 6))
		require.NoError(t, s.recover(10))
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 11).GetRWNodes(), 2)
		require.Len(t, s.m.Get(context.Background(), 21).GetRWNodes(), 1)
		// The RG grows into a node still RO in the collection's old RG.
		old := s.m.Get(context.Background(), 11).CopyForWrite()
		old.AddRONode(7)
		require.NoError(t, s.m.Put(context.Background(), old.IntoReplica()))
		s.m.groups["rg"].nodes = typeutil.NewSet[int64](7, 8, 9, 10)
		require.NoError(t, s.recover(20))
		require.NotContains(t, s.m.Get(context.Background(), 10).GetRWNodes(), int64(7))
		for _, id := range []int64{10, 20} {
			used := typeutil.NewSet[int64]()
			for _, r := range s.m.GetByCollection(context.Background(), id) {
				for _, n := range append(slices.Clone(r.GetRWNodes()), r.GetRONodes()...) {
					require.False(t, used.Contain(n))
					used.Insert(n)
				}
			}
		}
	})
}

func TestReplicaPlacementUncertainWriteReservesNodesAcrossRGs(t *testing.T) {
	mockey.PatchConvey("lost response reserves new nodes until a published full value is durable", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		s.m.putReplicasInMemory(10, placementReplica(10, 10, "rg", 1))
		s.m.putReplicasInMemory(10, placementReplica(11, 10, "other", 5))
		s.failWrite, s.failAfterSave = 1, true
		require.Error(t, s.recover(10))
		require.NotNil(t, s.m.placementPolicy.groups["rg"].pending)
		require.Contains(t, s.m.Get(context.Background(), 10).GetRONodes(), int64(2))
		// Move a possibly acquired node to another RG before settling. Even direct
		// ordinary recovery cannot claim it for a sibling while the save is uncertain.
		s.m.groups["rg"].nodes.Remove(2)
		s.m.groups["other"].nodes.Insert(2)
		rgs, _ := s.m.GetResourceGroups(context.Background(), []string{"rg", "other"})
		require.NoError(t, s.m.recoverNodesInCollection(context.Background(), 10, rgs, func(name string) bool { return name == "rg" }))
		require.NotContains(t, s.m.Get(context.Background(), 11).GetRWNodes(), int64(2))
		s.duringSave = func() {
			require.NotContains(t, s.persisted[10].GetNodes(), int64(2), "settlement does not blindly reacquire the stale RW destination")
		}
		require.NoError(t, s.recover(10))
		require.NotContains(t, s.m.Get(context.Background(), 10).GetRWNodes(), int64(2))
		require.Nil(t, s.m.placementPolicy.groups["rg"].pending)
	})
}

func TestReplicaPlacementRowsPublishCompleteBatch(t *testing.T) {
	mockey.PatchConvey("a later RPC failure must not publish partially refreshed weights", t, func() {
		s := newPlacementTestState(t, []string{"rg"})
		require.NoError(t, s.recover(10))
		g := s.m.placementPolicy.groups["rg"]
		before := g.rows
		s.rows[10], s.rows[20] = 100, 300
		s.failRowsFor = 20
		g.refreshed = time.Time{}
		require.Error(t, s.recover(20))
		require.Equal(t, before, g.rows)
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 3)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 1)
		s.failRowsFor = 0
		require.NoError(t, s.recover(10))
		require.Equal(t, s.rows, g.rows)
		require.Len(t, s.m.Get(context.Background(), 10).GetRWNodes(), 1)
		require.Len(t, s.m.Get(context.Background(), 20).GetRWNodes(), 3)
	})
}
