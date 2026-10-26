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

package balance

import (
	"context"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// nopCatalog persists nothing; only SaveReplica is reached here.
type nopCatalog struct{ metastore.QueryCoordCatalog }

func (nopCatalog) SaveReplica(ctx context.Context, replicas ...*querypb.Replica) error { return nil }

// movableTarget lets every segment move; only CanSegmentBeMoved is reached.
type movableTarget struct{ meta.TargetManagerInterface }

func (movableTarget) CanSegmentBeMoved(ctx context.Context, collectionID, segmentID int64) bool {
	return true
}

// TestTwoLevelScale measures one global balance round and placement calls
// with many small collections (TWO_LEVEL_SCALE, default 100000) beside two
// big ones on 16 nodes. Run with TWO_LEVEL_SCALE=1000000 for a million.
func TestTwoLevelScale(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("TWO_LEVEL_SCALE"))
	if n == 0 {
		t.Skip("set TWO_LEVEL_SCALE to run")
	}
	paramtable.Init()
	assign.ResetTwoLevelStateForTest()
	const nodes = 16
	ctx := context.Background()
	nodeMgr := session.NewNodeManager()
	rwNodes := typeutil.NewUniqueSet()
	for i := int64(1); i <= nodes; i++ {
		info := session.NewNodeInfo(session.ImmutableNodeInfo{NodeID: i, Address: "n" + strconv.FormatInt(i, 10)})
		info.UpdateStats(session.WithMemCapacity(1024*1024), session.WithCPUNum(32))
		nodeMgr.Add(info)
		rwNodes.Insert(i)
	}
	var nextID int64 = 1 << 40
	m := meta.NewMeta(func() (int64, error) { nextID++; return nextID, nil }, nopCatalog{}, nodeMgr, metacache.NewMetaStore(nil))
	dist := meta.NewDistributionManager(nodeMgr)
	target := movableTarget{}
	sched := task.NewMockScheduler(t)
	sched.EXPECT().GetSegmentTaskNum(mock.Anything).Return(0).Maybe()
	sched.EXPECT().GetSegmentTaskNum().Return(0).Maybe()
	sched.EXPECT().GetSegmentTaskDeltaSnapshot(mock.Anything, mock.Anything).Return(task.NewSegmentTaskDeltaSnapshot(nil, nil)).Maybe()
	sched.EXPECT().GetChannelTaskDelta(mock.Anything, mock.Anything).Return(0).Maybe()
	sched.EXPECT().GetChannelTaskNum(mock.Anything, mock.Anything).Return(0).Maybe()
	assign.ResetGlobalAssignPolicyFactoryForTest()
	assign.InitGlobalAssignPolicyFactory(sched, nodeMgr, dist, m, target)

	skew, err := strconv.ParseFloat(os.Getenv("TWO_LEVEL_SKEW"), 64)
	if err != nil {
		skew = 0.1
	}
	rng := rand.New(rand.NewSource(1))
	perNode := make([][]*meta.Segment, nodes+1)
	seg := int64(0)
	skewFrac := skew
	addColl := func(coll int64, segs int, bytes func() int64, skew bool) {
		r := meta.NewReplica(&querypb.Replica{ID: coll, CollectionID: coll, ResourceGroup: meta.DefaultResourceGroupName}, rwNodes)
		if err := m.Put(ctx, r); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < segs; i++ {
			seg++
			node := int64(rng.Intn(nodes)) + 1
			if skew && rng.Float64() < skewFrac {
				node = 1 // a share of the small segments pile onto node 1
			}
			s := &meta.Segment{
				SegmentInfo: &datapb.SegmentInfo{
					ID: seg, CollectionID: coll, NumOfRows: 1000, InsertChannel: "ch",
					Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: bytes()}}}},
				},
				Node: node,
			}
			perNode[node] = append(perNode[node], s)
		}
	}
	addColl(1, 4000, func() int64 { return 512 << 20 }, false)
	addColl(2, 4000, func() int64 { return 256 << 20 }, false)
	for c := 0; c < n; c++ {
		addColl(int64(10+c), 1+rng.Intn(2), func() int64 { return int64(1+rng.Intn(8)) << 20 }, true)
	}
	for i := int64(1); i <= nodes; i++ {
		dist.SegmentDistManager.Update(i, perNode[i]...)
	}
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("setup: %d small collections, %d segments, heap %d MB", n, seg, ms.HeapAlloc>>20)

	b := NewTwoLevelBalancer(sched, nodeMgr, dist, target)
	for round := 0; round < 3; round++ {
		start := time.Now()
		plans := b.BalanceGlobal(ctx, m)
		t.Logf("round %d: %d plans in %v", round, len(plans), time.Since(start))
	}
	runtime.ReadMemStats(&ms)
	t.Logf("heap after rounds %d MB", ms.HeapAlloc>>20)

	// Baseline: master's score based balancer, one replica at a time (the
	// balance checker visits balanceCheckCollectionMaxCount per round).
	sb := NewScoreBasedBalancer(sched, nodeMgr, dist, target)
	start0 := time.Now()
	const sample = 20
	for i := 0; i < sample; i++ {
		sb.BalanceReplica(ctx, m.Get(ctx, int64(10+rng.Intn(n))))
	}
	per := time.Since(start0) / sample
	t.Logf("score based: %v per replica; %v per round of 100; a full pass over %d collections takes %d rounds",
		per, per*100, n, (n+99)/100)

	policy := b.GetAssignPolicy()
	start := time.Now()
	const calls = 2000
	for i := 0; i < calls; i++ {
		seg++
		s := &meta.Segment{SegmentInfo: &datapb.SegmentInfo{
			ID: seg, CollectionID: int64(10 + rng.Intn(n)), NumOfRows: 1000,
			Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 8 << 20}}}},
		}}
		policy.AssignSegment(ctx, s.GetCollectionID(), []*meta.Segment{s}, rwNodes.Collect(), true)
	}
	t.Logf("placement: %v per call (view cached between dist changes)", time.Since(start)/calls)
}
