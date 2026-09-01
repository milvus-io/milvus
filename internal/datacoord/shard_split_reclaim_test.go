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

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/broker"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	reclaimSrc  = "by-dev-rootcoord-dml_0_777v0"
	reclaimTgtA = "by-dev-rootcoord-dml_1_777v1"
	reclaimTgtB = "by-dev-rootcoord-dml_2_777v2"
)

func hashShard(vchannel string, state schemapb.ShardState, residues ...uint64) *schemapb.CollectionShardInfo {
	info := &schemapb.CollectionShardInfo{VchannelName: vchannel, State: state}
	if len(residues) > 0 {
		info.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: residues},
		}
	}
	return info
}

// newReclaimMeta builds a collection that has completed a doubling: the source
// is retired but still held, the two targets are live.
func newReclaimMeta() *meta {
	m := &meta{
		ctx:         context.Background(),
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		channelCPs:  newChannelCps(),
	}
	m.collections.Insert(777, &collectionInfo{
		ID:             777,
		DatabaseName:   "default",
		Schema:         &schemapb.CollectionSchema{Name: "reclaim_test"},
		VChannelNames:  []string{reclaimSrc, reclaimTgtA, reclaimTgtB},
		RoutingModulus: 2,
		ShardInfos: map[string]*schemapb.CollectionShardInfo{
			reclaimSrc:  hashShard(reclaimSrc, schemapb.ShardState_ShardDropped, 0),
			reclaimTgtA: hashShard(reclaimTgtA, schemapb.ShardState_ShardNormal, 0),
			reclaimTgtB: hashShard(reclaimTgtB, schemapb.ShardState_ShardNormal, 1),
		},
	})
	return m
}

func newReclaimManager(t *testing.T, m *meta) (*shardSplitManager, *broker.MockBroker, *mock_streaming.MockWALAccesser) {
	router := broker.NewMockBroker(t)
	wal := mock_streaming.NewMockWALAccesser(t)
	mgr := &shardSplitManager{
		ctx:    context.Background(),
		meta:   m,
		router: router,
		wal:    wal,
		tasks:  typeutil.NewConcurrentMap[int64, *datapb.SplitShardTask](),
	}
	return mgr, router, wal
}

// expectTeardown records the DropVChannel appends a reclaim must make before it
// touches the collection meta.
func expectTeardown(wal *mock_streaming.MockWALAccesser, torn *[]string) {
	wal.EXPECT().RawAppend(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, msg message.MutableMessage, _ ...streaming.AppendOption) (*types.AppendResult, error) {
			if msg.MessageType() == message.MessageTypeDropVChannel {
				*torn = append(*torn, msg.VChannel())
			}
			return &types.AppendResult{MessageID: rmq.NewRmqID(1), TimeTick: 10}, nil
		}).Maybe()
}

func TestReclaimDropsARetiredSourceAndGivesTheSlotBack(t *testing.T) {
	m := newReclaimMeta()
	mgr, router, wal := newReclaimManager(t, m)

	var torn []string
	expectTeardown(wal, &torn)
	var committed *rootcoordpb.CommitShardSplitRoutingRequest
	router.EXPECT().CommitShardSplitRouting(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *rootcoordpb.CommitShardSplitRoutingRequest) error {
			committed = req
			return nil
		}).Once()

	require.NoError(t, mgr.reclaimRetiredVChannels(777))

	// The WAL teardown must have happened, and BEFORE the meta drop: a vchannel
	// removed from the collection first would never receive one, because
	// DropCollection is broadcast to exactly VirtualChannelNames.
	assert.Equal(t, []string{reclaimSrc}, torn)

	require.NotNil(t, committed)
	// The retired source is gone from all three parallel arrays; rootcoord
	// rebuilds ShardInfos from them, so a shorter list is the whole removal.
	assert.Equal(t, []string{reclaimTgtA, reclaimTgtB}, committed.GetVirtualChannelNames())
	assert.Equal(t, []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"},
		committed.GetPhysicalChannelNames())
	require.Len(t, committed.GetShardInfos(), 2)
	// The survivors keep the exact predicate they had: reclamation retires dead
	// channels, it never re-derives routing.
	residuesOfShard := func(i int) []uint64 {
		return committed.GetShardInfos()[i].GetHashRouting().GetBuckets()
	}
	assert.Equal(t, []uint64{0}, residuesOfShard(0))
	assert.Equal(t, []uint64{1}, residuesOfShard(1))
	assert.EqualValues(t, 2, committed.GetRoutingModulus())

	// datacoord's own cached view must follow, or the next commit rebuilds its
	// list from the stale names and resurrects the reclaimed channel.
	assert.Equal(t, []string{reclaimTgtA, reclaimTgtB},
		m.GetCollection(777).VChannelNames)
}

func TestReclaimSkipsAChannelStillReferencedByATask(t *testing.T) {
	m := newReclaimMeta()
	mgr, _, _ := newReclaimManager(t, m)
	// A task that is already Done but not yet reaped. datacoord calls the split
	// done before querycoord releases the source, so reclaiming here would race
	// the handover.
	mgr.tasks.Insert(9, &datapb.SplitShardTask{
		TaskId:       9,
		CollectionId: 777,
		State:        datapb.SplitShardTaskState_SplitShardTaskDone,
		Sources:      []*datapb.SplitShardTaskSource{{Vchannel: reclaimSrc}},
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: reclaimTgtA}, {Vchannel: reclaimTgtB},
		},
	})

	// No CommitShardSplitRouting expectation: the mock fails the test if called.
	require.NoError(t, mgr.reclaimRetiredVChannels(777))
	assert.Len(t, m.GetCollection(777).VChannelNames, 3)
}

func TestReclaimSkipsAChannelThatStillHoldsLiveSegments(t *testing.T) {
	m := newReclaimMeta()
	m.segments.SetSegment(4001, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            4001,
			CollectionID:  777,
			InsertChannel: reclaimSrc,
			State:         commonpb.SegmentState_Flushed,
			NumOfRows:     10,
		},
	})
	mgr, _, _ := newReclaimManager(t, m)

	require.NoError(t, mgr.reclaimRetiredVChannels(777))
	assert.Len(t, m.GetCollection(777).VChannelNames, 3,
		"a channel that still holds reachable data must not be removed")
}

// Dropped segments are the NORMAL state of a retired source: adoption retires
// them in place and GC removes them much later. Counting them would make a
// retired source effectively unreclaimable, which is exactly what the first
// end-to-end run showed -- nothing was ever reclaimed.
func TestReclaimIgnoresDroppedSegments(t *testing.T) {
	m := newReclaimMeta()
	m.segments.SetSegment(4002, &SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            4002,
			CollectionID:  777,
			InsertChannel: reclaimSrc,
			State:         commonpb.SegmentState_Dropped,
			NumOfRows:     10,
		},
	})
	mgr, router, wal := newReclaimManager(t, m)
	var torn []string
	expectTeardown(wal, &torn)
	router.EXPECT().CommitShardSplitRouting(mock.Anything, mock.Anything).Return(nil).Once()

	require.NoError(t, mgr.reclaimRetiredVChannels(777))
	assert.Equal(t, []string{reclaimSrc}, torn)
	assert.Len(t, m.GetCollection(777).VChannelNames, 2)
}

func TestReclaimLeavesLiveShardsAlone(t *testing.T) {
	m := newReclaimMeta()
	// Nothing retired: a collection mid-split carries Splitting, not Dropped.
	coll := m.GetCollection(777)
	coll.ShardInfos[reclaimSrc] = hashShard(reclaimSrc, schemapb.ShardState_ShardSplitting, 0)
	m.AddCollection(coll)
	mgr, _, _ := newReclaimManager(t, m)

	require.NoError(t, mgr.reclaimRetiredVChannels(777))
	assert.Len(t, m.GetCollection(777).VChannelNames, 3)
}

func TestReclaimRefusesToEmptyACollection(t *testing.T) {
	m := newReclaimMeta()
	coll := m.GetCollection(777)
	coll.VChannelNames = []string{reclaimSrc}
	coll.ShardInfos = map[string]*schemapb.CollectionShardInfo{
		reclaimSrc: hashShard(reclaimSrc, schemapb.ShardState_ShardDropped, 0),
	}
	m.AddCollection(coll)
	mgr, _, _ := newReclaimManager(t, m)

	err := mgr.reclaimRetiredVChannels(777)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "refuse to reclaim every vchannel")
}

func TestReclaimIsANoOpWithoutARouter(t *testing.T) {
	m := newReclaimMeta()
	mgr := &shardSplitManager{
		ctx:   context.Background(),
		meta:  m,
		tasks: typeutil.NewConcurrentMap[int64, *datapb.SplitShardTask](),
	}
	// router nil: wired during server init, must not be dereferenced before.
	mgr.reclaimRetiredVChannelsOnce()
	assert.Len(t, m.GetCollection(777).VChannelNames, 3)
}

// The provenance a split records -- which sources a target was carved from --
// is not in the collection meta at all: it lives in the split task and is
// discarded with it, so reclamation has nothing to clear. The commit below
// asserts what remains: the survivors keep exactly the residues they had.
func TestReclaimLeavesTheSurvivorsRoutingUntouched(t *testing.T) {
	m := newReclaimMeta()
	mgr, router, wal := newReclaimManager(t, m)

	var torn []string
	expectTeardown(wal, &torn)
	var committed *rootcoordpb.CommitShardSplitRoutingRequest
	router.EXPECT().CommitShardSplitRouting(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *rootcoordpb.CommitShardSplitRoutingRequest) error {
			committed = req
			return nil
		}).Once()

	require.NoError(t, mgr.reclaimRetiredVChannels(777))
	require.NotNil(t, committed)
	require.Len(t, committed.GetShardInfos(), 2)

	// This commit retires channels; it never re-derives routing. Each survivor
	// keeps exactly the residues it had, and the modulus is unchanged.
	assert.Equal(t, []uint64{0}, committed.GetShardInfos()[0].GetHashRouting().GetBuckets())
	assert.Equal(t, []uint64{1}, committed.GetShardInfos()[1].GetHashRouting().GetBuckets())
	assert.EqualValues(t, 2, committed.GetRoutingModulus())
}
