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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// captureRouter records the routing commit the split task sends.
type captureRouter struct {
	last *rootcoordpb.CommitShardSplitRoutingRequest
	err  error
}

func (c *captureRouter) CommitShardSplitRouting(_ context.Context, req *rootcoordpb.CommitShardSplitRoutingRequest) error {
	c.last = req
	return c.err
}

func newRoutingCommitManager(t *testing.T, vchannels []string, shardInfos map[string]*schemapb.CollectionShardInfo) (*shardSplitManager, *captureRouter) {
	return newRoutingCommitManagerAt(t, 0, vchannels, shardInfos)
}

// newRoutingCommitManagerAt builds the manager over a collection at the given
// routing modulus. Zero means the collection has never been split, which is the
// only case in which its shards may carry no residues.
func newRoutingCommitManagerAt(t *testing.T, modulus uint64, vchannels []string, shardInfos map[string]*schemapb.CollectionShardInfo) (*shardSplitManager, *captureRouter) {
	m := newHashRewriteMeta(nil)
	coll, _ := m.collections.Get(1)
	coll.VChannelNames = vchannels
	coll.ShardInfos = shardInfos
	coll.RoutingModulus = modulus
	coll.DatabaseName = "db"

	mgr, _ := newHashSplitTestManager(t, m)
	router := &captureRouter{}
	mgr.router = router
	return mgr, router
}

func TestCommitHashRoutingDoubling(t *testing.T) {
	mgr, router := newRoutingCommitManager(t, []string{hashSrcVChannel}, nil)
	task := fenceSources(newHashTask(nil), 100) // targets: {4,0} and {4,2}
	coll, _ := mgr.meta.collections.Get(1)

	require.NoError(t, mgr.commitRouting(task, coll,
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating))

	req := router.last
	require.NotNil(t, req)
	assert.NotZero(t, req.GetRoutingModulus())
	// Source plus the two targets.
	assert.ElementsMatch(t, []string{hashSrcVChannel, hashTgtA, hashTgtB}, req.GetVirtualChannelNames())

	byVChannel := map[string]*schemapb.CollectionShardInfo{}
	for i, v := range req.GetVirtualChannelNames() {
		byVChannel[v] = req.GetShardInfos()[i]
	}

	// The source is fenced and carries NO predicate: its key space now belongs
	// to the targets, which is what keeps the survivors an exact cover.
	src := byVChannel[hashSrcVChannel]
	assert.Equal(t, schemapb.ShardState_ShardSplitting, src.GetState())
	assert.Nil(t, src.GetRouting())

	// Each target carries its hash bucket and points back at the source.
	a := byVChannel[hashTgtA]
	assert.Equal(t, schemapb.ShardState_ShardCreating, a.GetState())
	assert.Equal(t, []uint64{0}, a.GetHashRouting().GetBuckets())
	assert.Equal(t, hashTgtA, a.GetVchannelName())
	assert.EqualValues(t, 4, req.GetRoutingModulus())

	b := byVChannel[hashTgtB]
	assert.Equal(t, []uint64{2}, b.GetHashRouting().GetBuckets())
}

func TestCommitHashRoutingCarriesOtherShardsThrough(t *testing.T) {
	// Splitting one shard of a multi-shard collection must leave the others'
	// routing exactly as it was, whatever predicate variant they carry.
	other := "by-dev-rootcoord-dml_9_100v0"
	otherInfo := &schemapb.CollectionShardInfo{
		State:                schemapb.ShardState_ShardNormal,
		VchannelName:         other,
		LastTruncateTimeTick: 4242,
		Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{1}},
		},
	}
	// A two-shard collection at modulus 2: the source owns residue 0, the
	// untouched shard residue 1.
	mgr, router := newRoutingCommitManagerAt(t, 2,
		[]string{hashSrcVChannel, other},
		map[string]*schemapb.CollectionShardInfo{
			hashSrcVChannel: {
				State: schemapb.ShardState_ShardNormal, VchannelName: hashSrcVChannel,
				Routing: &schemapb.CollectionShardInfo_HashRouting{
					HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
				},
			},
			other: otherInfo,
		})
	task := fenceSources(newHashTask(nil), 100)
	coll, _ := mgr.meta.collections.Get(1)

	require.NoError(t, mgr.commitRouting(task, coll,
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating))

	byVChannel := map[string]*schemapb.CollectionShardInfo{}
	for i, v := range router.last.GetVirtualChannelNames() {
		byVChannel[v] = router.last.GetShardInfos()[i]
	}
	carried := byVChannel[other]
	require.NotNil(t, carried)
	assert.Equal(t, schemapb.ShardState_ShardNormal, carried.GetState())
	assert.Equal(t, uint64(4242), carried.GetLastTruncateTimeTick())
	// The untouched shard owned residue 1 at modulus 2; the doubling re-expresses
	// its keys as residues 1 and 3 at modulus 4, which is the SAME set of keys.
	assert.Equal(t, []uint64{1, 3}, carried.GetHashRouting().GetBuckets())
}

func TestCommittedTopologyDerivesIntoAWorkingTable(t *testing.T) {
	// End-to-end on the routing contract: what the commit sends must be
	// derivable into a table that routes every key, with the fenced source
	// excluded.
	//
	// The source here is the collection's only shard, so it owned the whole key
	// space (residue 0 at modulus 1); its doubling gives residues 0 and 1 at
	// modulus 2, which must tile that space exactly once the source is filtered
	// out.
	mgr, router := newRoutingCommitManager(t, []string{hashSrcVChannel}, nil)
	task := fenceSources(newHashTask(nil), 100)
	task.Targets = []*datapb.SplitShardTaskTarget{
		{Vchannel: hashTgtA, Buckets: []uint64{0}},
		{Vchannel: hashTgtB, Buckets: []uint64{1}},
	}
	task.RoutingModulus = 2
	coll, _ := mgr.meta.collections.Get(1)
	require.NoError(t, mgr.commitRouting(task, coll,
		schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating))

	shards, err := routing.ShardsFromMeta(router.last.GetVirtualChannelNames(), router.last.GetShardInfos())
	require.NoError(t, err)
	// The fenced source is filtered out; the two targets remain.
	require.Len(t, shards, 2)

	tbl, err := routing.Derive(router.last.GetRoutingModulus(),
		router.last.GetVirtualChannelNames(), shards)
	require.NoError(t, err, "the committed topology must tile the key space")

	// Every hash the source owned — the whole space — now routes to one of its
	// targets.
	for h := uint64(0); h < 200; h++ {
		ch, err := tbl.Route(h)
		require.NoError(t, err)
		assert.Contains(t, []string{hashTgtA, hashTgtB}, ch, "hash %d", h)
	}
}

func TestToMessageHashSplitTargetsCarriesVChannels(t *testing.T) {
	// A doubling's single source fronts both targets.
	task := newHashTask(nil)
	converted := toMessageHashSplitTargets(task, hashSrcVChannel)
	require.Len(t, converted, 2)
	assert.Equal(t, hashTgtA, converted[0].GetVchannel())
	assert.Equal(t, hashTgtB, converted[1].GetVchannel())

	// A vchannel that is not a source of the task fronts nothing.
	assert.Empty(t, toMessageHashSplitTargets(task, "by-dev-rootcoord-dml_9_100v0"))
}

func TestAllMessageHashSplitTargetsCarriesEveryTarget(t *testing.T) {
	// Creating the target vchannels addresses the targets themselves, not any
	// one source's fronting duty, so it takes the full list.
	converted := allMessageHashSplitTargets([]*datapb.SplitShardTaskTarget{
		{Vchannel: hashTgtA, Buckets: []uint64{0}},
		{Vchannel: hashTgtB, Buckets: []uint64{2}},
	})
	require.Len(t, converted, 2)
	assert.Equal(t, hashTgtA, converted[0].GetVchannel())
	assert.Equal(t, hashTgtB, converted[1].GetVchannel())
}

func TestCarryThroughShardInfoPBHandlesNil(t *testing.T) {
	info := carryThroughShardInfoPB(nil)
	assert.Equal(t, schemapb.ShardState_ShardNormal, info.GetState())
	assert.Nil(t, info.GetRouting())
}

// The fence message is the permanent record a delegator derives the split
// window's fronting assignment from, and the streamingnode refuses a target
// that carries no residue -- so the per-source target list must carry each
// fronted target's residues, not only its name. This was the only live fence
// builder and it dropped them; the fence parameter's own validation is what
// caught it.
func TestMessageSplitTargetsCarryTheirResidues(t *testing.T) {
	task := &datapb.SplitShardTask{
		TaskId:         100,
		CollectionId:   1,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: "v0"}},
		RoutingModulus: 2,
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: "v1", Buckets: []uint64{0}},
			{Vchannel: "v2", Buckets: []uint64{1}},
		},
	}
	for _, targets := range [][]*message.SplitShardTarget{
		toMessageHashSplitTargets(task, "v0"),
		allMessageHashSplitTargets(task.GetTargets()),
	} {
		require.Len(t, targets, 2)
		for i, target := range targets {
			assert.Equal(t, task.Targets[i].GetVchannel(), target.GetVchannel())
			assert.Equal(t, task.Targets[i].GetBuckets(), target.GetRouting().GetBuckets())
		}
		// and the fence parameter built from them passes its own validation.
		param := streaming.SplitShardParam{CollectionID: 1, SourceVChannel: "v0", SplitTaskID: 100, RoutingModulus: 2, Targets: targets}
		require.NoError(t, param.Validate())
	}

	// Mutating the message must not reach back into the task.
	msgTargets := toMessageHashSplitTargets(task, "v0")
	msgTargets[0].Routing.Buckets[0] = 99
	assert.Equal(t, []uint64{0}, task.Targets[0].GetBuckets())
}
