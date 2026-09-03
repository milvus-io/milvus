// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// pbShard builds a CollectionShardInfo owning the given residues, for test
// fixtures.
func pbShard(state schemapb.ShardState, buckets ...uint64) *schemapb.CollectionShardInfo {
	si := &schemapb.CollectionShardInfo{State: state}
	if len(buckets) > 0 {
		si.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: buckets},
		}
	}
	return si
}

func TestDDLCallbacksCommitShardSplitRouting(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	ctx := context.Background()
	dbName := "testDB" + funcutil.RandomString(10)
	collectionName := "testSplitRouting" + funcutil.RandomString(10)

	// a single-shard collection so the post-split topology is exactly the
	// source plus the two split targets.
	resp, err := core.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{DbName: dbName})
	require.NoError(t, merr.CheckRPCCall(resp, err))
	schemaBytes, err := proto.Marshal(&schemapb.CollectionSchema{
		Name:   collectionName,
		Fields: []*schemapb.FieldSchema{{Name: "field1", DataType: schemapb.DataType_Int64}},
	})
	require.NoError(t, err)
	resp, err = core.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Schema:           schemaBytes,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
		ShardsNum:        1,
	})
	require.NoError(t, merr.CheckRPCCall(resp, err))

	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	require.Len(t, coll.VirtualChannelNames, 1)
	source := coll.VirtualChannelNames[0]
	collID := coll.CollectionID
	t1, t2 := source+"_split1", source+"_split2"

	buildReq := func(sourceState, targetState schemapb.ShardState) *rootcoordpb.CommitShardSplitRoutingRequest {
		return &rootcoordpb.CommitShardSplitRoutingRequest{
			DbName:               dbName,
			CollectionName:       collectionName,
			CollectionId:         collID,
			VirtualChannelNames:  []string{source, t1, t2},
			PhysicalChannelNames: []string{funcutil.ToPhysicalChannel(source), funcutil.ToPhysicalChannel(t1), funcutil.ToPhysicalChannel(t2)},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: sourceState},
				pbShard(targetState, 0),
				pbShard(targetState, 1),
			},
			RoutingModulus: 2,
			ShardBy:        "hash(pk)",
		}
	}

	assertStates := func(sourceState, targetState schemapb.ShardState) {
		coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
		require.NoError(t, err)
		require.ElementsMatch(t, []string{source, t1, t2}, coll.VirtualChannelNames)
		require.EqualValues(t, 2, coll.RoutingModulus)
		require.Equal(t, "hash(pk)", coll.ShardBy)
		require.Equal(t, sourceState, coll.ShardInfos[source].State)
		require.Equal(t, targetState, coll.ShardInfos[t1].State)
		require.Equal(t, targetState, coll.ShardInfos[t2].State)
		require.Equal(t, []uint64{0}, coll.ShardInfos[t1].Buckets)
		require.Equal(t, []uint64{1}, coll.ShardInfos[t2].Buckets)
	}

	// write-switch commit: source Splitting, two targets Creating.
	resp, err = core.CommitShardSplitRouting(ctx, buildReq(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertStates(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating)

	// idempotent: re-committing the same states is a no-op success.
	resp, err = core.CommitShardSplitRouting(ctx, buildReq(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertStates(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating)

	// adoption commit: source Dropped, targets Normal.
	resp, err = core.CommitShardSplitRouting(ctx, buildReq(schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardNormal))
	require.NoError(t, merr.CheckRPCCall(resp, err))
	assertStates(schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardNormal)

	// empty collection name is rejected.
	resp, err = core.CommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{DbName: dbName, CollectionId: collID})
	require.Error(t, merr.CheckRPCCall(resp, err))

	// channel and shard-info arrays must be parallel.
	bad := buildReq(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating)
	bad.ShardInfos = bad.ShardInfos[:2]
	resp, err = core.CommitShardSplitRouting(ctx, bad)
	require.Error(t, merr.CheckRPCCall(resp, err))

	// a collection that does not exist is rejected.
	resp, err = core.CommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		DbName:               util.DefaultDBName,
		CollectionName:       "does_not_exist",
		CollectionId:         424242,
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
		ShardInfos:           []*schemapb.CollectionShardInfo{pbShard(schemapb.ShardState_ShardNormal, 0)},
		RoutingModulus:       1,
	})
	require.Error(t, merr.CheckRPCCall(resp, err))

	// a topology that does not tile the key space is refused before the
	// broadcast: committing it would silently drop the writes of the residues
	// nobody claims.
	gap := buildReq(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating)
	gap.RoutingModulus = 4
	resp, err = core.CommitShardSplitRouting(ctx, gap)
	require.Error(t, merr.CheckRPCCall(resp, err))
}

// TestCommitShardSplitRoutingValidation covers the request validation that
// returns before the broadcast, so it needs no streaming system or etcd.
func TestCommitShardSplitRoutingValidation(t *testing.T) {
	c := &Core{}
	ctx := context.Background()

	// empty collection name.
	err := c.broadcastCommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	// no vchannels.
	err = c.broadcastCommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{CollectionName: "c"})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	// channel and shard-info arrays must be parallel.
	err = c.broadcastCommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		CollectionName:       "c",
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0"},
		ShardInfos:           []*schemapb.CollectionShardInfo{{}, {}},
	})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	// residues that overlap send one key to two shards.
	err = c.broadcastCommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		CollectionName:       "c",
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0", "p1"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0, 1),
			pbShard(schemapb.ShardState_ShardNormal, 1),
		},
		RoutingModulus: 2,
	})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	// residues with no modulus to read them against.
	err = c.broadcastCommitShardSplitRouting(ctx, &rootcoordpb.CommitShardSplitRoutingRequest{
		CollectionName:       "c",
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
		ShardInfos:           []*schemapb.CollectionShardInfo{pbShard(schemapb.ShardState_ShardNormal, 0)},
	})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestRoutingCommitAlreadyApplied(t *testing.T) {
	coll := &model.Collection{
		VirtualChannelNames: []string{"v0", "v1"},
		RoutingModulus:      2,
		ShardBy:             "hash(pk)",
		ShardInfos: map[string]*model.ShardInfo{
			"v0": {VChannelName: "v0", State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			"v1": {VChannelName: "v1", State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1}},
		},
	}
	req := func(modulus uint64, left, right []uint64) *rootcoordpb.CommitShardSplitRoutingRequest {
		return &rootcoordpb.CommitShardSplitRoutingRequest{
			VirtualChannelNames: []string{"v0", "v1"},
			RoutingModulus:      modulus,
			ShardInfos: []*schemapb.CollectionShardInfo{
				pbShard(schemapb.ShardState_ShardNormal, left...),
				pbShard(schemapb.ShardState_ShardNormal, right...),
			},
		}
	}

	require.True(t, routingCommitAlreadyApplied(coll, req(2, []uint64{0}, []uint64{1})))

	// A rebase onto a doubled modulus leaves every state alone and changes only
	// the residues. Comparing states alone would call this already committed and
	// silently drop it.
	require.False(t, routingCommitAlreadyApplied(coll, req(4, []uint64{0, 2}, []uint64{1, 3})))
	// Same modulus, different residues.
	require.False(t, routingCommitAlreadyApplied(coll, req(2, []uint64{1}, []uint64{0})))
	// A shard_by back-fill the collection does not carry yet.
	backfill := req(2, []uint64{0}, []uint64{1})
	backfill.ShardBy = "hash($namespace_id)"
	require.False(t, routingCommitAlreadyApplied(coll, backfill))
	// An empty shard_by asks for no back-fill, so it does not make the commit
	// look different.
	require.True(t, routingCommitAlreadyApplied(coll, req(2, []uint64{0}, []uint64{1})))
	// A vchannel the collection does not have.
	unknown := req(2, []uint64{0}, []uint64{1})
	unknown.VirtualChannelNames = []string{"v0", "v9"}
	require.False(t, routingCommitAlreadyApplied(coll, unknown))
}

func TestShardStateMayAdvance(t *testing.T) {
	all := []schemapb.ShardState{
		schemapb.ShardState_ShardNormal,
		schemapb.ShardState_ShardCreating,
		schemapb.ShardState_ShardSplitting,
		schemapb.ShardState_ShardDropped,
	}
	// Staying put is always allowed; that is what makes a retried commit a no-op.
	for _, s := range all {
		require.True(t, shardStateMayAdvance(s, s), s.String())
	}
	// Forward: fence a source, release it, adopt a target, abandon a target.
	require.True(t, shardStateMayAdvance(schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardSplitting))
	require.True(t, shardStateMayAdvance(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardDropped))
	require.True(t, shardStateMayAdvance(schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardNormal))
	require.True(t, shardStateMayAdvance(schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardDropped))
	// Backward: the fence is recorded in the WAL and cannot be undone, an adopted
	// target cannot go back to not-yet-serviceable, and Dropped is terminal.
	require.False(t, shardStateMayAdvance(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardNormal))
	require.False(t, shardStateMayAdvance(schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardCreating))
	require.False(t, shardStateMayAdvance(schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardSplitting))
	require.False(t, shardStateMayAdvance(schemapb.ShardState_ShardDropped, schemapb.ShardState_ShardNormal))
}

func TestCheckRoutingCommitAgainstMeta(t *testing.T) {
	// A collection mid-split: source fenced, two targets adopted.
	coll := &model.Collection{
		Name:                "c",
		VirtualChannelNames: []string{"v0", "v1", "v2"},
		RoutingModulus:      2,
		ShardInfos: map[string]*model.ShardInfo{
			"v0": {VChannelName: "v0", State: schemapb.ShardState_ShardDropped},
			"v1": {VChannelName: "v1", State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			"v2": {VChannelName: "v2", State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1}},
		},
	}

	// A late duplicate of the write-switch commit, arriving after adoption. With
	// no collection lock this is the lost update the check exists to stop: it
	// would put the released source back to fenced and un-adopt both targets.
	stale := &rootcoordpb.CommitShardSplitRoutingRequest{
		CollectionName:      "c",
		VirtualChannelNames: []string{"v0", "v1", "v2"},
		RoutingModulus:      2,
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardSplitting),
			pbShard(schemapb.ShardState_ShardCreating, 0),
			pbShard(schemapb.ShardState_ShardCreating, 1),
		},
	}
	require.ErrorIs(t, checkRoutingCommitAgainstMeta(coll, stale), merr.ErrParameterInvalid)

	// Routing is not revocable: a commit cannot take a split collection back to
	// no modulus, which would make it read as never-split and route by position
	// over a channel list that still holds the retired source.
	revoke := &rootcoordpb.CommitShardSplitRoutingRequest{
		CollectionName:      "c",
		VirtualChannelNames: []string{"v0", "v1", "v2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardDropped),
			pbShard(schemapb.ShardState_ShardNormal),
			pbShard(schemapb.ShardState_ShardNormal),
		},
	}
	require.ErrorIs(t, checkRoutingCommitAgainstMeta(coll, revoke), merr.ErrParameterInvalid)

	// Forward is fine: retire the source's vchannel and keep the two targets.
	forward := &rootcoordpb.CommitShardSplitRoutingRequest{
		CollectionName:      "c",
		VirtualChannelNames: []string{"v1", "v2"},
		RoutingModulus:      2,
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 1),
		},
	}
	require.NoError(t, checkRoutingCommitAgainstMeta(coll, forward))

	// A doubling that rebases every shard onto the new modulus is forward too.
	rebase := &rootcoordpb.CommitShardSplitRoutingRequest{
		CollectionName:      "c",
		VirtualChannelNames: []string{"v1", "v2", "v3"},
		RoutingModulus:      4,
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardSplitting),
			pbShard(schemapb.ShardState_ShardNormal, 1, 3),
			pbShard(schemapb.ShardState_ShardCreating, 0, 2),
		},
	}
	require.NoError(t, checkRoutingCommitAgainstMeta(coll, rebase))

	// A collection that has never been split may of course start at zero.
	fresh := &model.Collection{Name: "c", VirtualChannelNames: []string{"v0"}}
	require.NoError(t, checkRoutingCommitAgainstMeta(fresh, revoke))
}

// The namespace routing key is valid only for a collection whose rows have
// ALWAYS been placed by it -- namespace.sharding.enabled=true in partition_key
// mode -- and that is decidable from the collection's own properties because
// both are immutable after creation. A default namespace collection is placed by
// primary key, and back-filling hash($namespace_id) onto it would send a
// namespace's new rows to one shard while its existing rows stay everywhere.
func TestCheckRoutingCommitAgainstMetaRefusesTheNamespaceKeyForAPrimaryKeyPlacedCollection(t *testing.T) {
	commit := func(shardBy string) *rootcoordpb.CommitShardSplitRoutingRequest {
		return &rootcoordpb.CommitShardSplitRoutingRequest{
			CollectionName:      "c",
			VirtualChannelNames: []string{"v0", "v1", "v2"},
			RoutingModulus:      2,
			ShardBy:             shardBy,
			ShardInfos: []*schemapb.CollectionShardInfo{
				pbShard(schemapb.ShardState_ShardSplitting),
				pbShard(schemapb.ShardState_ShardCreating, 0),
				pbShard(schemapb.ShardState_ShardCreating, 1),
			},
		}
	}
	collWith := func(props ...*commonpb.KeyValuePair) *model.Collection {
		return &model.Collection{Name: "c", VirtualChannelNames: []string{"v0"}, Properties: props}
	}
	kv := func(k, v string) *commonpb.KeyValuePair { return &commonpb.KeyValuePair{Key: k, Value: v} }

	// The default namespace collection: sharding.enabled written as false at
	// create time. Its rows are placed by primary key.
	err := checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceShardingEnabledKey, "false"), kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)), commit(namespaceShardBy))
	require.ErrorIs(t, err, merr.ErrServiceInternal, "a planning bug, not user input")
	require.False(t, merr.IsRetryableErr(err), "asking again gets the same answer")
	assert.Contains(t, err.Error(), "placed by primary key")

	// sharding on, but partition mode: still placed by primary key.
	err = checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceShardingEnabledKey, "true"), kv(common.NamespaceModeKey, common.NamespaceModePartition)), commit(namespaceShardBy))
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	// The property absent altogether reads as false, the create-time default.
	err = checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)), commit(namespaceShardBy))
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	// The one configuration the key is valid for.
	require.NoError(t, checkRoutingCommitAgainstMeta(
		collWith(kv(common.NamespaceShardingEnabledKey, "true"), kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)),
		commit(namespaceShardBy)))

	// A primary-key routed split of the same default collection is untouched by
	// the gate: that is the key its rows were placed by.
	require.NoError(t, checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceShardingEnabledKey, "false")), commit("hash(pk)")))
}
