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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
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

	require.True(t, routingCommitAlreadyApplied(coll, routingUpdatesFromRequest(req(2, []uint64{0}, []uint64{1}))))

	// A rebase onto a doubled modulus leaves every state alone and changes only
	// the residues. Comparing states alone would call this already committed and
	// silently drop it.
	require.False(t, routingCommitAlreadyApplied(coll, routingUpdatesFromRequest(req(4, []uint64{0, 2}, []uint64{1, 3}))))
	// Same modulus, different residues.
	require.False(t, routingCommitAlreadyApplied(coll, routingUpdatesFromRequest(req(2, []uint64{1}, []uint64{0}))))
	// A shard_by back-fill the collection does not carry yet.
	backfill := req(2, []uint64{0}, []uint64{1})
	backfill.ShardBy = "hash($namespace_id)"
	require.False(t, routingCommitAlreadyApplied(coll, routingUpdatesFromRequest(backfill)))
	// An empty shard_by asks for no back-fill, so it does not make the commit
	// look different.
	require.True(t, routingCommitAlreadyApplied(coll, routingUpdatesFromRequest(req(2, []uint64{0}, []uint64{1}))))
	// A vchannel the collection does not have.
	unknown := req(2, []uint64{0}, []uint64{1})
	unknown.VirtualChannelNames = []string{"v0", "v9"}
	require.False(t, routingCommitAlreadyApplied(coll, routingUpdatesFromRequest(unknown)))
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
	// Forward: fence a source, release it, adopt a target.
	require.True(t, shardStateMayAdvance(schemapb.ShardState_ShardNormal, schemapb.ShardState_ShardSplitting))
	require.True(t, shardStateMayAdvance(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardDropped))
	require.True(t, shardStateMayAdvance(schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardNormal))
	// A target is write-routable from the moment it is published, so there is no
	// abandoning it: dropping one would discard the writes it has already taken.
	require.False(t, shardStateMayAdvance(schemapb.ShardState_ShardCreating, schemapb.ShardState_ShardDropped))
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
	require.ErrorIs(t, checkRoutingCommitAgainstMeta(coll, routingUpdatesFromRequest(stale)), merr.ErrParameterInvalid)

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
	require.ErrorIs(t, checkRoutingCommitAgainstMeta(coll, routingUpdatesFromRequest(revoke)), merr.ErrParameterInvalid)

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
	require.NoError(t, checkRoutingCommitAgainstMeta(coll, routingUpdatesFromRequest(forward)))

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
	require.NoError(t, checkRoutingCommitAgainstMeta(coll, routingUpdatesFromRequest(rebase)))

	// A collection that has never been split may of course start at zero.
	fresh := &model.Collection{Name: "c", VirtualChannelNames: []string{"v0"}}
	require.NoError(t, checkRoutingCommitAgainstMeta(fresh, routingUpdatesFromRequest(revoke)))
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
	err := checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceShardingEnabledKey, "false"), kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)), routingUpdatesFromRequest(commit(namespaceShardBy)))
	require.ErrorIs(t, err, merr.ErrServiceInternal, "a planning bug, not user input")
	require.False(t, merr.IsRetryableErr(err), "asking again gets the same answer")
	assert.Contains(t, err.Error(), "placed by primary key")

	// sharding on, but partition mode: still placed by primary key.
	err = checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceShardingEnabledKey, "true"), kv(common.NamespaceModeKey, common.NamespaceModePartition)), routingUpdatesFromRequest(commit(namespaceShardBy)))
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	// The property absent altogether reads as false, the create-time default.
	err = checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)), routingUpdatesFromRequest(commit(namespaceShardBy)))
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	// A malformed property is a System error too: nothing about the request can
	// be changed to make it parse.
	err = checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceShardingEnabledKey, "yes")), routingUpdatesFromRequest(commit(namespaceShardBy)))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Contains(t, err.Error(), common.NamespaceShardingEnabledKey)

	// The one configuration the key is valid for.
	require.NoError(t, checkRoutingCommitAgainstMeta(
		collWith(kv(common.NamespaceShardingEnabledKey, "true"), kv(common.NamespaceModeKey, common.NamespaceModePartitionKey)),
		routingUpdatesFromRequest(commit(namespaceShardBy))))

	// A primary-key routed split of the same default collection is untouched by
	// the gate: that is the key its rows were placed by.
	require.NoError(t, checkRoutingCommitAgainstMeta(collWith(kv(common.NamespaceShardingEnabledKey, "false")), routingUpdatesFromRequest(commit("hash(pk)"))))
}

// splitTestMidSplitCollection is the collection as the adoption commit finds
// it: the source fenced, the two targets created and already write-routable.
func splitTestMidSplitCollection() *model.Collection {
	coll := splitTestCollectionMeta(
		[]string{splitTestSource, splitTestTarget1, splitTestTarget2},
		map[string]*model.ShardInfo{
			splitTestSource:  {VChannelName: splitTestSource, State: schemapb.ShardState_ShardSplitting},
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{1}},
		}, 2)
	coll.ShardBy = "hash(pk)"
	return coll
}

// splitTestAdoptionPostImage is the adoption commit's post-image: the two
// targets adopted and the source delisted altogether.
func splitTestAdoptionPostImage() *messagespb.AlterCollectionMessageUpdates {
	return &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 1),
		},
		RoutingModulus: 2,
		ShardBy:        "hash(pk)",
		SplitTaskId:    7,
	}
}

// splitTestAlterResult builds the broadcast result of an AlterCollection that
// reached every vchannel of the collection, the delisted source included.
func splitTestAlterResult(updates *messagespb.AlterCollectionMessageUpdates, paths ...string) message.BroadcastResultAlterCollectionMessageV2 {
	vchannels := []string{splitTestSource, splitTestTarget1, splitTestTarget2, splitTestControl}
	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			DbId:         1,
			CollectionId: splitTestCollID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: paths},
			CacheExpirations: ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
				ce.OptLPCMDBName(splitTestDB),
				ce.OptLPCMCollectionName(splitTestCollection),
				ce.OptLPCMCollectionID(splitTestCollID),
				ce.OptLPCMMsgType(commonpb.MsgType_AlterCollection),
			).Build(),
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{Updates: updates}).
		WithBroadcast(vchannels).
		MustBuildBroadcast()
	results := make(map[string]*message.AppendResult, len(vchannels))
	for i, vchannel := range vchannels {
		results[vchannel] = &message.AppendResult{MessageID: rmq.NewRmqID(int64(i + 1)), TimeTick: uint64(100 + i)}
	}
	return message.BroadcastResultAlterCollectionMessageV2{
		Message: message.MustAsBroadcastAlterCollectionMessageV2(raw),
		Results: results,
	}
}

// splitTestRoutingAlterResult is the adoption commit: the routing field mask
// over the post-image that delists the source.
func splitTestRoutingAlterResult(updates *messagespb.AlterCollectionMessageUpdates) message.BroadcastResultAlterCollectionMessageV2 {
	return splitTestAlterResult(updates, message.FieldMaskCollectionShardSplitRouting)
}

// expectAlterCollection arms MetaTable.AlterCollection and records that it ran.
func (h *splitCallbackHarness) expectAlterCollection(err error) *mockrootcoord.IMetaTable_AlterCollection_Call {
	return h.meta.EXPECT().AlterCollection(mock.Anything, mock.Anything).
		Run(func(context.Context, message.BroadcastResultAlterCollectionMessageV2) {
			h.record("AlterCollection")
		}).Return(err)
}

// expectDrained arms CheckShardSplitDrained and records that it ran.
func (h *splitCallbackHarness) expectDrained(resp *datapb.CheckShardSplitDrainedResponse, err error) *imocks.MixCoord_CheckShardSplitDrained_Call {
	return h.mixCoord.EXPECT().CheckShardSplitDrained(mock.Anything, mock.Anything).
		Run(func(context.Context, *datapb.CheckShardSplitDrainedRequest) {
			h.record("CheckShardSplitDrained")
		}).Return(resp, err)
}

// TestCommitShardSplitRoutingBroadcastsToTheDelistedSource: the adoption commit
// drops a vchannel from the collection, and that vchannel's own replica is what
// retires it on the streamingnode -- so the broadcast has to reach it even
// though the post-image no longer names it. The commit also takes the
// collection's resource keys itself: its caller no longer holds them.
func TestCommitShardSplitRoutingBroadcastsToTheDelistedSource(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetCollectionByName(mock.Anything, splitTestDB, splitTestCollection, mock.Anything, mock.Anything).
		Return(splitTestMidSplitCollection(), nil)
	meta.EXPECT().ListAliases(mock.Anything, splitTestDB, splitTestCollection, mock.Anything).Return(nil, nil)
	c := newTestCore(withMeta(meta))

	wal := mock_streaming.NewMockWALAccesser(t)
	wal.EXPECT().ControlChannel().Return(splitTestControl).Maybe()
	streaming.SetWALForTest(wal)

	var got message.BroadcastMutableMessage
	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Broadcast(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			got = msg
			return &types.BroadcastAppendResult{}, nil
		}).Once()
	bapi.EXPECT().Close().Return().Maybe()

	var gotKeys []message.ResourceKey
	locker := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).
		To(func(_ context.Context, keys ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			gotKeys = keys
			return bapi, nil
		}).Build()
	defer locker.UnPatch()

	require.NoError(t, c.broadcastCommitShardSplitRouting(context.Background(), &rootcoordpb.CommitShardSplitRoutingRequest{
		DbName:               splitTestDB,
		CollectionName:       splitTestCollection,
		CollectionId:         splitTestCollID,
		VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 1),
		},
		RoutingModulus: 2,
		ShardBy:        "hash(pk)",
		SplitTaskId:    7,
	}))

	require.NotNil(t, got)
	// The control channel, the two adopted targets, and the source the
	// post-image delists -- each exactly once.
	require.ElementsMatch(t,
		[]string{splitTestControl, splitTestSource, splitTestTarget1, splitTestTarget2},
		got.BroadcastHeader().VChannels)
	// The split's id rides in the body, so every cluster replaying this commit
	// can ask its own datacoord about the same task.
	require.EqualValues(t, 7, message.MustAsBroadcastAlterCollectionMessageV2(got).MustBody().GetUpdates().GetSplitTaskId())
	require.ElementsMatch(t, []message.ResourceKey{
		message.NewSharedDBNameResourceKey(splitTestDB),
		message.NewExclusiveCollectionNameResourceKey(splitTestDB, splitTestCollection),
	}, gotKeys)
}

// TestAdoptionCallbackWaitsForTheLocalDrain: the adoption is what retires the
// source, and every cluster retires its own copy of it. The callback therefore
// asks THIS cluster's datacoord whether the split's sources have drained here,
// and refuses to apply the post-image until they have.
func TestAdoptionCallbackWaitsForTheLocalDrain(t *testing.T) {
	t.Run("not drained yet", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		var got *datapb.CheckShardSplitDrainedRequest
		h.mixCoord.EXPECT().CheckShardSplitDrained(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *datapb.CheckShardSplitDrainedRequest) {
				h.record("CheckShardSplitDrained")
				got = req
			}).Return(&datapb.CheckShardSplitDrainedResponse{Status: merr.Success(), Drained: false}, nil).Once()

		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "not drained")
		// Nothing applied: the meta still routes to the source.
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
		require.EqualValues(t, splitTestCollID, got.GetCollectionId())
		require.EqualValues(t, 7, got.GetSplitTaskId())
	})

	t.Run("drained", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectDrained(&datapb.CheckShardSplitDrainedResponse{Status: merr.Success(), Drained: true}, nil).Once()
		h.expectAlterCollection(nil).Once()

		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Equal(t, []string{"CheckShardSplitDrained", "AlterCollection"}, h.calls)
		require.Equal(t, 1, h.broadcasts)
	})

	t.Run("datacoord unreachable", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectDrained(nil, errors.New("rpc error")).Once()

		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
		require.Error(t, err)
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
	})

	t.Run("datacoord has no record of the task", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectDrained(&datapb.CheckShardSplitDrainedResponse{
			Status: merr.Status(merr.WrapErrServiceInternalMsg("no record of shard split task 7")),
		}, nil).Once()

		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
	})

	t.Run("the collection is gone", func(t *testing.T) {
		h := newSplitCallbackHarness(t, nil)
		h.meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, splitTestCollID, mock.Anything, mock.Anything).
			Return(nil, merr.WrapErrCollectionNotFound(splitTestCollID)).Once()
		h.meta.EXPECT().AlterCollection(mock.Anything, mock.Anything).Return(errAlterCollectionNotFound).Once()

		// The gate has nothing to gate; the apply below reports the same thing
		// and the callback finishes instead of retrying forever.
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
	})

	t.Run("the collection cannot be read", func(t *testing.T) {
		h := newSplitCallbackHarness(t, nil)
		h.meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, splitTestCollID, mock.Anything, mock.Anything).
			Return(nil, errors.New("etcd down")).Once()

		require.Error(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Empty(t, h.calls)
	})
}

// TestAdoptionCallbackSkipsTheDrainOnARedeliveredCommit: a redelivery after
// datacoord reclaimed the task must never ask about a task id datacoord no
// longer knows -- that answer is a System error, and the collection's DDL
// callback queue would wedge behind it forever. A post-image the collection
// already carries is therefore applied without asking.
func TestAdoptionCallbackSkipsTheDrainOnARedeliveredCommit(t *testing.T) {
	coll := splitTestCollectionMeta(
		[]string{splitTestTarget1, splitTestTarget2},
		map[string]*model.ShardInfo{
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1}},
		}, 2)
	coll.ShardBy = "hash(pk)"
	h := newSplitCallbackHarness(t, coll)
	h.expectAlterCollection(nil).Once()

	require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
	require.Equal(t, []string{"AlterCollection"}, h.calls)
}

// TestAdoptionCallbackAppliesANonSplitRoutingCommitImmediately: the drain gate
// belongs to the adoption alone. An alter that carries no routing mask, and a
// routing commit that delists nothing -- the write switch, which publishes the
// targets while the source keeps serving -- both apply straight away.
func TestAdoptionCallbackAppliesANonSplitRoutingCommitImmediately(t *testing.T) {
	t.Run("no routing mask", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectAlterCollection(nil).Once()

		result := splitTestAlterResult(&messagespb.AlterCollectionMessageUpdates{
			Description: "a plain properties alter",
		}, message.FieldMaskCollectionDescription)
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), result))
		require.Equal(t, []string{"AlterCollection"}, h.calls)
	})

	t.Run("a routing commit that delists nothing", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectAlterCollection(nil).Once()

		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestPostImage())))
		require.Equal(t, []string{"AlterCollection"}, h.calls)
	})
}

// TestCommitShardSplitRoutingReportsALockFailure: the commit takes the
// collection's resource keys itself, before it reads any meta, so a locker that
// refuses -- a secondary cluster rejects every broadcast -- fails the commit
// rather than letting it broadcast unserialized.
func TestCommitShardSplitRoutingReportsALockFailure(t *testing.T) {
	c := newTestCore(withMeta(mockrootcoord.NewIMetaTable(t)))
	locker := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).
		To(func(context.Context, ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return nil, broadcast.ErrNotPrimary
		}).Build()
	defer locker.UnPatch()

	err := c.broadcastCommitShardSplitRouting(context.Background(), &rootcoordpb.CommitShardSplitRoutingRequest{
		DbName:               splitTestDB,
		CollectionName:       splitTestCollection,
		VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 1),
		},
		RoutingModulus: 2,
	})
	require.ErrorIs(t, err, broadcast.ErrNotPrimary)
}

// TestCommitShardSplitRoutingRefusesAnAdoptionWithNoSplitTaskId: the adoption's
// callback asks datacoord about the split it names, and a broadcast whose
// callback can never succeed holds the collection's exclusive key forever. A
// commit that retires a vchannel without naming its split is therefore a failed
// RPC, not an appended message.
func TestCommitShardSplitRoutingRefusesAnAdoptionWithNoSplitTaskId(t *testing.T) {
	meta := mockrootcoord.NewIMetaTable(t)
	meta.EXPECT().GetCollectionByName(mock.Anything, splitTestDB, splitTestCollection, mock.Anything, mock.Anything).
		Return(splitTestMidSplitCollection(), nil)
	c := newTestCore(withMeta(meta))

	bapi := mock_broadcaster.NewMockBroadcastAPI(t)
	bapi.EXPECT().Close().Return().Maybe()
	locker := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).
		To(func(context.Context, ...message.ResourceKey) (broadcaster.BroadcastAPI, error) {
			return bapi, nil
		}).Build()
	defer locker.UnPatch()

	// Nothing is broadcast: the mock would fail the test on an unexpected
	// Broadcast call.
	err := c.broadcastCommitShardSplitRouting(context.Background(), &rootcoordpb.CommitShardSplitRoutingRequest{
		DbName:               splitTestDB,
		CollectionName:       splitTestCollection,
		CollectionId:         splitTestCollID,
		VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 1),
		},
		RoutingModulus: 2,
	})
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.ErrorContains(t, err, "without naming the split task")
	require.False(t, merr.IsRetryableErr(err), "the same request gets the same answer")
}
