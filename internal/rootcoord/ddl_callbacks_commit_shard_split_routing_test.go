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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogmocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/ce"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
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

// broadcastShardSplitRoutingForTest issues an AlterCollection carrying the
// shard-split routing mask under the collection's own name keys, to the control
// channel, the collection's current vchannels and the post-image's. Its
// production issuer lands with the split manager; what this exercises is the
// ack callback that applies it.
func broadcastShardSplitRoutingForTest(t *testing.T, core *Core, dbName, collectionName string, updates *messagespb.AlterCollectionMessageUpdates) error {
	t.Helper()
	ctx := context.Background()
	coll, err := core.meta.GetCollectionByName(ctx, dbName, collectionName, typeutil.MaxTimestamp, false)
	require.NoError(t, err)
	b, err := broadcast.StartBroadcastWithResourceKeys(ctx,
		message.NewSharedDBNameResourceKey(dbName),
		message.NewExclusiveCollectionNameResourceKey(dbName, collectionName))
	require.NoError(t, err)
	defer b.Close()

	cacheExpirations, err := core.getCacheExpireForCollection(ctx, dbName, collectionName)
	require.NoError(t, err)

	channels := []string{streaming.WAL().ControlChannel()}
	seen := typeutil.NewSet(channels...)
	for _, list := range [][]string{coll.VirtualChannelNames, updates.GetVirtualChannelNames()} {
		for _, vchannel := range list {
			if !seen.Contain(vchannel) {
				seen.Insert(vchannel)
				channels = append(channels, vchannel)
			}
		}
	}
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(&messagespb.AlterCollectionMessageHeader{
			DbId:             coll.DBID,
			CollectionId:     coll.CollectionID,
			UpdateMask:       &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
			CacheExpirations: cacheExpirations,
		}).
		WithBody(&messagespb.AlterCollectionMessageBody{Updates: updates}).
		WithBroadcast(channels).
		MustBuildBroadcast()
	_, err = b.Broadcast(ctx, msg)
	return err
}

// TestDDLCallbacksApplyAShardSplitRoutingAlter drives a routing alter through
// the real broadcaster and ack callback onto a real meta table. The write switch
// is seeded through ApplyShardSplitRouting, as the SplitShard callback applies it;
// then the adoption that keeps the fenced source listed, and a redelivery of it.
func TestDDLCallbacksApplyAShardSplitRoutingAlter(t *testing.T) {
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
	t1, t2 := source+"_split1", source+"_split2"

	// The adoption callback learns what it may retire and adopt from this
	// cluster's datacoord record of the split task.
	core.mixCoord.(*imocks.MixCoord).EXPECT().CheckShardSplitDrained(mock.Anything, mock.Anything).
		Return(&datapb.CheckShardSplitDrainedResponse{
			Status: merr.Success(), Recorded: true, Drained: false,
			SourceVchannels: []string{source}, TargetVchannels: []string{t1, t2},
		}, nil)

	postImage := func(sourceState, targetState schemapb.ShardState) *messagespb.AlterCollectionMessageUpdates {
		return &messagespb.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{source, t1, t2},
			PhysicalChannelNames: []string{funcutil.ToPhysicalChannel(source), funcutil.ToPhysicalChannel(t1), funcutil.ToPhysicalChannel(t2)},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: sourceState},
				pbShard(targetState, 0),
				pbShard(targetState, 1),
			},
			RoutingModulus: 2,
			ShardBy:        "hash(pk)",
			SplitTaskId:    7,
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

	// write-switch shape: source Splitting, two targets Creating.
	require.NoError(t, core.meta.ApplyShardSplitRouting(ctx, coll.CollectionID,
		postImage(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating),
		routing.SplitDelta(source, []string{t1, t2}, true), coll.UpdateTimestamp+1))
	assertStates(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating)

	// adoption that keeps the source listed: targets Normal, source still
	// Splitting. It delists nothing, so no drain gate applies.
	require.NoError(t, broadcastShardSplitRoutingForTest(t, core, dbName, collectionName, postImage(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardNormal)))
	assertStates(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardNormal)

	// a redelivery of the same post-image leaves the meta where it is.
	require.NoError(t, broadcastShardSplitRoutingForTest(t, core, dbName, collectionName, postImage(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardNormal)))
	assertStates(schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardNormal)
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

// splitTestDrainResponse is this cluster's datacoord answer about split task 7:
// recorded, splitting splitTestSource into the two targets.
func splitTestDrainResponse(drained bool) *datapb.CheckShardSplitDrainedResponse {
	return &datapb.CheckShardSplitDrainedResponse{
		Status:          merr.Success(),
		Drained:         drained,
		Recorded:        true,
		SourceVchannels: []string{splitTestSource},
		TargetVchannels: []string{splitTestTarget1, splitTestTarget2},
	}
}

// splitTestUnrecordedResponse is the answer about a task this cluster has no
// record of.
func splitTestUnrecordedResponse() *datapb.CheckShardSplitDrainedResponse {
	return &datapb.CheckShardSplitDrainedResponse{Status: merr.Success(), Recorded: false}
}

// splitTestAlterResult builds the broadcast result of an AlterCollection that
// reached every vchannel of the collection, the delisted source included.
func splitTestAlterResult(updates *messagespb.AlterCollectionMessageUpdates, paths ...string) message.BroadcastResultAlterCollectionMessageV2 {
	return splitTestAlterResultOn([]string{splitTestSource, splitTestTarget1, splitTestTarget2, splitTestControl}, false, updates, paths...)
}

// splitTestAlterResultOn builds the broadcast result of an AlterCollection sent
// to the given vchannels. replicated stamps a replicate header on it, which is
// how the message looks when a secondary cluster's broadcaster acks it.
func splitTestAlterResultOn(vchannels []string, replicated bool, updates *messagespb.AlterCollectionMessageUpdates, paths ...string) message.BroadcastResultAlterCollectionMessageV2 {
	return splitTestAlterResultWithHeader(vchannels, replicated, updates, nil, paths...)
}

// splitTestAlterResultWithHeader is splitTestAlterResultOn whose header, built
// as the collection's own AlterCollection would build it (its cache expiration
// naming the collection), is then mutated by headerOpt.
func splitTestAlterResultWithHeader(vchannels []string, replicated bool, updates *messagespb.AlterCollectionMessageUpdates, headerOpt func(*messagespb.AlterCollectionMessageHeader), paths ...string) message.BroadcastResultAlterCollectionMessageV2 {
	header := &messagespb.AlterCollectionMessageHeader{
		DbId:         1,
		CollectionId: splitTestCollID,
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: paths},
		CacheExpirations: ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
			ce.OptLPCMDBName(splitTestDB),
			ce.OptLPCMCollectionName(splitTestCollection),
			ce.OptLPCMCollectionID(splitTestCollID),
			ce.OptLPCMMsgType(commonpb.MsgType_AlterCollection),
		).Build(),
	}
	if headerOpt != nil {
		headerOpt(header)
	}
	raw := message.NewAlterCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(&messagespb.AlterCollectionMessageBody{Updates: updates}).
		WithBroadcast(vchannels).
		MustBuildBroadcast()
	if replicated {
		raw.(message.MutableMessage).WithReplicateHeader(&message.ReplicateHeader{
			ClusterID:              "primary",
			MessageID:              rmq.NewRmqID(1),
			LastConfirmedMessageID: rmq.NewRmqID(1),
			TimeTick:               1,
			VChannel:               splitTestControl,
		})
	}
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

// expectAlterCollection arms the generic MetaTable.AlterCollection and records
// that it ran. A routing commit must never reach it.
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

// requireAheadOfCollection asserts a refusal that waits, retriably, for an
// earlier routing commit of the collection to be applied on this cluster.
func requireAheadOfCollection(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.True(t, errors.Is(err, routing.ErrCommitAheadOfCollection), "%v", err)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.True(t, merr.IsRetryableErr(err))
}

// TestAdoptionCallbackWaitsForTheLocalDrain: the adoption is what retires the
// source, and every cluster retires its own copy of it. The callback therefore
// asks THIS cluster's datacoord whether the split's sources have drained here,
// and refuses to apply the post-image until they have.
func TestAdoptionCallbackWaitsForTheLocalDrain(t *testing.T) {
	t.Run("not drained yet: every retry refuses without applying, until it drains", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		var got *datapb.CheckShardSplitDrainedRequest
		h.mixCoord.EXPECT().CheckShardSplitDrained(mock.Anything, mock.Anything).
			Run(func(_ context.Context, req *datapb.CheckShardSplitDrainedRequest) {
				h.record("CheckShardSplitDrained")
				got = req
			}).Return(splitTestDrainResponse(false), nil).Twice()

		// The broadcaster retries the callback; each attempt refuses again and
		// nothing is written in between.
		for i := 0; i < 2; i++ {
			err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
			// A transient wait: System, and retriable.
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.True(t, merr.IsRetryableErr(err))
			require.NotEqual(t, merr.InputError, merr.GetErrorType(err))
			require.ErrorContains(t, err, "not drained")
		}
		require.Equal(t, []string{"CheckShardSplitDrained", "CheckShardSplitDrained"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
		require.EqualValues(t, splitTestCollID, got.GetCollectionId())
		require.EqualValues(t, 7, got.GetSplitTaskId())

		h.expectDrained(splitTestDrainResponse(true), nil).Once()
		h.expectApply(nil).Once()
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Equal(t, []string{"CheckShardSplitDrained", "CheckShardSplitDrained", "CheckShardSplitDrained", "ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, 1, h.broadcasts)
	})

	t.Run("drained: applied through ApplyShardSplitRouting, never the generic alter", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectDrained(splitTestDrainResponse(true), nil).Once()
		var gotUpdates *messagespb.AlterCollectionMessageUpdates
		var gotDelta routing.CommitDelta
		var gotTick uint64
		h.meta.EXPECT().ApplyShardSplitRouting(mock.Anything, splitTestCollID, mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, _ int64, updates *messagespb.AlterCollectionMessageUpdates, delta routing.CommitDelta, tick uint64) {
				h.record("ApplyShardSplitRouting")
				gotUpdates, gotDelta, gotTick = updates, delta, tick
			}).Return(nil).Once()

		result := splitTestRoutingAlterResult(splitTestAdoptionPostImage())
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), result))
		require.Equal(t, []string{"CheckShardSplitDrained", "ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, 1, h.broadcasts)
		require.True(t, proto.Equal(splitTestAdoptionPostImage(), gotUpdates))
		// The adoption's own delta, as datacoord's record names it, is what the
		// meta table judges again under its lock.
		require.Equal(t, routing.AdoptionDelta(splitTestSource, []string{splitTestTarget1, splitTestTarget2}, true), gotDelta)
		require.Equal(t, result.GetMaxTimeTick(), gotTick)
	})

	t.Run("datacoord unreachable", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectDrained(nil, errors.New("rpc error")).Once()

		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
		require.Error(t, err)
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
	})

	t.Run("datacoord has no record of the task: its split is not applied here", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectDrained(splitTestUnrecordedResponse(), nil).Once()

		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
		requireAheadOfCollection(t, err)
		require.ErrorContains(t, err, "no record of split task 7")
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})

	t.Run("datacoord records the task with more than one source", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		resp := splitTestDrainResponse(true)
		resp.SourceVchannels = []string{splitTestSource, splitTestTarget1}
		h.expectDrained(resp, nil).Once()

		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "a split has one")
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
	})

	t.Run("datacoord records the task with no source", func(t *testing.T) {
		// A delta with an empty source would read as "retired already" and let
		// the adoption retire whatever the post-image delists: refused before
		// the judge, whatever the drain says.
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		resp := splitTestDrainResponse(true)
		resp.SourceVchannels = nil
		h.expectDrained(resp, nil).Once()

		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "recorded with 0 sources")
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})

	// A routing commit that names no split task has no delta anyone could name
	// and no drain question anyone could answer. It is refused outright, before
	// datacoord is asked.
	t.Run("no split task named", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		taskless := splitTestAdoptionPostImage()
		taskless.SplitTaskId = 0
		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(taskless))
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "does not name its split task")
		require.Empty(t, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})

	// The retired shard's own replica is what tears its streamingnode state
	// down; a retiring commit that never reached it would leave that state, and
	// the WAL truncation it pins, behind forever.
	t.Run("the retired vchannel was not broadcast to", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectDrained(splitTestDrainResponse(true), nil).Once()
		result := splitTestAlterResultOn([]string{splitTestTarget1, splitTestTarget2, splitTestControl}, false,
			splitTestAdoptionPostImage(), message.FieldMaskCollectionShardSplitRouting)
		err := h.callback.alterCollectionV2AckCallback(context.Background(), result)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "was not broadcast to it")
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls, "the reach check needs the judge's word that the delist is this adoption's own")
	})

	t.Run("the collection is gone", func(t *testing.T) {
		h := newSplitCallbackHarness(t, nil)
		h.meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, splitTestCollID, mock.Anything, mock.Anything).
			Return(nil, merr.WrapErrCollectionNotFound(splitTestCollID)).Once()
		h.expectApply(errAlterCollectionNotFound).Once()

		// The gate has nothing to gate; the apply reports the same thing and the
		// callback finishes instead of retrying forever.
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Equal(t, []string{"ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})

	t.Run("the collection cannot be read", func(t *testing.T) {
		h := newSplitCallbackHarness(t, nil)
		h.meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, splitTestCollID, mock.Anything, mock.Anything).
			Return(nil, errors.New("etcd down")).Once()

		require.Error(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Empty(t, h.calls)
	})
}

// splitTestKeepSourceAdoption is the adoption commit that adopts both targets
// while keeping the fenced source listed; it retires nothing.
func splitTestKeepSourceAdoption() *messagespb.AlterCollectionMessageUpdates {
	u := splitTestPostImage()
	u.ShardInfos[1] = pbShard(schemapb.ShardState_ShardNormal, 0)
	u.ShardInfos[2] = pbShard(schemapb.ShardState_ShardNormal, 1)
	u.SplitTaskId = 7
	return u
}

// splitTestAdopted is the collection after the adoption: source retired, both
// targets Normal.
func splitTestAdopted() *model.Collection {
	coll := splitTestCollectionMeta(
		[]string{splitTestTarget1, splitTestTarget2},
		map[string]*model.ShardInfo{
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1}},
		}, 2)
	coll.ShardBy = "hash(pk)"
	return coll
}

// TestAdoptionCallbackSkipsTheDrainUnlessTheCommitIsForward: only a forward
// commit reaches the drain gate. A redelivery the collection already carries is
// a no-op, recognized before datacoord is asked -- after datacoord reclaimed
// the task, nothing could name its delta. A commit ahead of this cluster's meta
// waits, retriably, and never applies what another commit should.
func TestAdoptionCallbackSkipsTheDrainUnlessTheCommitIsForward(t *testing.T) {
	t.Run("already applied", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestAdopted())
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Empty(t, h.calls, "neither datacoord nor the meta apply runs")
		require.Equal(t, 1, h.broadcasts)
	})

	// F3, the shape the review reproduced on a secondary: the collection was
	// already split once at modulus 4, and the adoption of a second split of shard
	// a reaches its callback before that split's SplitShard callback has applied
	// the split here. Datacoord has no record of the task, so the adoption cannot
	// even name what it may change; it waits, retriably, and is never skipped.
	// Once the split is applied it goes on to the drain gate.
	t.Run("its split is not applied on this cluster yet: waits, never skipped", func(t *testing.T) {
		const a, b = "by-dev-rootcoord-dml_0_100v0", "by-dev-rootcoord-dml_3_100v3"
		adoptA := &messagespb.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2, b},
			PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				pbShard(schemapb.ShardState_ShardNormal, 0),
				pbShard(schemapb.ShardState_ShardNormal, 2),
				pbShard(schemapb.ShardState_ShardNormal, 1, 3),
			},
			RoutingModulus: 4,
			ShardBy:        "hash(pk)",
			SplitTaskId:    7,
		}
		result := splitTestAlterResultOn([]string{a, splitTestTarget1, splitTestTarget2, b, splitTestControl}, true,
			adoptA, message.FieldMaskCollectionShardSplitRouting)

		beforeSplit := splitTestCollectionMeta([]string{a, b}, map[string]*model.ShardInfo{
			a: {VChannelName: a, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0, 2}},
			b: {VChannelName: b, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1, 3}},
		}, 4)
		beforeSplit.ShardBy = "hash(pk)"
		h := newSplitCallbackHarness(t, beforeSplit)
		h.expectDrained(splitTestUnrecordedResponse(), nil).Once()
		requireAheadOfCollection(t, h.callback.alterCollectionV2AckCallback(context.Background(), result))
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls, "no apply")
		require.Equal(t, 0, h.broadcasts)

		// The split's callback recorded the task but crashed before the meta
		// apply: the record names the delta, and the meta says the source is
		// not fenced yet. Still ahead.
		h = newSplitCallbackHarness(t, beforeSplit)
		h.expectDrained(splitTestDrainResponse(true), nil).Once()
		err := h.callback.alterCollectionV2AckCallback(context.Background(), result)
		requireAheadOfCollection(t, err)
		require.ErrorContains(t, err, "not fenced")
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls, "no apply")

		afterSplit := splitTestCollectionMeta([]string{a, splitTestTarget1, splitTestTarget2, b}, map[string]*model.ShardInfo{
			a:                {VChannelName: a, State: schemapb.ShardState_ShardSplitting},
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{2}},
			b:                {VChannelName: b, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1, 3}},
		}, 4)
		afterSplit.ShardBy = "hash(pk)"
		h = newSplitCallbackHarness(t, afterSplit)
		resp := splitTestDrainResponse(true)
		resp.SourceVchannels = []string{a}
		h.expectDrained(resp, nil).Once()
		h.expectApply(nil).Once()
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), result))
		require.Equal(t, []string{"CheckShardSplitDrained", "ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, 1, h.broadcasts)
	})

	// A redelivery after later commits applied on top of it -- a crash before
	// its tombstone, then a later commit with free keys -- carries nothing left
	// to write: a no-op, not a refusal.
	t.Run("a stale redelivery is a no-op", func(t *testing.T) {
		// The adopt-first commit after the source was retired.
		h := newSplitCallbackHarness(t, splitTestAdopted())
		h.expectDrained(splitTestDrainResponse(true), nil).Once()
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestKeepSourceAdoption())))
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
		require.Equal(t, 1, h.broadcasts)

		// The adoption after a later split fenced one of its targets and doubled
		// the modulus.
		coll := splitTestCollectionMeta(
			[]string{splitTestTarget1, splitTestTarget2, "by-dev-rootcoord-dml_3_100v3"},
			map[string]*model.ShardInfo{
				splitTestTarget1:               {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardSplitting},
				splitTestTarget2:               {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1, 3}},
				"by-dev-rootcoord-dml_3_100v3": {VChannelName: "by-dev-rootcoord-dml_3_100v3", State: schemapb.ShardState_ShardCreating, Buckets: []uint64{0, 2}},
			}, 4)
		coll.ShardBy = "hash(pk)"
		h = newSplitCallbackHarness(t, coll)
		h.expectDrained(splitTestDrainResponse(true), nil).Once()
		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
		require.Equal(t, 1, h.broadcasts)
	})
}

// TestAdoptionCallbackRetiresOnlyItsOwnSource: on a secondary the callbacks of
// one collection's routing commits are not ordered by the broadcaster. A commit
// whose post-image already reflects an earlier adoption -- the source of another
// split retired, that split's targets Normal -- may not apply that adoption's
// delta on its behalf: doing so would retire a shard without the drain gate
// that only its own adoption runs. It is ahead, retriably, and never reaches
// the reach check, the drain gate or the apply.
func TestAdoptionCallbackRetiresOnlyItsOwnSource(t *testing.T) {
	const b, b1, b2 = "by-dev-rootcoord-dml_3_100v3", "by-dev-rootcoord-dml_4_100v4", "by-dev-rootcoord-dml_5_100v5"
	// After split 1 (s -> t1, t2) and split 2 (b -> b1, b2), before either
	// adoption.
	coll := splitTestCollectionMeta([]string{splitTestSource, splitTestTarget1, splitTestTarget2, b, b1, b2}, map[string]*model.ShardInfo{
		splitTestSource:  {VChannelName: splitTestSource, State: schemapb.ShardState_ShardSplitting},
		splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{0}},
		splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{2}},
		b:                {VChannelName: b, State: schemapb.ShardState_ShardSplitting},
		b1:               {VChannelName: b1, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{1}},
		b2:               {VChannelName: b2, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{3}},
	}, 4)
	coll.ShardBy = "hash(pk)"
	// Adoption 2, issued on the primary after adoption 1: neither source listed.
	adoption2 := &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2, b1, b2},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_4", "by-dev-rootcoord-dml_5"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 2),
			pbShard(schemapb.ShardState_ShardNormal, 1),
			pbShard(schemapb.ShardState_ShardNormal, 3),
		},
		RoutingModulus: 4,
		ShardBy:        "hash(pk)",
		SplitTaskId:    8,
	}
	// Broadcast to b (which it retires) and the rest, but not to s: on the
	// primary s was gone when adoption 2 was issued.
	result := splitTestAlterResultOn([]string{splitTestTarget1, splitTestTarget2, b, b1, b2, splitTestControl}, true,
		adoption2, message.FieldMaskCollectionShardSplitRouting)

	h := newSplitCallbackHarness(t, coll)
	h.expectDrained(&datapb.CheckShardSplitDrainedResponse{
		Status: merr.Success(), Recorded: true, Drained: true,
		SourceVchannels: []string{b}, TargetVchannels: []string{b1, b2},
	}, nil).Once()
	err := h.callback.alterCollectionV2AckCallback(context.Background(), result)
	requireAheadOfCollection(t, err)
	require.ErrorContains(t, err, "not this commit's to retire")
	require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls, "no apply, and no reach refusal for a delist that is not its own")
	require.Equal(t, 0, h.broadcasts)
}

// TestShardSplitRoutingAlterRefusesAnIncoherentReplicatedAdoption: a secondary
// receives the adoption through replication and acks it through the same
// callback, so it is subject to every refusal the primary's own commit is. The
// refusal happens before the drain is asked and before anything is written; a
// refusal the message alone decides happens before datacoord is asked at all.
func TestShardSplitRoutingAlterRefusesAnIncoherentReplicatedAdoption(t *testing.T) {
	kv := func(k, v string) *commonpb.KeyValuePair { return &commonpb.KeyValuePair{Key: k, Value: v} }
	cases := []struct {
		name     string
		coll     func() *model.Collection
		updates  func() *messagespb.AlterCollectionMessageUpdates
		contains string
		// asksDataCoord: the refusal needs the task record's delta.
		asksDataCoord bool
	}{
		{
			name: "moves a shard backwards",
			coll: splitTestMidSplitCollection,
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				// Un-fences the source while adopting the targets: sideways. A
				// writable source owning no residue is a dead shard, which the
				// tiling check names before the source's state is judged, and
				// before datacoord is asked.
				u := splitTestKeepSourceAdoption()
				u.ShardInfos[0] = pbShard(schemapb.ShardState_ShardNormal)
				return u
			},
			contains: "owns no residue",
		},
		{
			name: "moves an adopted target back to Creating",
			coll: func() *model.Collection {
				coll := splitTestMidSplitCollection()
				coll.ShardInfos[splitTestTarget1].State = schemapb.ShardState_ShardNormal
				return coll
			},
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestKeepSourceAdoption()
				u.ShardInfos[1] = pbShard(schemapb.ShardState_ShardCreating, 0)
				return u
			},
			contains:      "cannot go from ShardNormal to ShardCreating",
			asksDataCoord: true,
		},
		{
			// A shrink whose post-image still tiles (t1 {0}, t2 {1, 2} at
			// modulus 3 against a collection at 4), so the modulus is what the
			// refusal names.
			name: "shrinks the modulus",
			coll: func() *model.Collection {
				coll := splitTestMidSplitCollection()
				coll.RoutingModulus = 4
				coll.ShardInfos[splitTestTarget1].Buckets = []uint64{0, 2}
				coll.ShardInfos[splitTestTarget2].Buckets = []uint64{1, 3}
				return coll
			},
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestAdoptionPostImage()
				u.RoutingModulus = 3
				u.ShardInfos[1] = pbShard(schemapb.ShardState_ShardNormal, 1, 2)
				return u
			},
			contains:      "cannot take it down to",
			asksDataCoord: true,
		},
		{
			// High-4: the message-only checks the SplitShard post-image gets
			// before the fence apply to the adoption too, and run before this
			// cluster's datacoord is asked -- so a malformed adoption never
			// reaches the drain gate.
			name: "lists fewer pchannels than vchannels",
			coll: splitTestMidSplitCollection,
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestAdoptionPostImage()
				u.PhysicalChannelNames = u.PhysicalChannelNames[:1]
				return u
			},
			contains: "parallel and non-empty",
		},
		{
			name: "lists a vchannel twice",
			coll: splitTestMidSplitCollection,
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestAdoptionPostImage()
				u.VirtualChannelNames[1] = splitTestTarget1
				return u
			},
			contains: "twice",
		},
		{
			name: "names another vchannel in a shard info",
			coll: splitTestMidSplitCollection,
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestAdoptionPostImage()
				u.ShardInfos[0].VchannelName = splitTestTarget2
				return u
			},
			contains: "names vchannel",
		},
		{
			name: "does not tile the key space",
			coll: splitTestMidSplitCollection,
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestAdoptionPostImage()
				u.ShardInfos[1] = pbShard(schemapb.ShardState_ShardNormal, 0)
				return u
			},
			contains: "overlap at residue 0",
		},
		{
			name: "revokes the modulus",
			coll: splitTestMidSplitCollection,
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestAdoptionPostImage()
				u.RoutingModulus = 0
				return u
			},
			contains: "back to none",
		},
		{
			name: "changes the residues it adopts",
			coll: splitTestMidSplitCollection,
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestAdoptionPostImage()
				u.ShardInfos[0] = pbShard(schemapb.ShardState_ShardNormal, 1)
				u.ShardInfos[1] = pbShard(schemapb.ShardState_ShardNormal, 0)
				return u
			},
			contains:      "created with",
			asksDataCoord: true,
		},
		{
			// F4: keeping the source listed but Dropped would skip the reach check
			// and the drain gate, which only a delist gets.
			name: "moves a still-listed source to Dropped",
			coll: splitTestMidSplitCollection,
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestKeepSourceAdoption()
				u.ShardInfos[0] = pbShard(schemapb.ShardState_ShardDropped)
				return u
			},
			contains: "reaches Dropped only by being delisted",
		},
		{
			name: "fails namespace admission",
			coll: func() *model.Collection {
				coll := splitTestMidSplitCollection()
				coll.Properties = []*commonpb.KeyValuePair{
					kv(common.NamespaceShardingEnabledKey, "false"),
					kv(common.NamespaceModeKey, common.NamespaceModePartitionKey),
				}
				return coll
			},
			updates: func() *messagespb.AlterCollectionMessageUpdates {
				u := splitTestAdoptionPostImage()
				u.ShardBy = routing.NamespaceShardBy
				return u
			},
			contains: "placed by primary key",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSplitCallbackHarness(t, tc.coll())
			if tc.asksDataCoord {
				h.expectDrained(splitTestDrainResponse(true), nil).Once()
			}
			result := splitTestAlterResultOn([]string{splitTestSource, splitTestTarget1, splitTestTarget2, splitTestControl}, true,
				tc.updates(), message.FieldMaskCollectionShardSplitRouting)
			require.NotNil(t, result.Message.ReplicateHeader(), "staged as the secondary's replica")

			err := h.callback.alterCollectionV2AckCallback(context.Background(), result)
			require.ErrorIs(t, err, merr.ErrServiceInternal, "an incoherent post-image is a Milvus bug, not user input")
			require.False(t, merr.IsRetryableErr(err))
			require.ErrorContains(t, err, tc.contains)
			if tc.asksDataCoord {
				require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls, "the meta apply never runs")
			} else {
				require.Empty(t, h.calls, "neither datacoord nor the meta apply runs")
			}
			require.Equal(t, 0, h.broadcasts)
		})
	}

	t.Run("a replicated commit ahead of this cluster's meta waits", func(t *testing.T) {
		// The source is not fenced here: this adoption's own split has not
		// applied on this cluster.
		coll := splitTestMidSplitCollection()
		coll.ShardInfos[splitTestSource].State = schemapb.ShardState_ShardNormal
		h := newSplitCallbackHarness(t, coll)
		h.expectDrained(splitTestDrainResponse(true), nil).Once()
		result := splitTestAlterResultOn([]string{splitTestSource, splitTestTarget1, splitTestTarget2, splitTestControl}, true,
			splitTestAdoptionPostImage(), message.FieldMaskCollectionShardSplitRouting)

		requireAheadOfCollection(t, h.callback.alterCollectionV2AckCallback(context.Background(), result))
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})
}

// realMetaHarness wires the callbacks onto a REAL meta table holding coll, with
// a catalog mock that accepts every write, and a mixcoord mock.
func realMetaHarness(t *testing.T, coll *model.Collection) (*splitCallbackHarness, *MetaTable) {
	t.Helper()
	channel.ResetStaticPChannelStatsManager()
	channel.RecoverPChannelStatsManager([]string{})
	t.Cleanup(channel.ResetStaticPChannelStatsManager)

	catalog := catalogmocks.NewRootCoordCatalog(t)
	catalog.EXPECT().AlterCollection(mock.Anything, mock.Anything, mock.Anything, metastore.MODIFY, mock.Anything, false).
		Return(nil).Maybe()
	mt := &MetaTable{
		catalog:     catalog,
		names:       newNameDb(),
		aliases:     newNameDb(),
		dbName2Meta: map[string]*model.Database{splitTestDB: {ID: coll.DBID, Name: splitTestDB}},
		collID2Meta: map[typeutil.UniqueID]*model.Collection{splitTestCollID: coll},
	}
	mt.names.insert(splitTestDB, splitTestCollection, splitTestCollID)
	mt.aliases.createDbIfNotExist(splitTestDB)

	h := &splitCallbackHarness{}
	h.mixCoord = imocks.NewMixCoord(t)
	h.core = newTestCore(
		withMeta(mt),
		withMixCoord(h.mixCoord),
		withValidProxyManager(),
		withTsoAllocator(newMockTsoAllocator()),
		withBroker(&mockBroker{
			BroadcastAlteredCollectionFunc: func(ctx context.Context, collectionID int64) error {
				h.broadcasts++
				return nil
			},
		}),
	)
	h.callback = &DDLCallback{Core: h.core}
	return h, mt
}

// TestShardSplitRoutingAlterRetiresTheSourceOnADelistOnlyAdoption drives a
// replicated delist-only adoption onto a REAL meta table: every listed shard
// keeps its state and the fenced source is only removed.
func TestShardSplitRoutingAlterRetiresTheSourceOnADelistOnlyAdoption(t *testing.T) {
	coll := splitTestCollectionMeta(
		[]string{splitTestSource, splitTestTarget1, splitTestTarget2},
		map[string]*model.ShardInfo{
			splitTestSource:  {VChannelName: splitTestSource, State: schemapb.ShardState_ShardSplitting},
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1}},
		}, 2)
	coll.ShardBy = "hash(pk)"
	coll.State = etcdpb.CollectionState_CollectionCreated
	coll.ShardsNum = 2
	coll.PhysicalChannelNames = []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"}
	h, mt := realMetaHarness(t, coll)

	// Exactly one drain question across both deliveries.
	h.expectDrained(splitTestDrainResponse(true), nil).Once()

	result := splitTestAlterResultOn([]string{splitTestSource, splitTestTarget1, splitTestTarget2, splitTestControl}, true,
		splitTestAdoptionPostImage(), message.FieldMaskCollectionShardSplitRouting)

	require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), result))
	applied := mt.collID2Meta[splitTestCollID]
	require.Equal(t, []string{splitTestTarget1, splitTestTarget2}, applied.VirtualChannelNames)
	require.NotContains(t, applied.ShardInfos, splitTestSource, "the source is retired")
	require.Equal(t, schemapb.ShardState_ShardNormal, applied.ShardInfos[splitTestTarget1].State)
	require.EqualValues(t, 2, applied.ShardsNum)
	require.Equal(t, result.GetMaxTimeTick(), applied.UpdateTimestamp)
	require.Equal(t, 1, h.broadcasts)

	// Redelivered: a no-op. No drain question, no catalog write, only the cache
	// expiry again.
	require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), result))
	require.Same(t, applied, mt.collID2Meta[splitTestCollID])
	require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
	require.Equal(t, 2, h.broadcasts)
}

// TestSecondaryRoutingCommitsApplyInWALOrder is the secondary-side scenario the
// delta-only judge exists for, on a REAL meta table. Split 1 (s -> t1, t2) is
// applied here. Its adoption waits for this cluster's drain. Meanwhile split 2
// (b -> b1, b2), issued on the primary after adoption 1 and reaching its
// callback first here (a rename between the two gave the tasks different
// keys), carries a post-image that already reflects adoption 1. It must not
// retire s on adoption 1's behalf: it is ahead, CommitShardSplit is not
// called, the meta is untouched. Once adoption 1 drains and applies, split 2
// applies.
func TestSecondaryRoutingCommitsApplyInWALOrder(t *testing.T) {
	const b, b1, b2 = "by-dev-rootcoord-dml_3_100v3", "by-dev-rootcoord-dml_4_100v4", "by-dev-rootcoord-dml_5_100v5"
	coll := splitTestCollectionMeta([]string{splitTestSource, splitTestTarget1, splitTestTarget2, b}, map[string]*model.ShardInfo{
		splitTestSource:  {VChannelName: splitTestSource, State: schemapb.ShardState_ShardSplitting},
		splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{0}},
		splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{2}},
		b:                {VChannelName: b, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1, 3}},
	}, 4)
	coll.ShardBy = "hash(pk)"
	coll.State = etcdpb.CollectionState_CollectionCreated
	coll.ShardsNum = 3
	coll.PhysicalChannelNames = []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3"}
	h, mt := realMetaHarness(t, coll)

	adoption1 := &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2, b},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 2),
			pbShard(schemapb.ShardState_ShardNormal, 1, 3),
		},
		RoutingModulus: 4,
		ShardBy:        "hash(pk)",
		SplitTaskId:    7,
	}
	adoption1Result := splitTestAlterResultOn([]string{splitTestSource, splitTestTarget1, splitTestTarget2, b, splitTestControl}, true,
		adoption1, message.FieldMaskCollectionShardSplitRouting)
	split2 := &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2, b, b1, b2},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3", "by-dev-rootcoord-dml_4", "by-dev-rootcoord-dml_5"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 2),
			{State: schemapb.ShardState_ShardSplitting},
			pbShard(schemapb.ShardState_ShardCreating, 1),
			pbShard(schemapb.ShardState_ShardCreating, 3),
		},
		RoutingModulus: 4,
		ShardBy:        "hash(pk)",
	}
	split2Result := splitTestResultFor(split2, func(h *message.SplitShardMessageHeader) {
		h.SplitTaskId, h.SourceVchannel, h.TargetVchannels = 8, b, []string{b1, b2}
	}, b, b1, b2)
	task7 := func(drained bool) *datapb.CheckShardSplitDrainedResponse {
		return &datapb.CheckShardSplitDrainedResponse{
			Status: merr.Success(), Recorded: true, Drained: drained,
			SourceVchannels: []string{splitTestSource}, TargetVchannels: []string{splitTestTarget1, splitTestTarget2},
		}
	}
	before := mt.collID2Meta[splitTestCollID]

	// 1. Adoption 1 arrives; this cluster has not drained s.
	h.expectDrained(task7(false), nil).Once()
	err := h.callback.alterCollectionV2AckCallback(context.Background(), adoption1Result)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.ErrorContains(t, err, "not drained")
	require.Same(t, before, mt.collID2Meta[splitTestCollID])

	// 2. Split 2 overtakes it: ahead, no task recorded, meta untouched.
	err = h.callback.splitShardV2AckCallback(context.Background(), split2Result)
	requireAheadOfCollection(t, err)
	require.ErrorContains(t, err, "not this commit's to retire")
	require.Same(t, before, mt.collID2Meta[splitTestCollID])
	require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls, "CommitShardSplit was not called")
	require.Equal(t, 0, h.broadcasts)

	// 3. Adoption 1 retried, drained now: applies.
	h.expectDrained(task7(true), nil).Once()
	require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), adoption1Result))
	adopted := mt.collID2Meta[splitTestCollID]
	require.Equal(t, []string{splitTestTarget1, splitTestTarget2, b}, adopted.VirtualChannelNames)
	require.Equal(t, schemapb.ShardState_ShardNormal, adopted.ShardInfos[splitTestTarget1].State)
	require.Equal(t, 1, h.broadcasts)

	// 4. Split 2 retried: commits the task, then the routing.
	h.expectCommit(merr.Success(), nil).Once()
	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), split2Result))
	split := mt.collID2Meta[splitTestCollID]
	require.Equal(t, []string{splitTestTarget1, splitTestTarget2, b, b1, b2}, split.VirtualChannelNames)
	require.Equal(t, schemapb.ShardState_ShardSplitting, split.ShardInfos[b].State)
	require.Equal(t, schemapb.ShardState_ShardCreating, split.ShardInfos[b1].State)
	require.Equal(t, []uint64{1}, split.ShardInfos[b1].Buckets)
	require.Equal(t, []string{"CheckShardSplitDrained", "CheckShardSplitDrained", "CommitShardSplit"}, h.calls)
	require.Equal(t, 2, h.broadcasts)
}

// TestSecondaryRoutingCommitsRedeliveredAfterTheirTargetWasRetired: on a
// secondary, adoption 1 (task 7, s -> t1, t2) applied its meta but is stuck
// retrying its post-apply steps; the primary renamed the collection, split t1
// (task 9, t1 -> t11, t12 at modulus 8) and adopted it, and both applied here
// under the other name key. Adoption 1's next retry, and a redelivered split 1,
// find their own target t1 delisted: already applied, nothing written, the
// caches expired.
func TestSecondaryRoutingCommitsRedeliveredAfterTheirTargetWasRetired(t *testing.T) {
	const b, t11, t12 = "by-dev-rootcoord-dml_3_100v3", "by-dev-rootcoord-dml_4_100v4", "by-dev-rootcoord-dml_5_100v5"
	coll := splitTestCollectionMeta([]string{t11, t12, splitTestTarget2, b}, map[string]*model.ShardInfo{
		t11:              {VChannelName: t11, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
		t12:              {VChannelName: t12, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{4}},
		splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{2, 6}},
		b:                {VChannelName: b, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1, 3, 5, 7}},
	}, 8)
	coll.ShardBy = "hash(pk)"
	coll.State = etcdpb.CollectionState_CollectionCreated
	coll.ShardsNum = 4
	coll.PhysicalChannelNames = []string{"by-dev-rootcoord-dml_4", "by-dev-rootcoord-dml_5", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3"}
	h, mt := realMetaHarness(t, coll)
	before := mt.collID2Meta[splitTestCollID]

	task7 := &datapb.CheckShardSplitDrainedResponse{
		Status: merr.Success(), Recorded: true, Drained: true,
		SourceVchannels: []string{splitTestSource}, TargetVchannels: []string{splitTestTarget1, splitTestTarget2},
	}

	// Adoption 1 redelivered: its post-image is the primary's meta right after
	// it, at modulus 4, listing t1 Normal.
	adoption1 := &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  []string{splitTestTarget1, splitTestTarget2, b},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 2),
			pbShard(schemapb.ShardState_ShardNormal, 1, 3),
		},
		RoutingModulus: 4,
		ShardBy:        "hash(pk)",
		SplitTaskId:    7,
	}
	h.expectDrained(task7, nil).Once()
	require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(),
		splitTestAlterResultOn([]string{splitTestSource, splitTestTarget1, splitTestTarget2, b, splitTestControl}, true,
			adoption1, message.FieldMaskCollectionShardSplitRouting)))
	require.Same(t, before, mt.collID2Meta[splitTestCollID], "nothing written")
	require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls, "the drain is not asked again: the judge said already applied")
	require.Equal(t, 1, h.broadcasts)

	// Split 1 redelivered: the task is committed again (idempotent), the
	// routing is already applied.
	split1 := &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  []string{splitTestSource, splitTestTarget1, splitTestTarget2, b},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_3"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			pbShard(schemapb.ShardState_ShardCreating, 0),
			pbShard(schemapb.ShardState_ShardCreating, 2),
			pbShard(schemapb.ShardState_ShardNormal, 1, 3),
		},
		RoutingModulus: 4,
		ShardBy:        "hash(pk)",
	}
	h.expectDrained(task7, nil).Once()
	h.expectCommit(merr.Success(), nil).Once()
	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(),
		splitTestResultFor(split1, func(*message.SplitShardMessageHeader) {}, splitTestSource, splitTestTarget1, splitTestTarget2)))
	require.Same(t, before, mt.collID2Meta[splitTestCollID], "nothing written")
	require.Equal(t, []string{"CheckShardSplitDrained", "CheckShardSplitDrained", "CommitShardSplit"}, h.calls)
	require.Equal(t, 2, h.broadcasts)
}

// TestAdoptionCallbackAppliesANonSplitRoutingCommitImmediately: the drain gate
// belongs to a commit that retires a shard. An alter that carries no routing
// mask applies through the generic path; a routing commit that delists nothing
// -- an adoption that keeps the fenced source listed -- applies straight away,
// but still only through ApplyShardSplitRouting, and still with the delta its
// task record names.
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
		h.expectDrained(splitTestDrainResponse(false), nil).Once()
		h.expectApply(nil).Once()

		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestKeepSourceAdoption())))
		require.Equal(t, []string{"CheckShardSplitDrained", "ApplyShardSplitRouting"}, h.calls, "the record names the delta; the drain is not waited for")
		require.Equal(t, 1, h.broadcasts)
	})
}

// TestShardSplitRoutingAlterRefusesAMixedMessage: a routing commit is one atomic
// write through ApplyShardSplitRouting. A message that also asks for something
// else would need a second write through the generic path, so it is refused
// before anything runs rather than half-applied.
func TestShardSplitRoutingAlterRefusesAMixedMessage(t *testing.T) {
	t.Run("another field mask", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		updates := splitTestAdoptionPostImage()
		updates.Description = "and a description"
		err := h.callback.alterCollectionV2AckCallback(context.Background(),
			splitTestAlterResult(updates, message.FieldMaskCollectionShardSplitRouting, message.FieldMaskCollectionDescription))
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, message.FieldMaskCollectionDescription)
		require.Empty(t, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})

	t.Run("a load config change", func(t *testing.T) {
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		updates := splitTestAdoptionPostImage()
		updates.AlterLoadConfig = &messagespb.AlterLoadConfigOfAlterCollection{ReplicaNumber: 2}
		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(updates))
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "applied on its own")
		require.Empty(t, h.calls)
	})
}

// TestShardSplitRoutingAlterHonoursTheDecisionUnderTheLock: the gate judges a
// snapshot, ApplyShardSplitRouting judges again under ddLock, and its no-write
// outcomes are not failures.
func TestShardSplitRoutingAlterHonoursTheDecisionUnderTheLock(t *testing.T) {
	for _, tc := range []struct {
		name       string
		applyErr   error
		wantErr    bool
		broadcasts int
	}{
		{name: "already applied", applyErr: routing.ErrCommitAlreadyApplied, broadcasts: 1},
		{name: "ahead of the collection under the lock", applyErr: errors.Mark(merr.WrapErrServiceUnavailableMsg("split not applied"), routing.ErrCommitAheadOfCollection), wantErr: true, broadcasts: 0},
		{name: "collection vanished", applyErr: errAlterCollectionNotFound, broadcasts: 0},
		{name: "catalog failure", applyErr: errors.New("etcd down"), wantErr: true, broadcasts: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
			h.expectDrained(splitTestDrainResponse(false), nil).Once()
			h.expectApply(tc.applyErr).Once()
			err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestKeepSourceAdoption()))
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.broadcasts, h.broadcasts)
		})
	}
}

// TestAdoptionCallbackExpiresTheCachesOfTheCollectionItLoaded: the proxies
// route by the collection meta they cache, so an applied adoption must expire
// that cache under every name THIS cluster resolves the collection by -- read
// off the collection the callback loaded and its aliases, as the SplitShard
// callback does. The header's list is an addition: no adoption issuer fills
// it on this branch, and on a secondary a rename applied in between leaves it
// naming a collection this cluster no longer knows.
func TestAdoptionCallbackExpiresTheCachesOfTheCollectionItLoaded(t *testing.T) {
	type expired struct{ db, name string }
	run := func(t *testing.T, headerOpt func(*messagespb.AlterCollectionMessageHeader), byName func(*mockrootcoord.IMetaTable)) ([]expired, error) {
		coll := splitTestMidSplitCollection()
		h := newSplitCallbackHarness(t, nil)
		h.meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, splitTestCollID, mock.Anything, mock.Anything).Return(coll, nil)
		byName(h.meta)
		h.expectDrained(splitTestDrainResponse(true), nil).Once()
		h.expectApply(nil).Once()

		var got []expired
		pcm := proxyutil.NewMockProxyClientManager(t)
		pcm.EXPECT().InvalidateCollectionMetaCache(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, req *proxypb.InvalidateCollMetaCacheRequest, _ ...proxyutil.ExpireCacheOpt) error {
				require.Equal(t, splitTestCollID, req.GetCollectionID())
				got = append(got, expired{req.GetDbName(), req.GetCollectionName()})
				return nil
			}).Maybe()
		h.core.proxyClientManager = pcm

		// An adoption that delists nothing: no reach check, no drain to wait for.
		result := splitTestAlterResultWithHeader([]string{splitTestSource, splitTestTarget1, splitTestTarget2, splitTestControl}, true,
			splitTestKeepSourceAdoption(), headerOpt, message.FieldMaskCollectionShardSplitRouting)
		err := h.callback.alterCollectionV2AckCallback(context.Background(), result)
		require.Equal(t, []string{"CheckShardSplitDrained", "ApplyShardSplitRouting"}, h.calls)
		return got, err
	}
	resolves := func(aliases ...string) func(*mockrootcoord.IMetaTable) {
		return func(meta *mockrootcoord.IMetaTable) {
			meta.EXPECT().GetCollectionByName(mock.Anything, splitTestDB, splitTestCollection, mock.Anything, mock.Anything).
				Return(splitTestMidSplitCollection(), nil)
			meta.EXPECT().ListAliases(mock.Anything, splitTestDB, splitTestCollection, mock.Anything).Return(aliases, nil)
		}
	}
	own := expired{splitTestDB, splitTestCollection}

	t.Run("an empty header list still expires the collection and its aliases", func(t *testing.T) {
		got, err := run(t, func(h *messagespb.AlterCollectionMessageHeader) { h.CacheExpirations = nil }, resolves("ali"))
		require.NoError(t, err)
		require.ElementsMatch(t, []expired{own, {splitTestDB, "ali"}}, got)
	})
	t.Run("a stale name in the header is expired as well as the collection's own", func(t *testing.T) {
		got, err := run(t, func(h *messagespb.AlterCollectionMessageHeader) {
			h.CacheExpirations = ce.NewBuilder().WithLegacyProxyCollectionMetaCache(
				ce.OptLPCMDBName(splitTestDB),
				ce.OptLPCMCollectionName("renamed_away"),
				ce.OptLPCMCollectionID(splitTestCollID),
				ce.OptLPCMMsgType(commonpb.MsgType_AlterCollection),
			).Build()
		}, resolves())
		require.NoError(t, err)
		require.ElementsMatch(t, []expired{own, {splitTestDB, "renamed_away"}}, got)
	})
	t.Run("a header already naming the collection expires it once", func(t *testing.T) {
		got, err := run(t, nil, resolves())
		require.NoError(t, err)
		require.Equal(t, []expired{own}, got)
	})
	t.Run("a collection being dropped expires only what the header names", func(t *testing.T) {
		got, err := run(t, nil, func(meta *mockrootcoord.IMetaTable) {
			meta.EXPECT().GetCollectionByName(mock.Anything, splitTestDB, splitTestCollection, mock.Anything, mock.Anything).
				Return(nil, merr.WrapErrCollectionNotFound(splitTestCollection))
		})
		require.NoError(t, err)
		require.Equal(t, []expired{own}, got)
	})
	t.Run("a meta failure while collecting the names is returned", func(t *testing.T) {
		_, err := run(t, nil, func(meta *mockrootcoord.IMetaTable) {
			meta.EXPECT().GetCollectionByName(mock.Anything, splitTestDB, splitTestCollection, mock.Anything, mock.Anything).
				Return(nil, errors.New("etcd down"))
		})
		require.ErrorContains(t, err, "etcd down")
	})
}

// splitTestDropReadyCollection is the pre-split collection on a REAL meta, with
// what DropCollection needs to mark it Dropping and settle its counters.
func splitTestDropReadyCollection(t *testing.T) (*splitCallbackHarness, *MetaTable) {
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	coll.State = etcdpb.CollectionState_CollectionCreated
	coll.ShardsNum = 1
	coll.Partitions = []*model.Partition{{PartitionID: 10, PartitionName: "_default", State: etcdpb.PartitionState_PartitionCreated}}
	h, mt := realMetaHarness(t, coll)
	mt.catalog.(*catalogmocks.RootCoordCatalog).EXPECT().
		DeleteGrantByCollectionName(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()
	mt.generalCnt = 1
	channel.StaticPChannelStatsManager.MustGet().AddVChannel(splitTestSource)
	return h, mt
}

// requireTargetsUnregistered asserts that no pchannel stats were left behind for
// the split's targets.
func requireTargetsUnregistered(t *testing.T) {
	t.Helper()
	stats := channel.StaticPChannelStatsManager.MustGet()
	for _, target := range []string{splitTestTarget1, splitTestTarget2} {
		require.Zero(t, stats.GetPChannelStats(types.ChannelID{Name: funcutil.ToPhysicalChannel(target)}).VChannelCount(), target)
	}
}

// TestSplitShardAckCallbackIgnoresACollectionBeingDropped: on a secondary a
// rename between the split and a DropCollection gives their callbacks different
// collection-name keys, so the drop can mark the collection Dropping while the
// split's callback is still retrying. The retry must stop without applying the
// routing -- which would re-count shards and re-register targets the drop has
// already settled, for good -- and without recording the task at DataCoord.
func TestSplitShardAckCallbackIgnoresACollectionBeingDropped(t *testing.T) {
	t.Run("dropping before a retry", func(t *testing.T) {
		h, mt := splitTestDropReadyCollection(t)
		// The first attempt fails at DataCoord and is retried.
		h.mixCoord.On("CommitShardSplit", mock.Anything, mock.Anything).
			Return(nil, errors.New("datacoord unavailable")).Once()
		require.Error(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))

		require.NoError(t, mt.DropCollection(context.Background(), splitTestCollID, 200))
		require.Equal(t, etcdpb.CollectionState_CollectionDropping, mt.collID2Meta[splitTestCollID].State)
		require.Zero(t, mt.generalCnt)

		// The retry stops: no DataCoord call (the mock would fail it), no apply.
		require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
		dropping := mt.collID2Meta[splitTestCollID]
		require.Equal(t, []string{splitTestSource}, dropping.VirtualChannelNames)
		require.EqualValues(t, 1, dropping.ShardsNum)
		require.Zero(t, mt.generalCnt, "the drop settled the capacity count; the split must not move it")
		requireTargetsUnregistered(t)
		require.Equal(t, 0, h.broadcasts)
	})

	t.Run("dropping between the check and the apply", func(t *testing.T) {
		coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
			splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
		}, 0)
		h := newSplitCallbackHarness(t, coll)
		h.expectCommit(merr.Success(), nil).Once()
		h.expectApply(errShardSplitRoutingCollectionUnavailable).Once()

		require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
		require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, 0, h.broadcasts, "BroadcastAlteredCollection cannot resolve a Dropping collection and must not be reached")
	})
}

// TestAdoptionCallbackIgnoresACollectionBeingDropped: an adoption retrying on
// this cluster's drain gate while the collection is dropped stops, without
// waiting on the drain any longer and without applying.
func TestAdoptionCallbackIgnoresACollectionBeingDropped(t *testing.T) {
	t.Run("the apply refuses it, the drain is not asked", func(t *testing.T) {
		coll := splitTestMidSplitCollection()
		coll.State = etcdpb.CollectionState_CollectionDropping
		h := newSplitCallbackHarness(t, coll)
		// No CheckShardSplitDrained expectation: the strict mock fails the test
		// if the drain gate is consulted.
		var gotDelta routing.CommitDelta
		h.meta.EXPECT().ApplyShardSplitRouting(mock.Anything, splitTestCollID, mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, _ int64, _ *messagespb.AlterCollectionMessageUpdates, delta routing.CommitDelta, _ uint64) {
				h.record("ApplyShardSplitRouting")
				gotDelta = delta
			}).Return(errShardSplitRoutingCollectionUnavailable).Once()

		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Equal(t, []string{"ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, routing.CommitDelta{}, gotDelta, "nothing is judged for a Dropping collection")
		require.Equal(t, 0, h.broadcasts)
	})

	t.Run("on a real meta, after waiting on the drain", func(t *testing.T) {
		coll := splitTestMidSplitCollection()
		coll.State = etcdpb.CollectionState_CollectionCreated
		coll.ShardsNum = 2
		coll.PhysicalChannelNames = []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"}
		h, mt := realMetaHarness(t, coll)
		mt.generalCnt = 7

		h.expectDrained(splitTestDrainResponse(false), nil).Once()
		err := h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage()))
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)

		dropping := mt.collID2Meta[splitTestCollID].Clone()
		dropping.State = etcdpb.CollectionState_CollectionDropping
		mt.collID2Meta[splitTestCollID] = dropping

		require.NoError(t, h.callback.alterCollectionV2AckCallback(context.Background(), splitTestRoutingAlterResult(splitTestAdoptionPostImage())))
		require.Same(t, dropping, mt.collID2Meta[splitTestCollID], "nothing is written")
		require.Equal(t, 7, mt.generalCnt)
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls, "the drain is not asked again")
		require.Equal(t, 0, h.broadcasts)
	})
}
