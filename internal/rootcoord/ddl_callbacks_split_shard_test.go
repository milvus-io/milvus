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
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	splitTestDB         = "test_db"
	splitTestCollection = "test_collection"
	splitTestCollID     = int64(100)
)

var (
	splitTestSource  = "by-dev-rootcoord-dml_0_100v0"
	splitTestTarget1 = "by-dev-rootcoord-dml_1_100v1"
	splitTestTarget2 = "by-dev-rootcoord-dml_2_100v2"
	splitTestControl = funcutil.GetControlChannel("by-dev-rootcoord-cchannel")
)

// splitTestPostImage is the routing post-image of the write switch: the source
// fenced, the two targets created and owning one residue each.
func splitTestPostImage() *messagespb.AlterCollectionMessageUpdates {
	return &messagespb.AlterCollectionMessageUpdates{
		VirtualChannelNames:  []string{splitTestSource, splitTestTarget1, splitTestTarget2},
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			{State: schemapb.ShardState_ShardSplitting},
			pbShard(schemapb.ShardState_ShardCreating, 0),
			pbShard(schemapb.ShardState_ShardCreating, 1),
		},
		RoutingModulus: 2,
		ShardBy:        "hash(pk)",
	}
}

// splitTestSwitchTimeTick is the T_switch the source's StreamingNode reports in
// splitTestResult: deliberately below the source replica's append tick (100),
// which is what a re-driven fence looks like.
const splitTestSwitchTimeTick = uint64(90)

// splitTestSwitchExtra is a source replica's extra append response.
func splitTestSwitchExtra(switchTimeTick uint64) *anypb.Any {
	extra, err := anypb.New(&message.SplitShardExtraResponse{SplitTimeTick: switchTimeTick})
	if err != nil {
		panic(err)
	}
	return extra
}

// splitTestResult builds the broadcast result the ack callback receives: one
// append result per replica, the source's ticks strictly below the targets'.
// The optional mutators corrupt the header's names.
func splitTestResult(postImage *messagespb.AlterCollectionMessageUpdates, headerOpts ...func(*message.SplitShardMessageHeader)) message.BroadcastResultSplitShardMessageV2 {
	return splitTestResultWithGenesisProperties(postImage, nil, headerOpts...)
}

// splitTestResultWithGenesisProperties is splitTestResult whose genesis schema
// carries the given collection properties.
func splitTestResultWithGenesisProperties(postImage *messagespb.AlterCollectionMessageUpdates, properties []*commonpb.KeyValuePair, headerOpts ...func(*message.SplitShardMessageHeader)) message.BroadcastResultSplitShardMessageV2 {
	return splitTestResultWithGenesisSchema(postImage, &schemapb.CollectionSchema{Name: splitTestCollection, Properties: properties}, headerOpts...)
}

// splitTestResultWithGenesisSchema is splitTestResult whose genesis carries the
// given collection schema.
func splitTestResultWithGenesisSchema(postImage *messagespb.AlterCollectionMessageUpdates, schema *schemapb.CollectionSchema, headerOpts ...func(*message.SplitShardMessageHeader)) message.BroadcastResultSplitShardMessageV2 {
	header := &message.SplitShardMessageHeader{
		CollectionId:    splitTestCollID,
		SplitTaskId:     7,
		SourceVchannel:  splitTestSource,
		TargetVchannels: []string{splitTestTarget1, splitTestTarget2},
		PartitionIds:    []int64{10},
	}
	for _, opt := range headerOpts {
		if opt != nil {
			opt(header)
		}
	}
	raw := message.NewSplitShardMessageBuilderV2().
		WithHeader(header).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: schema},
			Routing: postImage,
		}).
		WithBroadcast(
			[]string{splitTestSource, splitTestTarget1, splitTestTarget2, splitTestControl},
			message.OptBuildBroadcastAppendFirst(splitTestSource),
		).
		MustBuildBroadcast()
	return message.BroadcastResultSplitShardMessageV2{
		Message: message.MustAsBroadcastSplitShardMessageV2(raw),
		Results: map[string]*message.AppendResult{
			splitTestSource:  {MessageID: rmq.NewRmqID(1), TimeTick: 100, Extra: splitTestSwitchExtra(splitTestSwitchTimeTick)},
			splitTestTarget1: {MessageID: rmq.NewRmqID(2), TimeTick: 101},
			splitTestTarget2: {MessageID: rmq.NewRmqID(3), TimeTick: 102},
			// Deliberately below the targets' ticks: the control channel replica
			// is not the max of the broadcast, which is what makes
			// TestSplitShardAckCallbackCommitsTheRouting catch a regression to
			// stamping the routing commit with the control channel's own tick.
			splitTestControl: {MessageID: rmq.NewRmqID(4), TimeTick: 95},
		},
	}
}

// splitTestResultFor is splitTestResult for a split of another source into
// other targets: the header is mutated by headerOpt and the results carry the
// given replicas.
func splitTestResultFor(postImage *messagespb.AlterCollectionMessageUpdates, headerOpt func(*message.SplitShardMessageHeader), source string, targets ...string) message.BroadcastResultSplitShardMessageV2 {
	header := &message.SplitShardMessageHeader{
		CollectionId:    splitTestCollID,
		SplitTaskId:     7,
		SourceVchannel:  splitTestSource,
		TargetVchannels: []string{splitTestTarget1, splitTestTarget2},
		PartitionIds:    []int64{10},
	}
	headerOpt(header)
	vchannels := append(append([]string{source}, targets...), splitTestControl)
	raw := message.NewSplitShardMessageBuilderV2().
		WithHeader(header).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: splitTestCollection}},
			Routing: postImage,
		}).
		WithBroadcast(vchannels, message.OptBuildBroadcastAppendFirst(source)).
		MustBuildBroadcast()
	results := make(map[string]*message.AppendResult, len(vchannels))
	for i, vchannel := range vchannels {
		results[vchannel] = &message.AppendResult{MessageID: rmq.NewRmqID(int64(i + 1)), TimeTick: uint64(100 + i)}
	}
	results[source].Extra = splitTestSwitchExtra(splitTestSwitchTimeTick)
	return message.BroadcastResultSplitShardMessageV2{
		Message: message.MustAsBroadcastSplitShardMessageV2(raw),
		Results: results,
	}
}

// splitTestCollectionMeta is the collection as the callback finds it: a single
// never-split shard, unless shardInfos says otherwise.
func splitTestCollectionMeta(vchannels []string, shardInfos map[string]*model.ShardInfo, modulus uint64) *model.Collection {
	return &model.Collection{
		CollectionID:         splitTestCollID,
		Name:                 splitTestCollection,
		DBName:               splitTestDB,
		DBID:                 1,
		VirtualChannelNames:  vchannels,
		PhysicalChannelNames: []string{"by-dev-rootcoord-dml_0"},
		ShardInfos:           shardInfos,
		RoutingModulus:       modulus,
	}
}

type splitCallbackHarness struct {
	core       *Core
	callback   *DDLCallback
	meta       *mockrootcoord.IMetaTable
	mixCoord   *imocks.MixCoord
	calls      []string
	callsMu    sync.Mutex
	broadcasts int
}

func (h *splitCallbackHarness) record(name string) {
	h.callsMu.Lock()
	defer h.callsMu.Unlock()
	h.calls = append(h.calls, name)
}

// newSplitCallbackHarness wires a DDLCallback whose meta, mixcoord, broker and
// proxy manager are all observable, and records the order the two commit halves
// are invoked in.
func newSplitCallbackHarness(t *testing.T, coll *model.Collection) *splitCallbackHarness {
	h := &splitCallbackHarness{}
	h.meta = mockrootcoord.NewIMetaTable(t)
	if coll != nil {
		h.meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, splitTestCollID, mock.Anything, mock.Anything).
			Return(coll, nil).Maybe()
		h.meta.EXPECT().GetCollectionByName(mock.Anything, splitTestDB, splitTestCollection, mock.Anything, mock.Anything).
			Return(coll, nil).Maybe()
		h.meta.EXPECT().ListAliases(mock.Anything, splitTestDB, splitTestCollection, mock.Anything).
			Return(nil, nil).Maybe()
	}
	h.mixCoord = imocks.NewMixCoord(t)
	h.core = newTestCore(
		withMeta(h.meta),
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
	return h
}

// expectApply arms ApplyShardSplitRouting and records that it ran.
func (h *splitCallbackHarness) expectApply(err error) *mockrootcoord.IMetaTable_ApplyShardSplitRouting_Call {
	return h.meta.EXPECT().ApplyShardSplitRouting(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(context.Context, int64, *messagespb.AlterCollectionMessageUpdates, routing.CommitDelta, uint64) {
			h.record("ApplyShardSplitRouting")
		}).Return(err)
}

// expectRecorded arms CheckShardSplitDrained, which the SplitShard callback
// asks only about a source the collection does not list, and records that it
// ran.
func (h *splitCallbackHarness) expectRecorded(recorded bool) *mock.Call {
	return h.mixCoord.On("CheckShardSplitDrained", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { h.record("CheckShardSplitDrained") }).
		Return(&datapb.CheckShardSplitDrainedResponse{Status: merr.Success(), Recorded: recorded}, nil)
}

// expectCommit arms CommitShardSplit and records that it ran.
func (h *splitCallbackHarness) expectCommit(status *commonpb.Status, err error) *mock.Call {
	return h.mixCoord.On("CommitShardSplit", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { h.record("CommitShardSplit") }).
		Return(status, err)
}

// TestSplitShardAckCallbackCommitsTheRouting is the happy path: the post-image
// the message carries is committed to the meta, datacoord is told, and the proxy
// caches are expired.
func TestSplitShardAckCallbackCommitsTheRouting(t *testing.T) {
	// dataCoord.shardSplit.enable gates issuing a split, never committing one
	// already in the WAL: the callback commits with the switch off.
	key := paramtable.Get().DataCoordCfg.ShardSplitEnable.Key
	paramtable.Get().Save(key, "false")
	defer paramtable.Get().Reset(key)

	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	h := newSplitCallbackHarness(t, coll)
	h.expectCommit(merr.Success(), nil).Once()

	var gotCollectionID int64
	var gotUpdates *messagespb.AlterCollectionMessageUpdates
	var gotTimetick uint64
	var gotDelta routing.CommitDelta
	h.meta.EXPECT().ApplyShardSplitRouting(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, collectionID int64, updates *messagespb.AlterCollectionMessageUpdates, delta routing.CommitDelta, timetick uint64) {
			h.record("ApplyShardSplitRouting")
			gotCollectionID, gotUpdates, gotDelta, gotTimetick = collectionID, updates, delta, timetick
		}).Return(nil).Once()

	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))

	require.Equal(t, splitTestCollID, gotCollectionID)
	require.Equal(t, []string{splitTestSource, splitTestTarget1, splitTestTarget2}, gotUpdates.GetVirtualChannelNames())
	// The split's own delta, judged again under the lock: the header's source
	// fenced, the header's two targets created, the task recorded by now.
	require.Equal(t, routing.SplitDelta(splitTestSource, []string{splitTestTarget1, splitTestTarget2}, true), gotDelta)
	// Every other collection-meta write stamps UpdateTimestamp/the snapshot ts
	// with the max tick over the broadcast's replicas (see the adoption
	// callback, ddl_callbacks_commit_shard_split_routing.go); the split's own
	// routing commit must use the same source, not the control channel's own
	// (possibly smaller) tick, or a secondary can persist it out of order
	// against a straggling sibling broadcast (adv-L1-report.md AV-L1-M1).
	require.EqualValues(t, 102, gotTimetick)
	require.Equal(t, 1, h.broadcasts)
	require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
}

// TestSplitShardAckCallbackCommitsTheTaskAtDataCoord pins the datacoord half:
// one source tick and one genesis position per target, and -- because a target
// with no seeded checkpoint reads back the collection's creation position --
// datacoord is told BEFORE the routing post-image makes the target visible.
func TestSplitShardAckCallbackCommitsTheTaskAtDataCoord(t *testing.T) {
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	h := newSplitCallbackHarness(t, coll)
	h.expectApply(nil).Once()

	var got *datapb.CommitShardSplitRequest
	h.mixCoord.On("CommitShardSplit", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			h.record("CommitShardSplit")
			got = args.Get(1).(*datapb.CommitShardSplitRequest)
		}).Return(merr.Success(), nil).Once()

	result := splitTestResult(splitTestPostImage())
	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), result))

	require.NotNil(t, got)
	require.Equal(t, splitTestCollID, got.GetCollectionId())
	require.EqualValues(t, 7, got.GetSplitTaskId())
	require.EqualValues(t, 2, got.GetRoutingModulus())
	require.Len(t, got.GetSources(), 1)
	require.Equal(t, splitTestSource, got.GetSources()[0].GetVchannel())
	// T_switch is the fence tick the source's StreamingNode reported, not the
	// append tick of the (possibly re-driven) source replica.
	require.Equal(t, splitTestSwitchTimeTick, got.GetSources()[0].GetSwitchTimeTick())
	require.Len(t, got.GetTargets(), 2)
	require.Equal(t, splitTestTarget1, got.GetTargets()[0].GetVchannel())
	require.Equal(t, []uint64{0}, got.GetTargets()[0].GetBuckets())
	require.Equal(t, splitTestTarget2, got.GetTargets()[1].GetVchannel())
	require.Equal(t, []uint64{1}, got.GetTargets()[1].GetBuckets())

	require.Len(t, got.GetTargetStartPositions(), 2)
	for i, vchannel := range []string{splitTestTarget1, splitTestTarget2} {
		position := got.GetTargetStartPositions()[i]
		appendResult := result.Results[vchannel]
		require.Equal(t, vchannel, position.GetChannelName())
		require.Equal(t, adaptor.MustGetMQWrapperIDFromMessage(appendResult.MessageID).Serialize(), position.GetMsgID())
		require.Equal(t, commonpb.WALName(appendResult.MessageID.WALName()), position.GetWALName())
		require.Equal(t, appendResult.TimeTick, position.GetTimestamp())
	}

	// datacoord first: the target's checkpoint must exist before the routing
	// post-image makes querycoord aware of the target vchannel.
	require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
}

// TestSplitShardAckCallbackRetriesWhenDataCoordFails: both a transport error and
// a non-OK status propagate, and the meta is left alone so the retry redoes the
// whole commit from the top.
func TestSplitShardAckCallbackRetriesWhenDataCoordFails(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status *commonpb.Status
		err    error
	}{
		{name: "transport error", status: nil, err: errors.New("rpc error")},
		{name: "non-ok status", status: merr.Status(merr.WrapErrServiceInternalMsg("datacoord is busy")), err: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
				splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
			}, 0)
			h := newSplitCallbackHarness(t, coll)
			h.expectCommit(tc.status, tc.err).Once()

			err := h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage()))
			require.Error(t, err)
			// Nothing after the datacoord half ran: no meta write, no cache expiry.
			require.Equal(t, []string{"CommitShardSplit"}, h.calls)
			require.Equal(t, 0, h.broadcasts)
		})
	}
}

// TestSplitShardAckCallbackIsIdempotent: a redelivered callback on a collection
// that already carries the post-image writes no meta -- the meta table reports
// that under its own lock -- but still tells datacoord and still expires the
// caches, because neither is known to have happened.
func TestSplitShardAckCallbackIsIdempotent(t *testing.T) {
	h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
	h.expectCommit(merr.Success(), nil).Once()
	h.expectApply(routing.ErrCommitAlreadyApplied).Once()

	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
	require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
	require.Equal(t, 1, h.broadcasts)
}

// TestSplitShardAckCallbackWaitsForAnEarlierRoutingCommit: a write switch whose
// post-image reflects a routing commit of the collection this cluster has not
// applied is ahead of the meta. It is judged so BEFORE the datacoord half, so
// no task is recorded for a split that has not applied here; the callback
// returns the retriable refusal and expires nothing.
func TestSplitShardAckCallbackWaitsForAnEarlierRoutingCommit(t *testing.T) {
	const b, b1, b2 = "by-dev-rootcoord-dml_3_100v3", "by-dev-rootcoord-dml_4_100v4", "by-dev-rootcoord-dml_5_100v5"
	// Split 2 (b -> b1, b2) issued on the primary after adoption 1 retired the
	// source of split 1; its post-image no longer lists that source and lists
	// t1/t2 as Normal.
	split2 := func() *messagespb.AlterCollectionMessageUpdates {
		return &messagespb.AlterCollectionMessageUpdates{
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
	}
	header := func(h *message.SplitShardMessageHeader) {
		h.SplitTaskId, h.SourceVchannel, h.TargetVchannels = 8, b, []string{b1, b2}
	}

	t.Run("adoption 1 not applied here: ahead, nothing recorded", func(t *testing.T) {
		coll := splitTestCollectionMeta([]string{splitTestSource, splitTestTarget1, splitTestTarget2, b}, map[string]*model.ShardInfo{
			splitTestSource:  {VChannelName: splitTestSource, State: schemapb.ShardState_ShardSplitting},
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardCreating, Buckets: []uint64{2}},
			b:                {VChannelName: b, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1, 3}},
		}, 4)
		coll.ShardBy = "hash(pk)"
		h := newSplitCallbackHarness(t, coll)

		err := h.callback.splitShardV2AckCallback(context.Background(), splitTestResultFor(split2(), header, b, b1, b2))
		require.Error(t, err)
		require.True(t, errors.Is(err, routing.ErrCommitAheadOfCollection))
		require.True(t, merr.IsRetryableErr(err))
		require.Empty(t, h.calls, "CommitShardSplit is not called for a split this cluster cannot apply yet")
		require.Equal(t, 0, h.broadcasts)
	})

	t.Run("adoption 1 applied: the same split commits", func(t *testing.T) {
		coll := splitTestCollectionMeta([]string{splitTestTarget1, splitTestTarget2, b}, map[string]*model.ShardInfo{
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{2}},
			b:                {VChannelName: b, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1, 3}},
		}, 4)
		coll.ShardBy = "hash(pk)"
		h := newSplitCallbackHarness(t, coll)
		h.expectCommit(merr.Success(), nil).Once()
		var gotDelta routing.CommitDelta
		h.meta.EXPECT().ApplyShardSplitRouting(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Run(func(_ context.Context, _ int64, _ *messagespb.AlterCollectionMessageUpdates, delta routing.CommitDelta, _ uint64) {
				h.record("ApplyShardSplitRouting")
				gotDelta = delta
			}).Return(nil).Once()

		require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResultFor(split2(), header, b, b1, b2)))
		require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, routing.SplitDelta(b, []string{b1, b2}, true), gotDelta)
		require.Equal(t, 1, h.broadcasts)
	})

	t.Run("ahead under the lock", func(t *testing.T) {
		// The gate judged a snapshot; the meta table judges again under ddLock
		// and its retriable answer is returned as such.
		h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
		h.expectCommit(merr.Success(), nil).Once()
		h.expectApply(errors.Mark(merr.WrapErrServiceUnavailableMsg("an earlier commit is not applied here"), routing.ErrCommitAheadOfCollection)).Once()

		err := h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage()))
		require.True(t, errors.Is(err, routing.ErrCommitAheadOfCollection))
		require.True(t, merr.IsRetryableErr(err))
		require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})
}

// TestSplitShardAckCallbackAsksDataCoordAboutAnUnlistedSource: a source the
// collection does not list is either retired -- this split applied and adopted
// here, so the task is recorded -- or never created here, because it is the
// target of an earlier split this cluster has not applied. Only datacoord's
// record tells the two apart.
func TestSplitShardAckCallbackAsksDataCoordAboutAnUnlistedSource(t *testing.T) {
	adopted := func() *model.Collection {
		coll := splitTestCollectionMeta([]string{splitTestTarget1, splitTestTarget2}, map[string]*model.ShardInfo{
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1}},
		}, 2)
		coll.ShardBy = "hash(pk)"
		return coll
	}

	t.Run("recorded: a redelivery after the adoption", func(t *testing.T) {
		h := newSplitCallbackHarness(t, adopted())
		h.expectRecorded(true).Once()
		h.expectCommit(merr.Success(), nil).Once()
		h.expectApply(routing.ErrCommitAlreadyApplied).Once()

		require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
		require.Equal(t, []string{"CheckShardSplitDrained", "CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, 1, h.broadcasts)
	})

	t.Run("not recorded: the split that creates the source is not applied here", func(t *testing.T) {
		// The collection lists only a shard this split's post-image does not
		// know: the split of that shard, whose target is this split's source,
		// has not applied here.
		coll := splitTestCollectionMeta([]string{"by-dev-rootcoord-dml_3_100v3"}, map[string]*model.ShardInfo{
			"by-dev-rootcoord-dml_3_100v3": {VChannelName: "by-dev-rootcoord-dml_3_100v3", State: schemapb.ShardState_ShardNormal},
		}, 0)
		h := newSplitCallbackHarness(t, coll)
		h.expectRecorded(false).Once()

		err := h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage()))
		require.True(t, errors.Is(err, routing.ErrCommitAheadOfCollection))
		require.True(t, merr.IsRetryableErr(err))
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})

	t.Run("datacoord unreachable", func(t *testing.T) {
		h := newSplitCallbackHarness(t, adopted())
		h.mixCoord.On("CheckShardSplitDrained", mock.Anything, mock.Anything).
			Run(func(mock.Arguments) { h.record("CheckShardSplitDrained") }).
			Return(nil, errors.New("rpc error")).Once()

		require.Error(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
		require.Equal(t, []string{"CheckShardSplitDrained"}, h.calls)
	})
}

// TestSplitShardAckCallbackReportsASidewaysPostImage: a transition that is
// neither forward nor overtaken is a genuine coordinator bug and stays loud.
func TestSplitShardAckCallbackReportsASidewaysPostImage(t *testing.T) {
	h := newSplitCallbackHarness(t, splitTestMidSplitCollection())
	h.expectCommit(merr.Success(), nil).Once()
	h.expectApply(merr.WrapErrServiceInternalMsg("shard cannot go from Dropped back to Splitting")).Once()

	err := h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage()))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, 0, h.broadcasts)
}

// TestSplitShardAckCallbackIgnoresADroppedCollection: a collection dropped while
// its split was in flight has nothing left to commit, and the callback must
// finish rather than retry forever.
func TestSplitShardAckCallbackIgnoresADroppedCollection(t *testing.T) {
	t.Run("gone before the lookup", func(t *testing.T) {
		h := newSplitCallbackHarness(t, nil)
		h.meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, splitTestCollID, mock.Anything, mock.Anything).
			Return(nil, merr.WrapErrCollectionNotFound(splitTestCollID)).Once()

		require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
		require.Empty(t, h.calls)
	})

	t.Run("gone between the lookup and the apply", func(t *testing.T) {
		coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
			splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
		}, 0)
		h := newSplitCallbackHarness(t, coll)
		h.expectCommit(merr.Success(), nil).Once()
		h.expectApply(errAlterCollectionNotFound).Once()

		require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
		require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
		require.Equal(t, 0, h.broadcasts)
	})

	t.Run("a real lookup failure is retried", func(t *testing.T) {
		h := newSplitCallbackHarness(t, nil)
		h.meta.EXPECT().GetCollectionByID(mock.Anything, mock.Anything, splitTestCollID, mock.Anything, mock.Anything).
			Return(nil, errors.New("etcd down")).Once()

		require.Error(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
		require.Empty(t, h.calls)
	})
}

// TestSplitShardAckCallbackRefusesAPostImageThatDoesNotTile: a post-image whose
// residues overlap sends one key to two shards. Refused before anything is
// committed, and as a System error -- the post-image was derived by a
// coordinator, so this is a Milvus bug and never the request's fault.
func TestSplitShardAckCallbackRefusesAPostImageThatDoesNotTile(t *testing.T) {
	for _, tc := range []struct {
		name      string
		postImage func() *messagespb.AlterCollectionMessageUpdates
		header    func(*message.SplitShardMessageHeader)
	}{
		{
			name: "overlapping residues",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				p := splitTestPostImage()
				p.ShardInfos[1] = pbShard(schemapb.ShardState_ShardCreating, 0, 1)
				return p
			},
		},
		{
			name: "a gap no shard claims",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				p := splitTestPostImage()
				p.RoutingModulus = 4
				return p
			},
		},
		{
			name: "non-parallel arrays",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				p := splitTestPostImage()
				p.ShardInfos = p.ShardInfos[:2]
				return p
			},
		},
		{
			name: "a shard info naming another vchannel",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				p := splitTestPostImage()
				// A permutation still tiles [0, M) exactly, so only the name can
				// catch it -- and if it is not caught, every residue binds to a
				// shard that does not own it.
				p.ShardInfos[1].VchannelName = splitTestTarget2
				return p
			},
		},
		{
			name: "a header target the post-image does not name",
			header: func(h *message.SplitShardMessageHeader) {
				h.TargetVchannels[1] = "by-dev-rootcoord-dml_9_100v9"
			},
		},
		{
			name: "a target the post-image gives no residue",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				p := splitTestPostImage()
				p.ShardInfos[1] = pbShard(schemapb.ShardState_ShardCreating)
				p.ShardInfos[2] = pbShard(schemapb.ShardState_ShardCreating, 0, 1)
				return p
			},
		},
		{
			name: "no vchannels at all",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				return &messagespb.AlterCollectionMessageUpdates{RoutingModulus: 2}
			},
		},
		{
			// The apply refuses a new vchannel in any state but Creating.
			name: "a target that is not Creating",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				p := splitTestPostImage()
				p.ShardInfos[1] = pbShard(schemapb.ShardState_ShardNormal, 0)
				return p
			},
		},
		{
			// A fenced source stays listed, Splitting, until adoption.
			name: "a source that is not Splitting",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				p := splitTestPostImage()
				p.ShardInfos[0] = &schemapb.CollectionShardInfo{State: schemapb.ShardState_ShardDropped}
				return p
			},
		},
		{
			// A shard split fences exactly one source.
			name: "a header naming no source",
			header: func(h *message.SplitShardMessageHeader) {
				h.SourceVchannel = ""
			},
		},
		{
			// And creates exactly two targets; one is a shrink or a partial share.
			name: "a header naming one target",
			header: func(h *message.SplitShardMessageHeader) {
				h.TargetVchannels = h.TargetVchannels[:1]
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
				splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
			}, 0)
			h := newSplitCallbackHarness(t, coll)

			postImage := splitTestPostImage()
			if tc.postImage != nil {
				postImage = tc.postImage()
			}
			err := h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(postImage, tc.header))
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Empty(t, h.calls)
		})
	}
}

// TestSplitShardAckCallbackAssertsNamespaceAdmissionBeforeCommitting (H4, G1):
// the callback re-runs the message-only checks SplitShardParam.Validate ran
// before the broadcast, as read-only assertions BEFORE CommitShardSplit, so a
// message that fails them records no task at datacoord and writes no meta. A
// namespace split that passes admission and granularity is still refused:
// namespace collections are not split yet (design §1.3).
func TestSplitShardAckCallbackAssertsNamespaceAdmissionBeforeCommitting(t *testing.T) {
	namespacePlaced := []*commonpb.KeyValuePair{
		{Key: common.NamespaceShardingEnabledKey, Value: "true"},
		{Key: common.NamespaceModeKey, Value: common.NamespaceModePartitionKey},
	}
	buckets := func(n int) func(*message.SplitShardMessageHeader) {
		return func(h *message.SplitShardMessageHeader) {
			h.PartitionIds = make([]int64, 0, n)
			for i := 0; i < n; i++ {
				h.PartitionIds = append(h.PartitionIds, int64(1000+i))
			}
		}
	}
	namespaced := func() *messagespb.AlterCollectionMessageUpdates {
		p := splitTestPostImage()
		p.ShardBy = routing.NamespaceShardBy
		return p
	}
	fresh := func() *model.Collection {
		coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
			splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
		}, 0)
		coll.Properties = namespacePlaced
		return coll
	}

	for _, tc := range []struct {
		name       string
		properties []*commonpb.KeyValuePair
		buckets    int
		contains   string
	}{
		{name: "rows placed by primary key", buckets: 16, contains: "placed by primary key"},
		{name: "a modulus that does not divide the buckets", properties: namespacePlaced, buckets: 3, contains: "must divide the 3 partition-key buckets"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSplitCallbackHarness(t, fresh())
			err := h.callback.splitShardV2AckCallback(context.Background(),
				splitTestResultWithGenesisProperties(namespaced(), tc.properties, buckets(tc.buckets)))
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.ErrorContains(t, err, tc.contains)
			require.Empty(t, h.calls, "CommitShardSplit is not called for a message that can never commit")
			require.Equal(t, 0, h.broadcasts)
		})
	}

	t.Run("a namespace split that passes admission is deferred", func(t *testing.T) {
		h := newSplitCallbackHarness(t, fresh())
		err := h.callback.splitShardV2AckCallback(context.Background(),
			splitTestResultWithGenesisSchema(namespaced(),
				&schemapb.CollectionSchema{Name: splitTestCollection, EnableNamespace: true, Properties: namespacePlaced}, buckets(16)))
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.False(t, merr.IsRetryableErr(err))
		require.ErrorContains(t, err, "namespace collections are not split")
		require.Empty(t, h.calls, "CommitShardSplit is not called")
		require.Equal(t, 0, h.broadcasts)
	})
}

// TestSplitShardAckCallbackRefusesANamespaceCollection (design §1.3): a
// SplitShard of a namespace collection is refused by the callback's re-run of
// ValidateSplitShardMessage, in either namespace.mode and under a hash(pk)
// post-image, before CommitShardSplit: no task is recorded and no meta written.
// The same collection without namespaces commits.
func TestSplitShardAckCallbackRefusesANamespaceCollection(t *testing.T) {
	for _, tc := range []struct {
		name       string
		properties []*commonpb.KeyValuePair
	}{
		{
			name: "partition_key mode",
			properties: []*commonpb.KeyValuePair{
				{Key: common.NamespaceShardingEnabledKey, Value: "true"},
				{Key: common.NamespaceModeKey, Value: common.NamespaceModePartitionKey},
			},
		},
		{
			name: "partition mode",
			properties: []*commonpb.KeyValuePair{
				{Key: common.NamespaceShardingEnabledKey, Value: "false"},
				{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fresh := func() *model.Collection {
				coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
					splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
				}, 0)
				coll.Properties = tc.properties
				return coll
			}

			h := newSplitCallbackHarness(t, fresh())
			err := h.callback.splitShardV2AckCallback(context.Background(), splitTestResultWithGenesisSchema(splitTestPostImage(),
				&schemapb.CollectionSchema{Name: splitTestCollection, EnableNamespace: true, Properties: tc.properties}))
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.False(t, merr.IsRetryableErr(err))
			require.ErrorContains(t, err, "namespace collections are not split")
			require.Empty(t, h.calls, "CommitShardSplit is not called")
			require.Equal(t, 0, h.broadcasts)

			h = newSplitCallbackHarness(t, fresh())
			h.expectCommit(merr.Success(), nil).Once()
			h.expectApply(nil).Once()
			require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(),
				splitTestResultWithGenesisProperties(splitTestPostImage(), tc.properties)))
			require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
		})
	}
}

// TestSplitShardAckCallbackRefusesGenesisPropertiesTheMetaDisagreesWith (F5):
// the genesis schema's copy of the collection properties is what admission read
// before the fence; the meta's copy is what the apply reads. A message whose copy
// disagrees is refused loudly before anything is committed, even when its own
// shard_by makes admission moot on both sides today.
func TestSplitShardAckCallbackRefusesGenesisPropertiesTheMetaDisagreesWith(t *testing.T) {
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	coll.Properties = []*commonpb.KeyValuePair{
		{Key: common.NamespaceShardingEnabledKey, Value: "true"},
		{Key: common.NamespaceModeKey, Value: common.NamespaceModePartitionKey},
	}
	h := newSplitCallbackHarness(t, coll)

	err := h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage()))
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.False(t, merr.IsRetryableErr(err))
	require.ErrorContains(t, err, "disagree with the collection meta")
	require.Empty(t, h.calls, "CommitShardSplit is not called")
	require.Equal(t, 0, h.broadcasts)
}

// TestSplitShardAckCallbackRefusesASourceResultWithoutTheFenceTick: the source's
// T_switch comes only from its extra append response. The append tick is never
// a fallback -- on a re-driven fence it is later than T_switch, and a drain gate
// waiting for it may never open -- so a result without a usable extra response
// is refused before anything is committed, as a retriable System error.
func TestSplitShardAckCallbackRefusesASourceResultWithoutTheFenceTick(t *testing.T) {
	anotherExtra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: []int64{1}})
	require.NoError(t, err)
	for _, tc := range []struct {
		name  string
		extra *anypb.Any
	}{
		{name: "no extra response", extra: nil},
		{name: "another message type's extra response", extra: anotherExtra},
		{name: "a zero fence tick", extra: splitTestSwitchExtra(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
				splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
			}, 0)
			h := newSplitCallbackHarness(t, coll)

			result := splitTestResult(splitTestPostImage())
			result.Results[splitTestSource].Extra = tc.extra
			err := h.callback.splitShardV2AckCallback(context.Background(), result)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			require.True(t, merr.IsRetryableErr(err))
			require.Empty(t, h.calls)
		})
	}
}

// TestSplitShardAckCallbackRefusesAResultMissingAReplica: the broadcaster acks
// only once every vchannel has been appended, so a missing entry is an internal
// bug -- reported rather than silently committing a source with no fence tick or
// a target with no genesis position.
func TestSplitShardAckCallbackRefusesAResultMissingAReplica(t *testing.T) {
	for _, vchannel := range []string{splitTestSource, splitTestTarget2} {
		t.Run("missing "+vchannel, func(t *testing.T) {
			coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
				splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
			}, 0)
			h := newSplitCallbackHarness(t, coll)

			result := splitTestResult(splitTestPostImage())
			delete(result.Results, vchannel)
			err := h.callback.splitShardV2AckCallback(context.Background(), result)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.ErrorContains(t, err, vchannel)
			require.Empty(t, h.calls)
		})
	}
}

// TestSplitShardAckCallbackReportsALateBroadcastFailure: everything after the
// meta apply is a plain retry, and the error carries through so the broadcaster
// runs the callback again.
func TestSplitShardAckCallbackReportsALateBroadcastFailure(t *testing.T) {
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	h := newSplitCallbackHarness(t, coll)
	h.expectCommit(merr.Success(), nil).Once()
	h.expectApply(nil).Once()
	h.core.broker = &mockBroker{
		BroadcastAlteredCollectionFunc: func(ctx context.Context, collectionID int64) error {
			return errors.New("datacoord unreachable")
		},
	}

	require.Error(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
}

// TestSplitShardAckCallbackReportsACacheExpirationFailure: the caches are the
// last step, and a failure there is retried like any other -- the proxies would
// otherwise keep routing by the pre-split topology.
func TestSplitShardAckCallbackReportsACacheExpirationFailure(t *testing.T) {
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	h := newSplitCallbackHarness(t, coll)
	h.expectCommit(merr.Success(), nil).Once()
	h.expectApply(nil).Once()
	cacheMocker := mockey.Mock((*Core).getCacheExpireForCollection).Return(nil, errors.New("alias listing failed")).Build()
	defer cacheMocker.UnPatch()

	require.Error(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
	require.Equal(t, 1, h.broadcasts)
}

// TestSplitShardAckCallbackStopsOnADroppingCollection: a collection that entered
// Dropping still resolves by id but no longer by name, so the cache-expiry
// lookup reports it missing. Its caches are about to be invalidated by the drop
// itself, so there is nothing left to expire -- and retrying would spin against
// a held resource key until the drop finishes.
func TestSplitShardAckCallbackStopsOnADroppingCollection(t *testing.T) {
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	h := newSplitCallbackHarness(t, coll)
	h.expectCommit(merr.Success(), nil).Once()
	h.expectApply(nil).Once()
	cacheMocker := mockey.Mock((*Core).getCacheExpireForCollection).
		Return(nil, merr.WrapErrCollectionNotFound(splitTestCollection)).Build()
	defer cacheMocker.UnPatch()

	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
	require.Equal(t, 1, h.broadcasts)
}

// TestSplitShardAckCallbackReportsAMetaFailure: an apply that fails for anything
// other than a vanished collection is retried, not swallowed.
func TestSplitShardAckCallbackReportsAMetaFailure(t *testing.T) {
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	h := newSplitCallbackHarness(t, coll)
	h.expectCommit(merr.Success(), nil).Once()
	h.expectApply(errors.New("etcd down")).Once()

	require.Error(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
	require.Equal(t, 0, h.broadcasts)
}

// TestSplitShardAckCallbackRefusesAResultWithoutTheControlChannel: the routing
// commit is stamped with the control-channel replica's tick, and the result is
// searched for that replica by name. A broadcast issued around
// SplitShardParam.Validate with a plain vchannel in the control-channel role
// acks with no such entry; dereferencing it would panic mixcoord on every
// retry and after every restart, since the task is persisted. The callback
// refuses it as a retriable System error instead, before either commit half.
func TestSplitShardAckCallbackRefusesAResultWithoutTheControlChannel(t *testing.T) {
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	h := newSplitCallbackHarness(t, coll)

	result := splitTestResult(splitTestPostImage())
	delete(result.Results, splitTestControl)
	require.Nil(t, result.GetControlChannelResult())

	var err error
	require.NotPanics(t, func() {
		err = h.callback.splitShardV2AckCallback(context.Background(), result)
	})
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.True(t, merr.IsRetryableErr(err))
	require.Empty(t, h.calls, "nothing is committed for a result that cannot be stamped")
	require.Zero(t, h.broadcasts)
}
