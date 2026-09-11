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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	imocks "github.com/milvus-io/milvus/internal/mocks"
	mockrootcoord "github.com/milvus-io/milvus/internal/rootcoord/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

// splitTestResult builds the broadcast result the ack callback receives: one
// append result per replica, the sources' ticks strictly below the targets'.
// The optional mutators corrupt the HEADER's copy of the residues and the
// modulus, which is how a header-vs-body disagreement is staged.
func splitTestResult(postImage *messagespb.AlterCollectionMessageUpdates, headerOpts ...func(*message.SplitShardMessageHeader)) message.BroadcastResultSplitShardMessageV2 {
	header := &message.SplitShardMessageHeader{
		CollectionId:    splitTestCollID,
		SplitTaskId:     7,
		DbId:            1,
		RoutingModulus:  2,
		SourceVchannels: []string{splitTestSource},
		PartitionIds:    []int64{10},
		Targets: []*message.SplitShardTarget{
			{Vchannel: splitTestTarget1, Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
			{Vchannel: splitTestTarget2, Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
		},
	}
	for _, opt := range headerOpts {
		if opt != nil {
			opt(header)
		}
	}
	raw := message.NewSplitShardMessageBuilderV2().
		WithHeader(header).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: splitTestCollection}},
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
			splitTestSource:  {MessageID: rmq.NewRmqID(1), TimeTick: 100},
			splitTestTarget1: {MessageID: rmq.NewRmqID(2), TimeTick: 101},
			splitTestTarget2: {MessageID: rmq.NewRmqID(3), TimeTick: 102},
			splitTestControl: {MessageID: rmq.NewRmqID(4), TimeTick: 103},
		},
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
	return h.meta.EXPECT().ApplyShardSplitRouting(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(context.Context, int64, *messagespb.AlterCollectionMessageUpdates, uint64) {
			h.record("ApplyShardSplitRouting")
		}).Return(err)
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
	coll := splitTestCollectionMeta([]string{splitTestSource}, map[string]*model.ShardInfo{
		splitTestSource: {VChannelName: splitTestSource, State: schemapb.ShardState_ShardNormal},
	}, 0)
	h := newSplitCallbackHarness(t, coll)
	h.expectCommit(merr.Success(), nil).Once()

	var gotCollectionID int64
	var gotUpdates *messagespb.AlterCollectionMessageUpdates
	var gotTimetick uint64
	h.meta.EXPECT().ApplyShardSplitRouting(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Run(func(_ context.Context, collectionID int64, updates *messagespb.AlterCollectionMessageUpdates, timetick uint64) {
			h.record("ApplyShardSplitRouting")
			gotCollectionID, gotUpdates, gotTimetick = collectionID, updates, timetick
		}).Return(nil).Once()

	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))

	require.Equal(t, splitTestCollID, gotCollectionID)
	require.Equal(t, []string{splitTestSource, splitTestTarget1, splitTestTarget2}, gotUpdates.GetVirtualChannelNames())
	// The control channel's tick orders the commit, not the max over replicas.
	require.EqualValues(t, 103, gotTimetick)
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
	require.EqualValues(t, 100, got.GetSources()[0].GetSwitchTimeTick())
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
	h.expectApply(errShardSplitRoutingAlreadyApplied).Once()

	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
	require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
	require.Equal(t, 1, h.broadcasts)
}

// TestSplitShardAckCallbackSkipsASupersededPostImage is the failure this
// callback must never have. The broadcaster retries an ack callback until it
// returns nil, holding the collection's resource keys while it does; a
// redelivery that arrives after the adoption commit finished the split is
// therefore not a bug to report but a job already done. Reporting it would
// queue every later DDL of the collection -- DropCollection included -- behind a
// refusal that can never clear.
func TestSplitShardAckCallbackSkipsASupersededPostImage(t *testing.T) {
	adopted := splitTestCollectionMeta(
		[]string{splitTestTarget1, splitTestTarget2},
		map[string]*model.ShardInfo{
			splitTestTarget1: {VChannelName: splitTestTarget1, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			splitTestTarget2: {VChannelName: splitTestTarget2, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1}},
		}, 2)
	adopted.ShardBy = "hash(pk)"
	h := newSplitCallbackHarness(t, adopted)
	h.expectCommit(merr.Success(), nil).Once()
	h.expectApply(errShardSplitRoutingSuperseded).Once()

	require.NoError(t, h.callback.splitShardV2AckCallback(context.Background(), splitTestResult(splitTestPostImage())))
	require.Equal(t, []string{"CommitShardSplit", "ApplyShardSplitRouting"}, h.calls)
	// The caches are still expired: the callback ends here, so nothing else will.
	require.Equal(t, 1, h.broadcasts)
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
			name: "header and post-image disagree on the modulus",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				p := splitTestPostImage()
				// Still a valid tiling on its own -- 4 residues over 4 shards is
				// not what this post-image has, so make it one the tiling check
				// accepts while the header still says 2.
				p.RoutingModulus = 2
				p.ShardInfos[1] = pbShard(schemapb.ShardState_ShardCreating, 0)
				p.ShardInfos[2] = pbShard(schemapb.ShardState_ShardCreating, 1)
				return p
			},
			header: func(h *message.SplitShardMessageHeader) { h.RoutingModulus = 4 },
		},
		{
			name: "header and post-image disagree on a target's residues",
			header: func(h *message.SplitShardMessageHeader) {
				h.Targets[1].Routing = &schemapb.HashRouting{Buckets: []uint64{0}}
			},
		},
		{
			name: "a header target claims more residues than the post-image gives it",
			header: func(h *message.SplitShardMessageHeader) {
				h.Targets[1].Routing = &schemapb.HashRouting{Buckets: []uint64{0, 1}}
			},
		},
		{
			name: "a header target the post-image does not name",
			header: func(h *message.SplitShardMessageHeader) {
				h.Targets[1].Vchannel = "by-dev-rootcoord-dml_9_100v9"
			},
		},
		{
			name: "no vchannels at all",
			postImage: func() *messagespb.AlterCollectionMessageUpdates {
				return &messagespb.AlterCollectionMessageUpdates{RoutingModulus: 2}
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

// TestRoutingUpdatesFromRequest pins the shape the RPC path converts into, so
// the two callers of the shared checks read the same five fields.
func TestRoutingUpdatesFromRequest(t *testing.T) {
	updates := routingUpdatesFromRequest(&rootcoordpb.CommitShardSplitRoutingRequest{
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0", "p1"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			pbShard(schemapb.ShardState_ShardNormal, 0),
			pbShard(schemapb.ShardState_ShardNormal, 1),
		},
		RoutingModulus: 2,
		ShardBy:        "hash(pk)",
	})
	require.Equal(t, []string{"v0", "v1"}, updates.GetVirtualChannelNames())
	require.Equal(t, []string{"p0", "p1"}, updates.GetPhysicalChannelNames())
	require.Len(t, updates.GetShardInfos(), 2)
	require.EqualValues(t, 2, updates.GetRoutingModulus())
	require.Equal(t, "hash(pk)", updates.GetShardBy())
}
