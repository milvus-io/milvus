package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitReplicateConfig is a two-pchannel primary replicating into a
// two-pchannel secondary: p0 -> q0 and p1 -> q1, by index position. Every name
// assertion below is against that mapping.
func splitReplicateConfig() *replicateutil.ConfigHelper {
	return replicateutil.MustNewConfigHelper(
		"by-dev",
		&commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"p0", "p1"}},
				{ClusterId: "by-dev", Pchannels: []string{"q0", "q1"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "by-dev"},
			},
		},
	)
}

// splitShardBroadcastOnPrimary builds the SplitShard broadcast exactly as the
// primary's DataCoord does: source p0_1v0 fenced, one target taking the fenced
// source's own pchannel and one a fresh pchannel, the routing post-image naming
// all three, and the source named append-first.
func splitShardBroadcastOnPrimary() message.BroadcastMutableMessage {
	return buildSplitShardOnPrimary(splitShardParamOnPrimary()).WithBroadcastID(700)
}

// buildSplitShardOnPrimary builds param's broadcast the way the primary issues
// it: with the primary's dataCoord.shardSplit.enable on, for the build only.
// The switch gates issuing a split; the secondary these tests play receives the
// replica with its own switch at the default, off.
func buildSplitShardOnPrimary(param SplitShardParam) message.BroadcastMutableMessage {
	key := paramtable.Get().DataCoordCfg.ShardSplitEnable.Key
	paramtable.Get().Save(key, "true")
	defer paramtable.Get().Reset(key)
	msg, err := NewSplitShardBroadcastMessage(param)
	if err != nil {
		panic(err)
	}
	return msg
}

// splitShardParamOnPrimary is the parameter splitShardBroadcastOnPrimary builds
// its broadcast from, for tests that need another broadcast id or control
// channel.
func splitShardParamOnPrimary() SplitShardParam {
	return SplitShardParam{
		CollectionID:    1,
		SplitTaskID:     100,
		SourceVChannel:  "p0_1v0",
		TargetVChannels: []string{"p0_1v1", "p1_1v2"},
		Schema:          &schemapb.CollectionSchema{Name: "col"},
		PartitionIDs:    []int64{10, 11},
		Routing: &message.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{"p0_1v0", "p0_1v1", "p1_1v2"},
			PhysicalChannelNames: []string{"p0", "p0", "p1"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{VchannelName: "p0_1v0", State: schemapb.ShardState_ShardSplitting},
				{
					VchannelName: "p0_1v1",
					State:        schemapb.ShardState_ShardCreating,
					Routing: &schemapb.CollectionShardInfo_HashRouting{
						HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
					},
				},
				{
					VchannelName: "p1_1v2",
					State:        schemapb.ShardState_ShardCreating,
					Routing: &schemapb.CollectionShardInfo_HashRouting{
						HashRouting: &schemapb.HashRouting{Buckets: []uint64{1}},
					},
				},
			},
			RoutingModulus: 2,
		},
		ControlChannel: "p0_vcchan",
	}
}

// replicaOf turns one vchannel's replica of a broadcast into the replicate
// message a secondary's proxy receives.
func replicaOf(msg message.BroadcastMutableMessage, vchannel string) message.ReplicateMutableMessage {
	return replicaAt(msg, vchannel, 1)
}

// replicaAt is replicaOf with the primary's time tick for the replica, which is
// the replicate header's time tick the secondary deduplicates by.
func replicaAt(msg message.BroadcastMutableMessage, vchannel string, timeTick uint64) message.ReplicateMutableMessage {
	for _, replica := range msg.SplitIntoMutableMessage() {
		if replica.VChannel() != vchannel {
			continue
		}
		immutable := replica.WithLastConfirmedUseMessageID().WithTimeTick(timeTick).
			IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timeTick)))
		return message.MustNewReplicateMessage("primary", immutable.IntoImmutableMessageProto())
	}
	panic("vchannel is not one of the broadcast's own")
}

// newSplitReplicateService builds a secondary-cluster replicate service whose
// producer records every message it appends.
func newSplitReplicateService(t *testing.T, appended chan<- message.MutableMessage) (*replicateService, *mock_client.MockBroadcastService) {
	c := mock_client.NewMockClient(t)
	as := mock_client.NewMockAssignmentService(t)
	c.EXPECT().Assignment().Return(as).Maybe()
	as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(splitReplicateConfig(), nil).Maybe()

	bs := mock_client.NewMockBroadcastService(t)
	c.EXPECT().Broadcast().Return(bs).Maybe()

	h := mock_handler.NewMockHandlerClient(t)
	// Nothing replicated here yet: every gated replica is a new one.
	h.EXPECT().GetReplicateCheckpoint(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, pchannel string) (*wal.ReplicateCheckpoint, error) {
			return &wal.ReplicateCheckpoint{ClusterID: "primary", PChannel: pchannel}, nil
		}).Maybe()
	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
			if appended != nil {
				appended <- msg
			}
			return &types.AppendResult{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 1}, nil
		}).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	h.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil).Maybe()

	return &replicateService{
		walAccesserImpl: &walAccesserImpl{
			lifetime:             typeutil.NewLifetime(),
			clusterID:            "by-dev",
			streamingCoordClient: c,
			handlerClient:        h,
			producers:            make(map[string]*producer.ResumableProducer),
		},
	}, bs
}

// TestOverwriteReplicateMessageRefusesAMalformedAppendFirstOnce: a broadcast
// header whose append-first vchannel is not one of its own broadcast vchannels
// was malformed by the primary. The replicate service refuses it once, as a
// ReplicateViolation naming the vchannel -- not as an input error re-formatted
// into a violation -- and fails only that message.
func TestOverwriteReplicateMessageRefusesAMalformedAppendFirstOnce(t *testing.T) {
	rs, _ := newSplitReplicateService(t, nil)

	var forged *commonpb.ImmutableMessage
	for _, replica := range splitShardBroadcastOnPrimary().SplitIntoMutableMessage() {
		if replica.VChannel() != "p0_1v0" {
			continue
		}
		forged = replica.WithLastConfirmedUseMessageID().WithTimeTick(1).
			IntoImmutableMessage(walimplstest.NewTestMessageID(1)).IntoImmutableMessageProto()
	}
	require.NotNil(t, forged)
	bh := &messagespb.BroadcastHeader{}
	require.NoError(t, message.DecodeProto(forged.GetProperties()["_bh"], bh))
	bh.AppendFirstVchannels = []string{"p9_1v9"}
	encoded, err := message.EncodeProto(bh)
	require.NoError(t, err)
	forged.Properties["_bh"] = encoded

	replicate := message.MustNewReplicateMessage("primary", forged)
	msg, err := rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
	require.Nil(t, msg)
	require.Error(t, err)
	streamingErr := status.AsStreamingError(err)
	require.True(t, streamingErr.IsReplicateViolation())
	assert.Contains(t, err.Error(), "p9_1v9")
	assert.NotContains(t, err.Error(), "invalid parameter", "classified once, not an input error formatted into a violation")
}

// TestOverwriteSplitShardMessageRemapsEveryName asserts that a replicated
// SplitShard reaches this cluster's WAL with EVERY channel name in this
// cluster's namespace -- header, body and broadcast header alike -- and with
// everything that is not a channel name left exactly as the primary wrote it.
//
// A single name left behind is not a cosmetic defect: the shard interceptor
// resolves the replica's role by matching msg.VChannel() against the header, so
// a stale source or target name silently turns a fence into a misroute, and a
// stale routing post-image makes the collection's routing table unreadable here.
func TestOverwriteSplitShardMessageRemapsEveryName(t *testing.T) {
	// This cluster is the secondary, and its dataCoord.shardSplit.enable is off:
	// the switch gates issuing a split, never carrying a replicated one.
	key := paramtable.Get().DataCoordCfg.ShardSplitEnable.Key
	paramtable.Get().Save(key, "false")
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	rs, _ := newSplitReplicateService(t, nil)

	replicate := replicaOf(splitShardBroadcastOnPrimary(), "p0_1v0")
	rh := replicate.ReplicateHeader()
	require.NotNil(t, rh)

	msg, err := rs.overwriteReplicateMessage(context.Background(), replicate, rh)
	require.NoError(t, err)

	assert.Equal(t, "q0_1v0", msg.VChannel())

	bh := msg.BroadcastHeader()
	require.NotNil(t, bh)
	assert.ElementsMatch(t, []string{"q0_1v0", "q0_1v1", "q1_1v2", "q0_vcchan"}, bh.VChannels)
	// The append-first list is what the gate below waits on; a name left in the
	// primary's namespace would make it wait on a vchannel that does not exist
	// in this cluster, forever.
	assert.Equal(t, []string{"q0_1v0"}, bh.AppendFirstVChannels)

	splitShardMsg := message.MustAsMutableSplitShardMessageV2(msg)
	header := splitShardMsg.Header()
	assert.Equal(t, "q0_1v0", header.GetSourceVchannel())
	assert.Equal(t, []string{"q0_1v1", "q1_1v2"}, header.GetTargetVchannels())

	// The facts that are the same in both clusters stay put.
	assert.EqualValues(t, 1, header.GetCollectionId())
	assert.EqualValues(t, 100, header.GetSplitTaskId())
	assert.Equal(t, []int64{10, 11}, header.GetPartitionIds())

	body := splitShardMsg.MustBody()
	routing := body.GetRouting()
	assert.Equal(t, []string{"q0_1v0", "q0_1v1", "q1_1v2"}, routing.GetVirtualChannelNames())
	assert.Equal(t, []string{"q0", "q0", "q1"}, routing.GetPhysicalChannelNames())
	assert.Equal(t, []string{"q0_1v0", "q0_1v1", "q1_1v2"},
		shardInfoNames(routing.GetShardInfos()),
		"a shard info naming a primary vchannel makes routing.NewTable refuse the whole table")
	// The residues and the modulus live only in the post-image, and are not
	// names: they stay exactly as the primary wrote them.
	assert.EqualValues(t, 2, routing.GetRoutingModulus())
	assert.Equal(t, []uint64{0}, routing.GetShardInfos()[1].GetHashRouting().GetBuckets())
	assert.Equal(t, []uint64{1}, routing.GetShardInfos()[2].GetHashRouting().GetBuckets())
	assert.Equal(t, "col", body.GetGenesis().GetCollectionSchema().GetName())
}

// TestOverwriteSplitShardMessageRemapsTheGenesisChannelNames covers the genesis
// CreateCollectionRequest's own channel lists. The split's genesis carries only
// a schema today, but the field is part of the message and a name left in it
// would create the target's collection meta pointing at the primary's channels.
func TestOverwriteSplitShardMessageRemapsTheGenesisChannelNames(t *testing.T) {
	rs, _ := newSplitReplicateService(t, nil)

	msg := splitShardBroadcastOnPrimary()
	replicate := replicaOf(msg, "p0_1v1")
	// Fill in the genesis channel lists the way a CreateCollection carries them.
	splitShardMsg := message.MustAsMutableSplitShardMessageV2(replicate)
	body := splitShardMsg.MustBody()
	body.Genesis = &msgpb.CreateCollectionRequest{
		CollectionSchema:     &schemapb.CollectionSchema{Name: "col"},
		VirtualChannelNames:  []string{"p0_1v1", "p1_1v2"},
		PhysicalChannelNames: []string{"p0", "p1"},
	}
	splitShardMsg.OverwriteBody(body)

	out, err := rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
	require.NoError(t, err)

	genesis := message.MustAsMutableSplitShardMessageV2(out).MustBody().GetGenesis()
	assert.Equal(t, []string{"q0_1v1", "q1_1v2"}, genesis.GetVirtualChannelNames())
	assert.Equal(t, []string{"q0", "q1"}, genesis.GetPhysicalChannelNames())
}

// TestOverwriteRoutingAlterCollectionRemapsChannelNames asserts the split's
// adoption message -- an AlterCollection under the shard_split_routing mask --
// gets the same treatment, and that an AlterCollection without that mask is left
// untouched (nothing else carries channel names in its updates).
func TestOverwriteRoutingAlterCollectionRemapsChannelNames(t *testing.T) {
	rs, _ := newSplitReplicateService(t, nil)

	routingUpdates := func() *message.AlterCollectionMessageUpdates {
		return &message.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{"p0_1v0", "p1_1v1"},
			PhysicalChannelNames: []string{"p0", "p1"},
			ShardInfos: []*schemapb.CollectionShardInfo{
				{VchannelName: "p0_1v0"},
				// An empty name is the persisted shape of a pre-field record and
				// must survive as empty rather than being invented.
				{},
			},
			SplitTaskId: 7,
		}
	}

	build := func(paths []string) message.ReplicateMutableMessage {
		msg := message.NewAlterCollectionMessageBuilderV2().
			WithHeader(&message.AlterCollectionMessageHeader{
				CollectionId: 1,
				UpdateMask:   &fieldmaskpb.FieldMask{Paths: paths},
			}).
			WithBody(&message.AlterCollectionMessageBody{Updates: routingUpdates()}).
			WithBroadcast([]string{"p0_1v0", "p1_1v1"}).
			MustBuildBroadcast().
			WithBroadcastID(701)
		return replicaOf(msg, "p0_1v0")
	}

	// (a) under the routing mask: every name is remapped.
	replicate := build([]string{message.FieldMaskCollectionShardSplitRouting})
	out, err := rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
	require.NoError(t, err)
	updates := message.MustAsMutableAlterCollectionMessageV2(out).MustBody().GetUpdates()
	assert.Equal(t, []string{"q0_1v0", "q1_1v1"}, updates.GetVirtualChannelNames())
	assert.Equal(t, []string{"q0", "q1"}, updates.GetPhysicalChannelNames())
	assert.Equal(t, []string{"q0_1v0", ""}, shardInfoNames(updates.GetShardInfos()))
	// The split task id is an id, not a name: nothing to remap, and the in-place
	// rewrite above must not drop it -- the secondary's adoption gate asks its
	// own datacoord about exactly this task.
	assert.EqualValues(t, 7, updates.GetSplitTaskId())

	// (a2) a routing commit naming a channel the topology cannot map is refused
	// rather than half-remapped: a topology naming channels of both clusters is
	// worse than no commit at all.
	replicate = build([]string{message.FieldMaskCollectionShardSplitRouting})
	unmappable := message.MustAsMutableAlterCollectionMessageV2(replicate)
	unmappableBody := unmappable.MustBody()
	unmappableBody.Updates.VirtualChannelNames = []string{"nosuch_1v0"}
	unmappable.OverwriteBody(unmappableBody)
	_, err = rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
	assert.Error(t, err)

	// (b) any other mask: the updates are left alone.
	replicate = build([]string{"collection_name"})
	out, err = rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
	require.NoError(t, err)
	updates = message.MustAsMutableAlterCollectionMessageV2(out).MustBody().GetUpdates()
	assert.Equal(t, []string{"p0_1v0", "p1_1v1"}, updates.GetVirtualChannelNames())
	assert.Equal(t, []string{"p0", "p1"}, updates.GetPhysicalChannelNames())
}

// TestOverwriteSplitShardMessageRejectsAnUnmappableChannel asserts that a
// channel name the topology cannot map is reported rather than carried through:
// a half-remapped SplitShard would fence one cluster's shard while creating
// another's.
func TestOverwriteSplitShardMessageRejectsAnUnmappableChannel(t *testing.T) {
	rs, _ := newSplitReplicateService(t, nil)

	msg := splitShardBroadcastOnPrimary()
	replicate := replicaOf(msg, "p0_1v0")
	splitShardMsg := message.MustAsMutableSplitShardMessageV2(replicate)
	header := splitShardMsg.Header()
	header.TargetVchannels[0] = "nosuch_1v1"
	splitShardMsg.OverwriteHeader(header)

	_, err := rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
	assert.Error(t, err)
}

// TestOverwriteSplitShardMessageRejectsAnUnmappableName walks the remap's other
// entry points with a name the topology cannot map. Each one must refuse rather
// than carry the name through: a half-remapped SplitShard would fence one
// cluster's shard while creating another's, and a half-remapped routing
// post-image would commit a topology that names channels from both.
func TestOverwriteSplitShardMessageRejectsAnUnmappableName(t *testing.T) {
	rs, _ := newSplitReplicateService(t, nil)

	mutate := func(f func(*message.SplitShardMessageHeader, *message.SplitShardMessageBody)) error {
		replicate := replicaOf(splitShardBroadcastOnPrimary(), "p0_1v0")
		splitShardMsg := message.MustAsMutableSplitShardMessageV2(replicate)
		header := splitShardMsg.Header()
		body := splitShardMsg.MustBody()
		f(header, body)
		splitShardMsg.OverwriteHeader(header)
		splitShardMsg.OverwriteBody(body)
		_, err := rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
		return err
	}

	assert.Error(t, mutate(func(h *message.SplitShardMessageHeader, _ *message.SplitShardMessageBody) {
		h.SourceVchannel = "nosuch_1v0"
	}), "an unmappable source vchannel")
	assert.Error(t, mutate(func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
		b.Routing.VirtualChannelNames = []string{"nosuch_1v0"}
	}), "an unmappable routing vchannel")
	assert.Error(t, mutate(func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
		b.Routing.PhysicalChannelNames = []string{"nosuch"}
	}), "an unmappable routing pchannel")
	assert.Error(t, mutate(func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
		b.Routing.ShardInfos = []*schemapb.CollectionShardInfo{{VchannelName: "nosuch_1v0"}}
	}), "an unmappable shard info vchannel")
	assert.Error(t, mutate(func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
		b.Genesis = &msgpb.CreateCollectionRequest{VirtualChannelNames: []string{"nosuch_1v0"}}
	}), "an unmappable genesis vchannel")
	assert.Error(t, mutate(func(_ *message.SplitShardMessageHeader, b *message.SplitShardMessageBody) {
		b.Genesis = &msgpb.CreateCollectionRequest{PhysicalChannelNames: []string{"nosuch"}}
	}), "an unmappable genesis pchannel")
}

// TestReplicateAppendWaitsForTheAppendFirstReplicas is the append gate itself:
// a replica named append-first is appended without asking anyone, while every
// other replica of the same broadcast is held until the coord reports the
// append-first ones landed here.
//
// Without it the two are independent per-pchannel streams and the target's
// genesis can be appended before the source's fence -- inverting the split's
// only ordering invariant on the secondary.
func TestReplicateAppendWaitsForTheAppendFirstReplicas(t *testing.T) {
	appended := make(chan message.MutableMessage, 4)
	rs, bs := newSplitReplicateService(t, appended)

	release := make(chan struct{})
	var gotBroadcastID uint64
	var gotVChannels []string
	bs.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, broadcastID uint64, vchannels []string) error {
			gotBroadcastID = broadcastID
			gotVChannels = vchannels
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		}).Maybe()

	msg := splitShardBroadcastOnPrimary()

	// The source replica is append-first: it never waits.
	_, err := rs.Append(context.Background(), replicaOf(msg, "p0_1v0"))
	require.NoError(t, err)
	select {
	case landed := <-appended:
		assert.Equal(t, "q0_1v0", landed.VChannel())
	case <-time.After(time.Second):
		t.Fatal("an append-first replica must not wait for anything")
	}

	// A target replica waits, and its append does not happen while it does.
	done := make(chan error, 1)
	go func() {
		_, err := rs.Append(context.Background(), replicaOf(msg, "p1_1v2"))
		done <- err
	}()
	select {
	case landed := <-appended:
		t.Fatalf("a gated replica was appended before the append-first ones landed: %s", landed.VChannel())
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-done)
	select {
	case landed := <-appended:
		assert.Equal(t, "q1_1v2", landed.VChannel())
	case <-time.After(time.Second):
		t.Fatal("the gated replica was never appended after the gate opened")
	}

	// The gate asks about THIS cluster's names, under the broadcast's own id.
	assert.EqualValues(t, 700, gotBroadcastID)
	assert.Equal(t, []string{"q0_1v0"}, gotVChannels)
}

// TestReplicateAppendReturnsTheGateErrorAsItStands asserts a failed gate stops
// the append and surfaces the error unchanged. Every failure here is transient
// -- the replicate stream retries from its checkpoint -- so rewriting it into
// anything terminal would strand the secondary.
func TestReplicateAppendReturnsTheGateErrorAsItStands(t *testing.T) {
	appended := make(chan message.MutableMessage, 4)
	rs, bs := newSplitReplicateService(t, appended)

	bs.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything, mock.Anything).
		Return(context.Canceled).Maybe()

	_, err := rs.Append(context.Background(), replicaOf(splitShardBroadcastOnPrimary(), "p1_1v2"))
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, appended, "a replica whose gate failed must not be appended")
}

// TestReplicateAppendDoesNotGateABroadcastWithoutAppendFirst asserts an ordinary
// broadcast -- one whose header names no append-first vchannel -- never reaches
// the coord at all: the gate is the split's cost, not everyone's.
func TestReplicateAppendDoesNotGateABroadcastWithoutAppendFirst(t *testing.T) {
	appended := make(chan message.MutableMessage, 4)
	rs, _ := newSplitReplicateService(t, appended)

	msg := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithBroadcast([]string{"p0_1v0", "p1_1v1"}).
		MustBuildBroadcast().
		WithBroadcastID(702)

	_, err := rs.Append(context.Background(), replicaOf(msg, "p1_1v1"))
	require.NoError(t, err)
	landed := <-appended
	assert.Equal(t, "q1_1v1", landed.VChannel())
}

func shardInfoNames(infos []*schemapb.CollectionShardInfo) []string {
	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.GetVchannelName())
	}
	return names
}

// forgedBroadcastHeaderReplica takes the p0_1v0 replica of the primary's
// SplitShard broadcast and rewrites its broadcast header the way a
// malformed-by-the-primary header would arrive.
func forgedBroadcastHeaderReplica(t *testing.T, mutate func(bh *messagespb.BroadcastHeader)) message.ReplicateMutableMessage {
	t.Helper()
	var forged *commonpb.ImmutableMessage
	for _, replica := range splitShardBroadcastOnPrimary().SplitIntoMutableMessage() {
		if replica.VChannel() != "p0_1v0" {
			continue
		}
		forged = replica.WithLastConfirmedUseMessageID().WithTimeTick(1).
			IntoImmutableMessage(walimplstest.NewTestMessageID(1)).IntoImmutableMessageProto()
	}
	require.NotNil(t, forged)
	bh := &messagespb.BroadcastHeader{}
	require.NoError(t, message.DecodeProto(forged.GetProperties()["_bh"], bh))
	mutate(bh)
	encoded, err := message.EncodeProto(bh)
	require.NoError(t, err)
	forged.Properties["_bh"] = encoded
	return message.MustNewReplicateMessage("primary", forged)
}

// TestOverwriteReplicateMessageRefusesTheBuilderInvariantsOnce: the header
// shapes the builder forbids by panicking -- ack-sync-up combined with an
// append-first vchannel, more than one append-first vchannel, and the control
// channel named as append-first -- are refused on the secondary as replicate
// violations, once per delivery, and never reach the broadcaster, where a
// force-promote re-drive asserts the same invariants with panics (AckPartial,
// splitAppendFirst).
func TestOverwriteReplicateMessageRefusesTheBuilderInvariantsOnce(t *testing.T) {
	rs, _ := newSplitReplicateService(t, nil)

	t.Run("ack sync up with append-first", func(t *testing.T) {
		replicate := forgedBroadcastHeaderReplica(t, func(bh *messagespb.BroadcastHeader) {
			bh.AckSyncUp = true
		})
		var msg message.MutableMessage
		var err error
		require.NotPanics(t, func() {
			msg, err = rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
		})
		require.Nil(t, msg)
		require.Error(t, err)
		assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
		assert.Contains(t, err.Error(), "ack sync up")
	})

	t.Run("more than one append-first vchannel", func(t *testing.T) {
		replicate := forgedBroadcastHeaderReplica(t, func(bh *messagespb.BroadcastHeader) {
			// Both are broadcast vchannels, so the subset check alone passes.
			bh.AppendFirstVchannels = []string{"p0_1v0", "p0_1v1"}
		})
		var msg message.MutableMessage
		var err error
		require.NotPanics(t, func() {
			msg, err = rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
		})
		require.Nil(t, msg)
		require.Error(t, err)
		assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
		assert.Contains(t, err.Error(), "at most one append-first vchannel")
	})

	t.Run("control channel append-first", func(t *testing.T) {
		replicate := forgedBroadcastHeaderReplica(t, func(bh *messagespb.BroadcastHeader) {
			// The control channel is a broadcast vchannel, so the subset check
			// alone passes; only the control-channel check refuses it.
			require.Contains(t, bh.Vchannels, "p0_vcchan")
			bh.AppendFirstVchannels = []string{"p0_vcchan"}
		})
		var msg message.MutableMessage
		var err error
		require.NotPanics(t, func() {
			msg, err = rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
		})
		require.Nil(t, msg)
		require.Error(t, err)
		assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
		assert.Contains(t, err.Error(), "control channel p0_vcchan cannot be appended first")
	})
}

// TestOverwriteSplitShardMessageClearsThePrimarysFlushedSegments: the source
// replica's flushed_segment_ids are the segments the PRIMARY's StreamingNode
// sealed at ITS fence. This cluster's first fence overwrites them with its own,
// but a same-task re-fence appends the header as received, so a replicated
// re-fence record would otherwise name segments that exist only in the
// primary. The remap clears them; nothing here reads them off the record.
func TestOverwriteSplitShardMessageClearsThePrimarysFlushedSegments(t *testing.T) {
	rs, _ := newSplitReplicateService(t, nil)

	// The primary's source handler embeds the ids AFTER the broadcast is
	// built, so stamp them onto the replica the way its WAL record carries them.
	var sourceReplica message.MutableMessage
	for _, replica := range splitShardBroadcastOnPrimary().SplitIntoMutableMessage() {
		if replica.VChannel() == "p0_1v0" {
			sourceReplica = replica
		}
	}
	require.NotNil(t, sourceReplica)
	splitShardMsg := message.MustAsMutableSplitShardMessageV2(sourceReplica)
	header := splitShardMsg.Header()
	header.FlushedSegmentIds = []int64{4001, 4002}
	splitShardMsg.OverwriteHeader(header)
	immutable := sourceReplica.WithLastConfirmedUseMessageID().WithTimeTick(1).
		IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	replicate := message.MustNewReplicateMessage("primary", immutable.IntoImmutableMessageProto())
	require.Equal(t, []int64{4001, 4002}, message.MustAsMutableSplitShardMessageV2(replicate).Header().GetFlushedSegmentIds(),
		"the replica arrives carrying the primary's ids")

	msg, err := rs.overwriteReplicateMessage(context.Background(), replicate, replicate.ReplicateHeader())
	require.NoError(t, err)
	remapped := message.MustAsMutableSplitShardMessageV2(msg).Header()
	assert.Empty(t, remapped.GetFlushedSegmentIds())
	// Everything else is still remapped as before.
	assert.Equal(t, "q0_1v0", remapped.GetSourceVchannel())
	assert.Equal(t, []string{"q0_1v1", "q1_1v2"}, remapped.GetTargetVchannels())
}

// TestReplicateAppendReturnsAGateViolationAsItStands: a gate refused as a
// replicate violation -- the coord's task does not carry an append-first
// vchannel this header names -- surfaces with its code intact, so the caller
// can tell it from a transient failure, and the replica is not appended.
func TestReplicateAppendReturnsAGateViolationAsItStands(t *testing.T) {
	appended := make(chan message.MutableMessage, 4)
	rs, bs := newSplitReplicateService(t, appended)

	bs.EXPECT().WaitVChannelsAcked(mock.Anything, mock.Anything, mock.Anything).
		Return(status.NewReplicateViolation("vchannel q0_1v0 is not a vchannel of broadcast 700")).Maybe()

	_, err := rs.Append(context.Background(), replicaOf(splitShardBroadcastOnPrimary(), "p1_1v2"))
	require.Error(t, err)
	assert.True(t, status.AsStreamingError(err).IsReplicateViolation())
	assert.Empty(t, appended, "a replica whose gate was refused must not be appended")
}
