package streaming_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// newSplitShardParam builds a valid two-target param: one source, two targets
// whose residues exactly cover the source's, and a routing post-image whose
// grown vchannel list covers every target and tiles the key space.
//
// One target takes the source's own pchannel -- legal, because the fence
// removes the source's registration in the same critical section -- and the
// other a fresh one. Two targets on ONE pchannel would not be legal: a
// collection holds at most one vchannel per pchannel.
func newSplitShardParam() streaming.SplitShardParam {
	return streaming.SplitShardParam{
		CollectionID:        1,
		DBID:                2,
		SplitTaskID:         100,
		SourceVChannels:     []string{"p0_1v0"},
		CollectionVChannels: []string{"p0_1v0"},
		RoutingModulus:      2,
		Targets: []*message.SplitShardTarget{
			{Vchannel: "p0_1v1", Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
			{Vchannel: "p1_1v2", Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
		},
		Schema:         &schemapb.CollectionSchema{Name: "col"},
		PartitionIDs:   []int64{10, 11},
		Routing:        newSplitShardPostImage(),
		ControlChannel: "p0_vcchan",
	}
}

// newSplitShardPostImage is the routing post-image the param above commits:
// the source fenced and owning nothing, the two targets created and owning one
// residue each, which tiles [0, 2) exactly.
func newSplitShardPostImage() *message.AlterCollectionMessageUpdates {
	return &message.AlterCollectionMessageUpdates{
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
	}
}

func TestNewSplitShardBroadcastMessage(t *testing.T) {
	param := newSplitShardParam()
	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// the broadcast covers sources, targets and the control channel, deduplicated.
	bh := msg.BroadcastHeader()
	assert.ElementsMatch(t, []string{"p0_1v0", "p0_1v1", "p1_1v2", "p0_vcchan"}, bh.VChannels)
	// only the sources are appended (and therefore persisted) first.
	assert.ElementsMatch(t, param.SourceVChannels, bh.AppendFirstVChannels)
	assert.Equal(t,
		message.NewCollectionScopedIdempotencyKey(param.CollectionID, fmt.Sprintf("shard-split-%d", param.SplitTaskID)),
		message.IdempotencyKeyOf(msg))

	// header/body round-trip through every replica the broadcast splits into.
	msg.OverwriteBroadcastHeader(1)
	splits := msg.SplitIntoMutableMessage()
	require.Len(t, splits, 4)
	for _, replica := range splits {
		specialized := message.MustAsMutableSplitShardMessageV2(replica)
		header := specialized.Header()
		assert.Equal(t, param.CollectionID, header.GetCollectionId())
		assert.Equal(t, param.SplitTaskID, header.GetSplitTaskId())
		assert.Equal(t, param.SourceVChannels, header.GetSourceVchannels())
		assert.EqualValues(t, param.RoutingModulus, header.GetRoutingModulus())
		assert.Equal(t, param.PartitionIDs, header.GetPartitionIds())
		assert.Equal(t, param.DBID, header.GetDbId())
		require.Len(t, header.GetTargets(), 2)

		body, err := specialized.Body()
		require.NoError(t, err)
		assert.Equal(t, "col", body.GetGenesis().GetCollectionSchema().GetName())
		assert.Equal(t, param.Routing.GetVirtualChannelNames(), body.GetRouting().GetVirtualChannelNames())
	}
}

// TestNewSplitShardBroadcastMessageCoversTheCollection pins the broadcast
// redesign: it must reach every vchannel of the collection, not only the
// sources, the targets and the control channel. A vchannel that is none of
// those -- a bystander shard the split neither fences nor creates -- still
// needs the message so it can observe the split; the vchannel set is the
// union of CollectionVChannels, SourceVChannels, the targets and the control
// channel, and only the sources are appended (and therefore persisted) first.
func TestNewSplitShardBroadcastMessageCoversTheCollection(t *testing.T) {
	param := newSplitShardParam()
	// "p2_1v0" is a bystander shard of the same collection: neither a source
	// nor a target of this split, but still listed among the collection's
	// current vchannels.
	param.CollectionVChannels = []string{"p0_1v0", "p2_1v0"}

	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	require.NoError(t, err)
	require.NotNil(t, msg)

	bh := msg.BroadcastHeader()
	assert.ElementsMatch(t,
		[]string{"p0_1v0", "p2_1v0", "p0_1v1", "p1_1v2", "p0_vcchan"},
		bh.VChannels)
	assert.ElementsMatch(t, param.SourceVChannels, bh.AppendFirstVChannels)
}

// TestSplitShardBroadcastIsReplicable pins the decision this whole redesign
// rests on: the split travels down the replicate stream instead of being
// withheld from it, so a secondary cluster ends up with the same shard topology
// as the primary rather than silently diverging at the first split.
//
// Marking it unreplicable again would not fail any other test here -- it would
// just stop the split from ever reaching a secondary -- which is exactly why
// this assertion exists on its own.
func TestSplitShardBroadcastIsReplicable(t *testing.T) {
	msg, err := streaming.NewSplitShardBroadcastMessage(newSplitShardParam())
	require.NoError(t, err)
	assert.False(t, msg.IsUnreplicable())

	// Every replica of it, not just the broadcast envelope.
	msg.OverwriteBroadcastHeader(1)
	for _, replica := range msg.SplitIntoMutableMessage() {
		assert.False(t, replica.IsUnreplicable(), replica.VChannel())
	}
}

// assertSplitShardParamInvalid asserts that Validate reports the caller of
// this package -- DataCoord's FSM -- with a Milvus bug, never a caller
// mistake: the blame test (docs/dev/error_handling_guide.md) puts the fault on
// whichever internal component built the malformed param, not on request
// content, so every Validate failure is a System error (ErrServiceInternal)
// and never retriable.
func assertSplitShardParamInvalid(t *testing.T, param streaming.SplitShardParam) {
	err := param.Validate()
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
	assert.False(t, merr.IsRetryableErr(err))
}

func TestSplitShardParamValidate(t *testing.T) {
	// valid param.
	param := newSplitShardParam()
	assert.NoError(t, param.Validate())

	// ids must be positive.
	param = newSplitShardParam()
	param.CollectionID = 0
	assertSplitShardParamInvalid(t, param)

	param = newSplitShardParam()
	param.SplitTaskID = 0
	assertSplitShardParamInvalid(t, param)

	// the db id must be positive: DataCoord always resolves a real db for the
	// collection it is splitting, so a non-positive value is a Milvus bug.
	param = newSplitShardParam()
	param.DBID = 0
	assertSplitShardParamInvalid(t, param)

	// partition ids must be set: a target registered with no partition
	// accepts no insert.
	param = newSplitShardParam()
	param.PartitionIDs = nil
	assertSplitShardParamInvalid(t, param)

	// sources must be set, and each must be non-empty.
	param = newSplitShardParam()
	param.SourceVChannels = nil
	assertSplitShardParamInvalid(t, param)

	param = newSplitShardParam()
	param.SourceVChannels = []string{""}
	assertSplitShardParamInvalid(t, param)

	// the source vchannels must not duplicate each other.
	param = newSplitShardParam()
	param.SourceVChannels = []string{"p0_1v0", "p0_1v0"}
	assertSplitShardParamInvalid(t, param)

	// routing modulus must be set.
	param = newSplitShardParam()
	param.RoutingModulus = 0
	assertSplitShardParamInvalid(t, param)

	// schema must be set.
	param = newSplitShardParam()
	param.Schema = nil
	assertSplitShardParamInvalid(t, param)

	// control channel must be set.
	param = newSplitShardParam()
	param.ControlChannel = ""
	assertSplitShardParamInvalid(t, param)

	// routing post-image must be set.
	param = newSplitShardParam()
	param.Routing = nil
	assertSplitShardParamInvalid(t, param)

	// One target is legal: a source of a rehash fronts only its share of the
	// split's targets, and with more sources than targets it may front none.
	param = newSplitShardParam()
	param.Targets = param.Targets[:1]
	assert.NoError(t, param.Validate())
	param.Targets = nil
	assert.NoError(t, param.Validate())

	// target vchannel must be set.
	param = newSplitShardParam()
	param.Targets[0].Vchannel = ""
	assertSplitShardParamInvalid(t, param)

	// the target vchannel must not duplicate a source.
	param = newSplitShardParam()
	param.Targets[0].Vchannel = "p0_1v0"
	assertSplitShardParamInvalid(t, param)

	// the target vchannels must not duplicate each other.
	param = newSplitShardParam()
	param.Targets[1].Vchannel = param.Targets[0].Vchannel
	assertSplitShardParamInvalid(t, param)

	// a target with no residue is refused.
	param = newSplitShardParam()
	param.Targets[0].Routing = &schemapb.HashRouting{}
	assertSplitShardParamInvalid(t, param)

	// a residue not below the modulus is refused.
	param = newSplitShardParam()
	param.Targets[0].Routing = &schemapb.HashRouting{Buckets: []uint64{2}}
	assertSplitShardParamInvalid(t, param)

	// the routing post-image must cover every target.
	param = newSplitShardParam()
	param.Routing = &message.AlterCollectionMessageUpdates{VirtualChannelNames: []string{"p0_1v0", "p0_1v1"}}
	assertSplitShardParamInvalid(t, param)

	// every source must be one of the collection's current vchannels: a
	// source the coordinator did not list there cannot be a real shard of
	// the collection this split claims to act on.
	param = newSplitShardParam()
	param.CollectionVChannels = []string{"p9_9v9"}
	assertSplitShardParamInvalid(t, param)

	// no target may already be one of the collection's current vchannels: a
	// target is a NEW shard, so it cannot also be a pre-existing one.
	param = newSplitShardParam()
	param.CollectionVChannels = append(param.CollectionVChannels, param.Targets[0].Vchannel)
	assertSplitShardParamInvalid(t, param)

	// validation failure happens before any message is built.
	w := newSplitShardParam()
	w.CollectionID = 0
	msg, err := streaming.NewSplitShardBroadcastMessage(w)
	assert.Nil(t, msg)
	assert.Error(t, err)
}

func TestSplitShardResultFrom(t *testing.T) {
	param := newSplitShardParam()
	// exercise more than one source.
	param.SourceVChannels = []string{"p0_1v0", "p2_1v0"}
	param.Routing.VirtualChannelNames = append(param.Routing.VirtualChannelNames, "p2_1v0")

	newResult := func() *types.BroadcastAppendResult {
		return &types.BroadcastAppendResult{
			AppendResults: map[string]*types.AppendResult{
				"p0_1v0":             {MessageID: rmq.NewRmqID(1), TimeTick: 1000},
				"p2_1v0":             {MessageID: rmq.NewRmqID(2), TimeTick: 1100},
				"p0_1v1":             {MessageID: rmq.NewRmqID(3), TimeTick: 1200},
				"p1_1v2":             {MessageID: rmq.NewRmqID(4), TimeTick: 1300},
				param.ControlChannel: {MessageID: rmq.NewRmqID(5), TimeTick: 1400},
			},
		}
	}

	sr, err := streaming.SplitShardResultFrom(param, newResult())
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"p0_1v0": 1000, "p2_1v0": 1100}, sr.SwitchTimeTicks)

	require.Len(t, sr.GenesisPositions, 2)
	byChannel := lo.SliceToMap(sr.GenesisPositions, func(pos *msgpb.MsgPosition) (string, *msgpb.MsgPosition) {
		return pos.GetChannelName(), pos
	})
	pos1, ok := byChannel["p0_1v1"]
	require.True(t, ok)
	assert.Equal(t, uint64(1200), pos1.GetTimestamp())
	assert.NotEmpty(t, pos1.GetMsgID())
	pos2, ok := byChannel["p1_1v2"]
	require.True(t, ok)
	assert.Equal(t, uint64(1300), pos2.GetTimestamp())
	assert.NotEmpty(t, pos2.GetMsgID())

	// a result missing a source is a Milvus-internal bug: System, non-retriable.
	missingSource := newResult()
	delete(missingSource.AppendResults, "p2_1v0")
	sr, err = streaming.SplitShardResultFrom(param, missingSource)
	assert.Nil(t, sr)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
	assert.False(t, merr.IsRetryableErr(err))

	// a result missing a target is the same class of error.
	missingTarget := newResult()
	delete(missingTarget.AppendResults, "p1_1v2")
	sr, err = streaming.SplitShardResultFrom(param, missingTarget)
	assert.Nil(t, sr)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
	assert.False(t, merr.IsRetryableErr(err))
}

// TestSplitShardParamRefusesAMisplacedTarget is the first of the two wedges
// this validation exists to prevent.
//
// A shard manager holds ONE entry per collection per pchannel, so a target
// placed on a pchannel the collection still occupies is refused by the target
// replica's own handler with ErrVChannelConflict -- but only after the source
// replicas have landed and been persisted by AckPartial. From there the
// broadcaster retries the target append forever, holding the collection's
// exclusive resource key, with the source fenced and the target never created:
// the residues the split moved become permanently unwritable and every later
// DDL of the collection queues behind it. There is no rollback, so the
// placement is either refused before the fence or never.
func TestSplitShardParamRefusesAMisplacedTarget(t *testing.T) {
	// Two targets on one pchannel: the second one can never be registered.
	param := newSplitShardParam()
	param.Targets[1].Vchannel = "p0_1v2"
	param.Routing.VirtualChannelNames[2] = "p0_1v2"
	param.Routing.PhysicalChannelNames[2] = "p0"
	param.Routing.ShardInfos[2].VchannelName = "p0_1v2"
	assertSplitShardParamInvalid(t, param)

	// A target on a BYSTANDER's pchannel: that shard is not fenced by this
	// split, so it keeps its registration for the whole window.
	param = newSplitShardParam()
	param.CollectionVChannels = []string{"p0_1v0", "p1_1v0"}
	assertSplitShardParamInvalid(t, param)

	// A target on a SOURCE's pchannel is legal and must stay legal: the fence
	// removes the source's registration in the same critical section that
	// marks it SPLITTED, which is what lets a shard split back onto its own
	// pchannel. newSplitShardParam already does exactly that.
	param = newSplitShardParam()
	require.Equal(t, "p0_1v1", param.Targets[0].GetVchannel())
	assert.NoError(t, param.Validate())
}

// TestSplitShardParamRefusesAnUntiledPostImage is the second wedge: a
// post-image that does not tile, or one that disagrees with the header, is
// refused by the ack callback -- which runs only once every replica has landed,
// i.e. once the source is already fenced. The same check therefore has to run
// before the broadcast, and it runs through the very function the callback
// calls (ValidateSplitShardRoutingPostImage), so the two can never drift.
func TestSplitShardParamRefusesAnUntiledPostImage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		corrupt func(p *streaming.SplitShardParam)
	}{
		{
			name: "the arrays are not parallel",
			corrupt: func(p *streaming.SplitShardParam) {
				p.Routing.PhysicalChannelNames = p.Routing.PhysicalChannelNames[:2]
			},
		},
		{
			name: "the post-image names no channel at all",
			corrupt: func(p *streaming.SplitShardParam) {
				p.Routing.VirtualChannelNames = nil
				p.Routing.PhysicalChannelNames = nil
				p.Routing.ShardInfos = nil
			},
		},
		{
			name: "a gap: residue 1 is owned by nobody",
			corrupt: func(p *streaming.SplitShardParam) {
				p.Routing.ShardInfos[2].Routing = &schemapb.CollectionShardInfo_HashRouting{
					HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
				}
				p.Targets[1].Routing = &schemapb.HashRouting{Buckets: []uint64{0}}
			},
		},
		{
			name: "an overlap: residue 0 is owned twice",
			corrupt: func(p *streaming.SplitShardParam) {
				p.Routing.ShardInfos[0].State = schemapb.ShardState_ShardNormal
				p.Routing.ShardInfos[0].Routing = &schemapb.CollectionShardInfo_HashRouting{
					HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
				}
			},
		},
		{
			name: "a shard info naming another shard's vchannel",
			corrupt: func(p *streaming.SplitShardParam) {
				p.Routing.ShardInfos[1].VchannelName = "p9_9v9"
			},
		},
		{
			name: "the header and the post-image disagree on the modulus",
			corrupt: func(p *streaming.SplitShardParam) {
				// The header's modulus is what DataCoord is handed; the
				// post-image's is what the routing table is built from.
				p.RoutingModulus = 4
				p.Targets[0].Routing = &schemapb.HashRouting{Buckets: []uint64{0}}
				p.Targets[1].Routing = &schemapb.HashRouting{Buckets: []uint64{1}}
			},
		},
		{
			name: "the header and the post-image disagree on a target's residues",
			corrupt: func(p *streaming.SplitShardParam) {
				p.Targets[0].Routing = &schemapb.HashRouting{Buckets: []uint64{1}}
				p.Targets[1].Routing = &schemapb.HashRouting{Buckets: []uint64{0}}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			param := newSplitShardParam()
			tc.corrupt(&param)
			assertSplitShardParamInvalid(t, param)

			// and nothing is built from it.
			msg, err := streaming.NewSplitShardBroadcastMessage(param)
			assert.Nil(t, msg)
			assert.Error(t, err)
		})
	}
}

// TestValidateSplitShardRoutingPostImageIsTheOnePreAndPostFenceGate asserts the
// exported predicate rootcoord's ack callback calls is the same one the builder
// runs: a post-image the builder accepts must be one the callback accepts, or
// the pre-fence gate would refuse broadcasts the post-fence one allows (and,
// worse, the other way round).
func TestValidateSplitShardRoutingPostImageIsTheOnePreAndPostFenceGate(t *testing.T) {
	param := newSplitShardParam()
	require.NoError(t, param.Validate())

	header := &message.SplitShardMessageHeader{
		CollectionId:    param.CollectionID,
		SplitTaskId:     param.SplitTaskID,
		Targets:         param.Targets,
		RoutingModulus:  param.RoutingModulus,
		SourceVchannels: param.SourceVChannels,
		PartitionIds:    param.PartitionIDs,
		DbId:            param.DBID,
	}
	assert.NoError(t, streaming.ValidateSplitShardRoutingPostImage(header, param.Routing))

	// A modulus the post-image does not share is refused as a System error,
	// non-retriable: the post-image is derived by the split coordinator, never
	// by a user request.
	header.RoutingModulus = 4
	err := streaming.ValidateSplitShardRoutingPostImage(header, param.Routing)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
	assert.False(t, merr.IsRetryableErr(err))
}
