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
// grown vchannel list covers every target.
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
			{Vchannel: "p0_1v2", Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
		},
		Schema:       &schemapb.CollectionSchema{Name: "col"},
		PartitionIDs: []int64{10, 11},
		Routing: &message.AlterCollectionMessageUpdates{
			VirtualChannelNames: []string{"p0_1v0", "p0_1v1", "p0_1v2"},
		},
		ControlChannel: "p0_vcchan",
	}
}

func TestNewSplitShardBroadcastMessage(t *testing.T) {
	param := newSplitShardParam()
	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// the broadcast covers sources, targets and the control channel, deduplicated.
	bh := msg.BroadcastHeader()
	assert.ElementsMatch(t, []string{"p0_1v0", "p0_1v1", "p0_1v2", "p0_vcchan"}, bh.VChannels)
	// only the sources are appended (and therefore persisted) first.
	assert.ElementsMatch(t, param.SourceVChannels, bh.AppendFirstVChannels)
	assert.True(t, msg.IsUnreplicable())
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
	// "p1_1v0" is a bystander shard of the same collection: neither a source
	// nor a target of this split, but still listed among the collection's
	// current vchannels.
	param.CollectionVChannels = []string{"p0_1v0", "p1_1v0"}

	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	require.NoError(t, err)
	require.NotNil(t, msg)

	bh := msg.BroadcastHeader()
	assert.ElementsMatch(t,
		[]string{"p0_1v0", "p1_1v0", "p0_1v1", "p0_1v2", "p0_vcchan"},
		bh.VChannels)
	assert.ElementsMatch(t, param.SourceVChannels, bh.AppendFirstVChannels)
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
	param.SourceVChannels = []string{"p0_1v0", "p1_1v0"}
	param.Routing.VirtualChannelNames = append(param.Routing.VirtualChannelNames, "p1_1v0")

	newResult := func() *types.BroadcastAppendResult {
		return &types.BroadcastAppendResult{
			AppendResults: map[string]*types.AppendResult{
				"p0_1v0":             {MessageID: rmq.NewRmqID(1), TimeTick: 1000},
				"p1_1v0":             {MessageID: rmq.NewRmqID(2), TimeTick: 1100},
				"p0_1v1":             {MessageID: rmq.NewRmqID(3), TimeTick: 1200},
				"p0_1v2":             {MessageID: rmq.NewRmqID(4), TimeTick: 1300},
				param.ControlChannel: {MessageID: rmq.NewRmqID(5), TimeTick: 1400},
			},
		}
	}

	sr, err := streaming.SplitShardResultFrom(param, newResult())
	require.NoError(t, err)
	assert.Equal(t, map[string]uint64{"p0_1v0": 1000, "p1_1v0": 1100}, sr.SwitchTimeTicks)

	require.Len(t, sr.GenesisPositions, 2)
	byChannel := lo.SliceToMap(sr.GenesisPositions, func(pos *msgpb.MsgPosition) (string, *msgpb.MsgPosition) {
		return pos.GetChannelName(), pos
	})
	pos1, ok := byChannel["p0_1v1"]
	require.True(t, ok)
	assert.Equal(t, uint64(1200), pos1.GetTimestamp())
	assert.NotEmpty(t, pos1.GetMsgID())
	pos2, ok := byChannel["p0_1v2"]
	require.True(t, ok)
	assert.Equal(t, uint64(1300), pos2.GetTimestamp())
	assert.NotEmpty(t, pos2.GetMsgID())

	// a result missing a source is a Milvus-internal bug: System, non-retriable.
	missingSource := newResult()
	delete(missingSource.AppendResults, "p1_1v0")
	sr, err = streaming.SplitShardResultFrom(param, missingSource)
	assert.Nil(t, sr)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
	assert.False(t, merr.IsRetryableErr(err))

	// a result missing a target is the same class of error.
	missingTarget := newResult()
	delete(missingTarget.AppendResults, "p0_1v2")
	sr, err = streaming.SplitShardResultFrom(param, missingTarget)
	assert.Nil(t, sr)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
	assert.False(t, merr.IsRetryableErr(err))
}
