package streaming_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// enableShardSplit turns dataCoord.shardSplit.enable on for the test. The
// builder refuses to issue a split while it is off, which is the default.
func enableShardSplit(t *testing.T) {
	t.Helper()
	key := paramtable.Get().DataCoordCfg.ShardSplitEnable.Key
	paramtable.Get().Save(key, "true")
	t.Cleanup(func() { paramtable.Get().Reset(key) })
}

// TestNewSplitShardBroadcastMessageIsGatedByTheSwitch pins that
// dataCoord.shardSplit.enable gates issuing a split at the builder: off, the
// builder refuses before it validates or builds anything, with a System code
// that is not retriable; on, the same param builds. The switch is refreshable,
// so flipping it takes effect on the next build.
func TestNewSplitShardBroadcastMessageIsGatedByTheSwitch(t *testing.T) {
	key := paramtable.Get().DataCoordCfg.ShardSplitEnable.Key
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	paramtable.Get().Save(key, "false")
	msg, err := streaming.NewSplitShardBroadcastMessage(newSplitShardParam())
	assert.Nil(t, msg)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	assert.False(t, merr.IsRetryableErr(err))
	assert.ErrorContains(t, err, key)

	// Refused before validation: a malformed param reports the switch, not the
	// malformation.
	invalid := newSplitShardParam()
	invalid.SourceVChannel = ""
	msg, err = streaming.NewSplitShardBroadcastMessage(invalid)
	assert.Nil(t, msg)
	require.ErrorIs(t, err, merr.ErrOperationNotSupported)
	assert.NotErrorIs(t, err, merr.ErrServiceInternal)

	paramtable.Get().Save(key, "true")
	msg, err = streaming.NewSplitShardBroadcastMessage(newSplitShardParam())
	require.NoError(t, err)
	require.NotNil(t, msg)

	// A malformed param is still refused by validation once the switch is on.
	msg, err = streaming.NewSplitShardBroadcastMessage(invalid)
	assert.Nil(t, msg)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

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
		CollectionID:    1,
		SplitTaskID:     100,
		SourceVChannel:  "p0_1v0",
		TargetVChannels: []string{"p0_1v1", "p1_1v2"},
		Schema:          &schemapb.CollectionSchema{Name: "col"},
		PartitionIDs:    []int64{10, 11},
		Routing:         newSplitShardPostImage(),
		ControlChannel:  "p0_vcchan",
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
	enableShardSplit(t)
	param := newSplitShardParam()
	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	require.NoError(t, err)
	require.NotNil(t, msg)

	// the broadcast covers sources, targets and the control channel, deduplicated.
	bh := msg.BroadcastHeader()
	assert.ElementsMatch(t, []string{"p0_1v0", "p0_1v1", "p1_1v2", "p0_vcchan"}, bh.VChannels)
	// only the source is appended (and therefore persisted) first.
	assert.Equal(t, []string{param.SourceVChannel}, bh.AppendFirstVChannels)
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
		assert.Equal(t, param.SourceVChannel, header.GetSourceVchannel())
		assert.Equal(t, param.TargetVChannels, header.GetTargetVchannels())
		assert.Equal(t, param.PartitionIDs, header.GetPartitionIds())

		body, err := specialized.Body(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "col", body.GetGenesis().GetCollectionSchema().GetName())
		assert.Equal(t, param.Routing.GetVirtualChannelNames(), body.GetRouting().GetVirtualChannelNames())
	}
}

// withUntouchedShard grows the param's collection by one shard the split leaves
// alone: the post-image lists it Normal, owning residues 2 and 3 of a modulus
// raised to 4, so the post-image still tiles.
func withUntouchedShard(param streaming.SplitShardParam, vchannel, pchannel string) streaming.SplitShardParam {
	param.Routing.RoutingModulus = 4
	param.Routing.VirtualChannelNames = append(param.Routing.VirtualChannelNames, vchannel)
	param.Routing.PhysicalChannelNames = append(param.Routing.PhysicalChannelNames, pchannel)
	param.Routing.ShardInfos = append(param.Routing.ShardInfos, &schemapb.CollectionShardInfo{
		VchannelName: vchannel,
		State:        schemapb.ShardState_ShardNormal,
		Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{2, 3}},
		},
	})
	return param
}

// TestNewSplitShardBroadcastMessageReachesOnlyTheSplitsVChannels pins that the
// broadcast has no bystanders: a shard the split neither fences nor creates gets
// no replica, even though the post-image lists it. The vchannel set is exactly
// the source, the targets and the control channel, and only the source is
// appended (and therefore persisted) first.
func TestNewSplitShardBroadcastMessageReachesOnlyTheSplitsVChannels(t *testing.T) {
	enableShardSplit(t)
	// "p2_1v3" is a shard of the same collection the split leaves alone.
	param := withUntouchedShard(newSplitShardParam(), "p2_1v3", "p2")
	require.NoError(t, param.Validate())

	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	require.NoError(t, err)
	require.NotNil(t, msg)

	bh := msg.BroadcastHeader()
	assert.ElementsMatch(t, []string{"p0_1v0", "p0_1v1", "p1_1v2", "p0_vcchan"}, bh.VChannels)
	assert.NotContains(t, bh.VChannels, "p2_1v3")
	assert.Equal(t, []string{param.SourceVChannel}, bh.AppendFirstVChannels)
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
	enableShardSplit(t)
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
	enableShardSplit(t)
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

	// partition ids must be set: a target registered with no partition
	// accepts no insert.
	param = newSplitShardParam()
	param.PartitionIDs = nil
	assertSplitShardParamInvalid(t, param)

	// the source must be set.
	param = newSplitShardParam()
	param.SourceVChannel = ""
	assertSplitShardParamInvalid(t, param)

	// the post-image must carry an explicit routing modulus.
	param = newSplitShardParam()
	param.Routing.RoutingModulus = 0
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

	// a split creates exactly two targets: one (a shrink or a partial share),
	// none, or three are refused.
	param = newSplitShardParam()
	param.TargetVChannels = param.TargetVChannels[:1]
	assertSplitShardParamInvalid(t, param)
	param.TargetVChannels = nil
	assertSplitShardParamInvalid(t, param)
	param = newSplitShardParam()
	param.TargetVChannels = append(param.TargetVChannels, "p2_1v3")
	assertSplitShardParamInvalid(t, param)

	// target vchannel must be set.
	param = newSplitShardParam()
	param.TargetVChannels[0] = ""
	assertSplitShardParamInvalid(t, param)

	// the target vchannel must not duplicate the source.
	param = newSplitShardParam()
	param.TargetVChannels[0] = "p0_1v0"
	assertSplitShardParamInvalid(t, param)

	// the target vchannels must not duplicate each other.
	param = newSplitShardParam()
	param.TargetVChannels[1] = param.TargetVChannels[0]
	assertSplitShardParamInvalid(t, param)

	// a target the post-image gives no residue is refused.
	param = newSplitShardParam()
	param.Routing.ShardInfos[1].Routing = nil
	assertSplitShardParamInvalid(t, param)

	// a residue not below the modulus is refused.
	param = newSplitShardParam()
	param.Routing.ShardInfos[1].Routing = &schemapb.CollectionShardInfo_HashRouting{
		HashRouting: &schemapb.HashRouting{Buckets: []uint64{2}},
	}
	assertSplitShardParamInvalid(t, param)

	// the routing post-image must cover every target.
	param = newSplitShardParam()
	param.Routing = &message.AlterCollectionMessageUpdates{VirtualChannelNames: []string{"p0_1v0", "p0_1v1"}}
	assertSplitShardParamInvalid(t, param)

	// the routing post-image must name the source: it stays listed, Splitting,
	// until adoption.
	param = newSplitShardParam()
	param.SourceVChannel = "p9_1v9"
	assertSplitShardParamInvalid(t, param)

	// validation failure happens before any message is built.
	w := newSplitShardParam()
	w.CollectionID = 0
	msg, err := streaming.NewSplitShardBroadcastMessage(w)
	assert.Nil(t, msg)
	assert.Error(t, err)
}

// TestSplitShardParamRefusesAMisplacedTarget is the first of the two wedges
// this validation exists to prevent.
//
// A shard manager holds ONE entry per collection per pchannel, so a target
// placed on a pchannel the collection still occupies is refused by the target
// replica's own handler with ErrVChannelConflict -- but only after the source
// replica has landed and been persisted by AckPartial. From there the
// broadcaster retries the target append forever, holding the collection's
// exclusive resource key, with the source fenced and the target never created:
// the residues the split moved become permanently unwritable and every later
// DDL of the collection queues behind it. There is no rollback, so the
// placement is either refused before the fence or never.
func TestSplitShardParamRefusesAMisplacedTarget(t *testing.T) {
	// Two targets on one pchannel: the second one can never be registered.
	param := newSplitShardParam()
	param.TargetVChannels[1] = "p0_1v2"
	param.Routing.VirtualChannelNames[2] = "p0_1v2"
	param.Routing.PhysicalChannelNames[2] = "p0"
	param.Routing.ShardInfos[2].VchannelName = "p0_1v2"
	assertSplitShardParamInvalid(t, param)

	// A target on the pchannel of a shard the split leaves alone: that shard is
	// not fenced by this split, so it keeps its registration for the whole
	// window. The post-image is the only list of the collection's shards the
	// param carries, so that is where the incumbent is found.
	param = withUntouchedShard(newSplitShardParam(), "p1_1v3", "p1")
	assertSplitShardParamInvalid(t, param)
	// The same shard on a pchannel no target uses is fine.
	param = withUntouchedShard(newSplitShardParam(), "p2_1v3", "p2")
	assert.NoError(t, param.Validate())

	// A target on the SOURCE's pchannel is legal and must stay legal: the fence
	// removes the source's registration in the same critical section that
	// marks it SPLITTED, which is what lets a shard split back onto its own
	// pchannel. newSplitShardParam already does exactly that.
	param = newSplitShardParam()
	require.Equal(t, "p0_1v1", param.TargetVChannels[0])
	assert.NoError(t, param.Validate())
}

// TestSplitShardParamRefusesAnUntiledPostImage is the second wedge: a
// post-image that does not tile is refused by the ack callback -- which runs
// only once every replica has landed, i.e. once the source is already fenced.
// The same check therefore has to run before the broadcast, and it runs through
// the very function the callback calls (ValidateSplitShardMessage), so
// the two can never drift.
func TestSplitShardParamRefusesAnUntiledPostImage(t *testing.T) {
	enableShardSplit(t)
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
			// the shape check is routing.CheckPostImageShape, shared with the
			// adoption's judge: a vchannel listed twice is refused here too.
			name: "a vchannel listed twice",
			corrupt: func(p *streaming.SplitShardParam) {
				p.Routing.VirtualChannelNames = append(p.Routing.VirtualChannelNames, p.Routing.VirtualChannelNames[0])
				p.Routing.PhysicalChannelNames = append(p.Routing.PhysicalChannelNames, p.Routing.PhysicalChannelNames[0])
				p.Routing.ShardInfos = append(p.Routing.ShardInfos, p.Routing.ShardInfos[0])
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

// TestValidateSplitShardMessageIsTheOnePreAndPostFenceGate asserts the
// exported predicate rootcoord's ack callback calls is the same one the builder
// runs: a message the builder accepts must be one the callback accepts, or the
// pre-fence gate would refuse broadcasts the post-fence one allows (and, worse,
// the other way round).
func TestValidateSplitShardMessageIsTheOnePreAndPostFenceGate(t *testing.T) {
	param := newSplitShardParam()
	require.NoError(t, param.Validate())

	header := &message.SplitShardMessageHeader{
		CollectionId:    param.CollectionID,
		SplitTaskId:     param.SplitTaskID,
		SourceVchannel:  param.SourceVChannel,
		TargetVchannels: param.TargetVChannels,
		PartitionIds:    param.PartitionIDs,
	}
	body := &message.SplitShardMessageBody{
		Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: param.Schema},
		Routing: param.Routing,
	}
	assert.NoError(t, streaming.ValidateSplitShardMessage(header, body))

	// A body without a post-image has nothing to commit.
	err := streaming.ValidateSplitShardMessage(header, &message.SplitShardMessageBody{})
	require.ErrorIs(t, err, merr.ErrServiceInternal)

	// The arity is part of the same gate, so a message that reached the WAL
	// without the builder is refused by the callback too -- as a System error,
	// non-retriable: the message is derived by the split coordinator, never by a
	// user request.
	header.SourceVchannel = ""
	err = streaming.ValidateSplitShardMessage(header, body)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.False(t, merr.IsRetryableErr(err))
	assert.ErrorContains(t, err, "exactly one source")

	header.SourceVchannel = param.SourceVChannel
	header.TargetVchannels = param.TargetVChannels[:1]
	err = streaming.ValidateSplitShardMessage(header, body)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.ErrorContains(t, err, "exactly 2 target")
}

// namespacePlacedProperties are the collection properties of a collection whose
// rows the proxy has always placed by namespace: sharding enabled, in
// partition_key mode.
func namespacePlacedProperties() []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{Key: common.NamespaceShardingEnabledKey, Value: "true"},
		{Key: common.NamespaceModeKey, Value: common.NamespaceModePartitionKey},
	}
}

// withNamespaceRouting turns the param into a namespace split: shard_by is the
// namespace key, the genesis schema is a namespace collection's
// (enable_namespace set) carrying properties, and the collection has buckets
// partition-key partitions. Namespace collections are not split yet (§1.3), so
// such a param is refused; the admission and granularity checks run first and
// keep their own refusals.
func withNamespaceRouting(param streaming.SplitShardParam, buckets int, properties []*commonpb.KeyValuePair) streaming.SplitShardParam {
	param.Routing.ShardBy = routing.NamespaceShardBy
	param.Schema.EnableNamespace = true
	param.Schema.Properties = properties
	param.PartitionIDs = make([]int64, 0, buckets)
	for i := 0; i < buckets; i++ {
		param.PartitionIDs = append(param.PartitionIDs, int64(1000+i))
	}
	return param
}

// assertSplitShardParamRefused asserts a System refusal that names why.
func assertSplitShardParamRefused(t *testing.T, param streaming.SplitShardParam, contains string) {
	t.Helper()
	assertSplitShardParamInvalid(t, param)
	assert.ErrorContains(t, param.Validate(), contains)
	msg, err := streaming.NewSplitShardBroadcastMessage(param)
	assert.Nil(t, msg)
	assert.Error(t, err)
}

// TestSplitShardParamRefusesWhatTheApplyWouldRefuseAfterTheFence (H4): every
// refusal the ack callback or the routing apply could make that depends on the
// message alone is made here, before the broadcast. After the fence the same
// refusal can only be retried forever with the source fenced and the
// collection's DDL queued behind it.
func TestSplitShardParamRefusesWhatTheApplyWouldRefuseAfterTheFence(t *testing.T) {
	enableShardSplit(t)
	// Admission and granularity pass, and the split is still refused: namespace
	// collections are not split yet (§1.3).
	t.Run("a namespace split that passes admission is deferred", func(t *testing.T) {
		param := withNamespaceRouting(newSplitShardParam(), 16, namespacePlacedProperties())
		assertSplitShardParamRefused(t, param, namespaceDeferral)
	})

	t.Run("namespace admission (§3.1)", func(t *testing.T) {
		for _, tc := range []struct {
			name       string
			properties []*commonpb.KeyValuePair
			contains   string
		}{
			{name: "no properties: sharding defaults to disabled", contains: "placed by primary key"},
			{
				name:       "sharding disabled",
				properties: []*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "false"}},
				contains:   "placed by primary key",
			},
			{
				name: "partition mode is always placed by primary key",
				properties: []*commonpb.KeyValuePair{
					{Key: common.NamespaceShardingEnabledKey, Value: "true"},
					{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
				},
				contains: "placed by primary key",
			},
			{
				name:       "a malformed sharding property",
				properties: []*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "yes"}},
				contains:   common.NamespaceShardingEnabledKey,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				assertSplitShardParamRefused(t, withNamespaceRouting(newSplitShardParam(), 16, tc.properties), tc.contains)
			})
		}
	})

	t.Run("a target that is not Creating", func(t *testing.T) {
		param := newSplitShardParam()
		param.Routing.ShardInfos[1].State = schemapb.ShardState_ShardNormal
		assertSplitShardParamRefused(t, param, "born Creating")
	})

	t.Run("a source that is not Splitting", func(t *testing.T) {
		param := newSplitShardParam()
		param.Routing.ShardInfos[0].State = schemapb.ShardState_ShardDropped
		assertSplitShardParamRefused(t, param, "a fenced source is Splitting")

		// Normal, owning nothing, is refused too -- by the tiling, since a
		// writable shard must own a residue.
		param = newSplitShardParam()
		param.Routing.ShardInfos[0].State = schemapb.ShardState_ShardNormal
		assertSplitShardParamInvalid(t, param)
	})

	// F4: a shard reaches Dropped only by being delisted (after its drain), so a
	// post-image listing any shard as Dropped is refused before the fence.
	t.Run("a listed Dropped shard", func(t *testing.T) {
		param := newSplitShardParam()
		param.Routing.VirtualChannelNames = append(param.Routing.VirtualChannelNames, "p2_1v3")
		param.Routing.PhysicalChannelNames = append(param.Routing.PhysicalChannelNames, "p2")
		param.Routing.ShardInfos = append(param.Routing.ShardInfos,
			&schemapb.CollectionShardInfo{VchannelName: "p2_1v3", State: schemapb.ShardState_ShardDropped})
		assertSplitShardParamRefused(t, param, "reaches Dropped only by being delisted")
	})

	t.Run("a modulus above the cap", func(t *testing.T) {
		param := newSplitShardParam()
		param.Routing.RoutingModulus = 1 << 16
		assertSplitShardParamRefused(t, param, "exceeds the cap")
	})
}

// TestSplitShardParamNamespaceModulusMustDivideTheBuckets (design gap G1): a
// namespace split moves data by relabeling whole partition-key buckets, which
// is possible only when the modulus divides the bucket count.
func TestSplitShardParamNamespaceModulusMustDivideTheBuckets(t *testing.T) {
	// Modulus 2 over 16 buckets: every bucket lies on one residue, so the
	// granularity check passes and only the namespace deferral (§1.3) refuses.
	divides := withNamespaceRouting(newSplitShardParam(), 16, namespacePlacedProperties())
	assertSplitShardParamRefused(t, divides, namespaceDeferral)

	// Modulus 3: the post-image still tiles (an untouched shard owns residue 2),
	// but 3 does not divide 16.
	three := func() streaming.SplitShardParam {
		param := newSplitShardParam()
		param.Routing.RoutingModulus = 3
		param.Routing.VirtualChannelNames = append(param.Routing.VirtualChannelNames, "p2_1v3")
		param.Routing.PhysicalChannelNames = append(param.Routing.PhysicalChannelNames, "p2")
		param.Routing.ShardInfos = append(param.Routing.ShardInfos, &schemapb.CollectionShardInfo{
			VchannelName: "p2_1v3",
			State:        schemapb.ShardState_ShardNormal,
			Routing:      &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: []uint64{2}}},
		})
		return param
	}
	tiledAtThree := three()
	require.NoError(t, tiledAtThree.Validate(), "the tiling itself is fine")
	assertSplitShardParamRefused(t, withNamespaceRouting(three(), 16, namespacePlacedProperties()), "must divide the 16 partition-key buckets")

	// Modulus 32 over 16 buckets: more residues than buckets.
	thirtyTwo := func() streaming.SplitShardParam {
		param := newSplitShardParam()
		param.Routing.RoutingModulus = 32
		var evens, odds []uint64
		for r := uint64(0); r < 32; r++ {
			if r%2 == 0 {
				evens = append(evens, r)
			} else {
				odds = append(odds, r)
			}
		}
		param.Routing.ShardInfos[1].Routing = &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: evens}}
		param.Routing.ShardInfos[2].Routing = &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: odds}}
		return param
	}
	tiledAtThirtyTwo := thirtyTwo()
	require.NoError(t, tiledAtThirtyTwo.Validate(), "the tiling itself is fine")
	assertSplitShardParamRefused(t, withNamespaceRouting(thirtyTwo(), 16, namespacePlacedProperties()), "must divide the 16 partition-key buckets")

	// hash(pk) post-images are not constrained by the bucket count.
	pk := three()
	pk.Routing.ShardBy = "hash(pk)"
	require.NoError(t, pk.Validate())
}

// namespaceDeferral is what the refusal of a namespace collection's split says.
const namespaceDeferral = "namespace collections are not split"

// TestSplitShardRefusesANamespaceCollection (§1.3): no split is issued for a
// collection whose schema has enable_namespace set, in either namespace.mode,
// even under a hash(pk) post-image that every other check accepts. The refusal
// is made by ValidateSplitShardMessage, so the builder, the planner's check and
// the ack callback's re-run all make it from the same message, before and after
// the fence alike.
func TestSplitShardRefusesANamespaceCollection(t *testing.T) {
	enableShardSplit(t)
	for _, tc := range []struct {
		name       string
		properties []*commonpb.KeyValuePair
	}{
		{name: "partition_key mode", properties: namespacePlacedProperties()},
		{
			name: "partition mode",
			properties: []*commonpb.KeyValuePair{
				{Key: common.NamespaceShardingEnabledKey, Value: "false"},
				{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The same hash(pk) split of a collection without namespaces passes.
			plain := newSplitShardParam()
			plain.Schema.Properties = tc.properties
			require.NoError(t, plain.Validate())

			param := newSplitShardParam()
			param.Schema.EnableNamespace = true
			param.Schema.Properties = tc.properties
			assertSplitShardParamRefused(t, param, namespaceDeferral)

			err := streaming.ValidateSplitShardMessage(splitShardMessageOf(param))
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			assert.False(t, merr.IsRetryableErr(err))
			assert.ErrorContains(t, err, "collection 1")

			coll := &model.Collection{
				CollectionID:        1,
				Name:                "col",
				VirtualChannelNames: []string{param.SourceVChannel},
				EnableNamespace:     true,
				Properties:          tc.properties,
			}
			header, body := splitShardMessageOf(param)
			err = streaming.CheckSplitShardAgainstCollection(coll, header, body)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			assert.ErrorContains(t, err, namespaceDeferral)
		})
	}
}

// TestSplitShardTargetBuckets: the post-image is the only copy of a target's
// residues, and a vchannel it does not name owns none.
func TestSplitShardTargetBuckets(t *testing.T) {
	postImage := newSplitShardPostImage()
	assert.Equal(t, []uint64{0}, streaming.SplitShardTargetBuckets(postImage, "p0_1v1"))
	assert.Equal(t, []uint64{1}, streaming.SplitShardTargetBuckets(postImage, "p1_1v2"))
	assert.Nil(t, streaming.SplitShardTargetBuckets(postImage, "p0_1v0"))
	assert.Nil(t, streaming.SplitShardTargetBuckets(postImage, "p9_1v9"))

	// A post-image whose shard infos are shorter than its names answers nil
	// rather than indexing past the end.
	postImage.ShardInfos = postImage.ShardInfos[:1]
	assert.Nil(t, streaming.SplitShardTargetBuckets(postImage, "p1_1v2"))
}

// splitShardMessageOf is the header and body NewSplitShardBroadcastMessage puts
// on the wire for param.
func splitShardMessageOf(param streaming.SplitShardParam) (*message.SplitShardMessageHeader, *message.SplitShardMessageBody) {
	return &message.SplitShardMessageHeader{
			CollectionId:    param.CollectionID,
			SplitTaskId:     param.SplitTaskID,
			SourceVchannel:  param.SourceVChannel,
			TargetVchannels: param.TargetVChannels,
			PartitionIds:    param.PartitionIDs,
		}, &message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: param.Schema},
			Routing: param.Routing,
		}
}

// TestCheckSplitShardAgainstCollection (F5): the planner's pre-broadcast check
// against the collection meta refuses, before the fence, what the ack callback
// or the routing apply would refuse only after it.
func TestCheckSplitShardAgainstCollection(t *testing.T) {
	const source = "p0_1v0"
	legacy := func() *model.Collection {
		return &model.Collection{CollectionID: 1, Name: "col", VirtualChannelNames: []string{source}}
	}
	check := func(coll *model.Collection, param streaming.SplitShardParam) error {
		header, body := splitShardMessageOf(param)
		return streaming.CheckSplitShardAgainstCollection(coll, header, body)
	}
	requireRefused := func(t *testing.T, err error, contains string) {
		t.Helper()
		require.ErrorIs(t, err, merr.ErrServiceInternal, "a planning bug, not user input")
		assert.False(t, merr.IsRetryableErr(err))
		assert.ErrorContains(t, err, contains)
	}

	t.Run("a split of a never-split collection passes", func(t *testing.T) {
		require.NoError(t, check(legacy(), newSplitShardParam()))
		coll := legacy()
		coll.ShardInfos = map[string]*model.ShardInfo{source: {VChannelName: source, State: schemapb.ShardState_ShardNormal}}
		require.NoError(t, check(coll, newSplitShardParam()))
	})

	t.Run("a post-image the collection already carries passes", func(t *testing.T) {
		param := newSplitShardParam()
		coll := legacy()
		coll.VirtualChannelNames = param.Routing.GetVirtualChannelNames()
		coll.RoutingModulus = param.Routing.GetRoutingModulus()
		coll.ShardInfos = map[string]*model.ShardInfo{}
		for i, vchannel := range param.Routing.GetVirtualChannelNames() {
			info := param.Routing.GetShardInfos()[i]
			coll.ShardInfos[vchannel] = &model.ShardInfo{VChannelName: vchannel, State: info.GetState(), Buckets: info.GetHashRouting().GetBuckets()}
		}
		require.NoError(t, check(coll, param))
	})

	t.Run("a message-only refusal", func(t *testing.T) {
		param := newSplitShardParam()
		param.Routing.ShardInfos[1].State = schemapb.ShardState_ShardNormal
		requireRefused(t, check(legacy(), param), "born Creating")
	})

	t.Run("no meta, or another collection's", func(t *testing.T) {
		requireRefused(t, check(nil, newSplitShardParam()), "no collection meta")
		coll := legacy()
		coll.CollectionID = 2
		requireRefused(t, check(coll, newSplitShardParam()), "meta of collection 2")
	})

	t.Run("the source is not Normal in the meta", func(t *testing.T) {
		for _, state := range []schemapb.ShardState{schemapb.ShardState_ShardSplitting, schemapb.ShardState_ShardCreating} {
			coll := legacy()
			coll.RoutingModulus = 2
			coll.ShardInfos = map[string]*model.ShardInfo{source: {VChannelName: source, State: state, Buckets: []uint64{0, 1}}}
			err := check(coll, newSplitShardParam())
			require.ErrorIs(t, err, merr.ErrServiceInternal, state.String())
			assert.False(t, merr.IsRetryableErr(err))
		}
	})

	t.Run("a target that already exists", func(t *testing.T) {
		coll := legacy()
		coll.VirtualChannelNames = []string{source, "p0_1v1"}
		coll.ShardInfos = map[string]*model.ShardInfo{
			source:   {VChannelName: source, State: schemapb.ShardState_ShardNormal},
			"p0_1v1": {VChannelName: "p0_1v1", State: schemapb.ShardState_ShardCreating, Buckets: []uint64{0}},
		}
		requireRefused(t, check(coll, newSplitShardParam()), "already exists")
	})

	t.Run("a modulus smaller than the collection's", func(t *testing.T) {
		coll := legacy()
		coll.RoutingModulus = 4
		coll.ShardInfos = map[string]*model.ShardInfo{source: {VChannelName: source, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0, 1, 2, 3}}}
		requireRefused(t, check(coll, newSplitShardParam()), "cannot take it down to")
	})

	t.Run("a live shard the post-image forgets", func(t *testing.T) {
		coll := legacy()
		coll.VirtualChannelNames = []string{source, "p2_1v3"}
		coll.RoutingModulus = 2
		coll.ShardInfos = map[string]*model.ShardInfo{
			source:   {VChannelName: source, State: schemapb.ShardState_ShardNormal, Buckets: []uint64{0}},
			"p2_1v3": {VChannelName: "p2_1v3", State: schemapb.ShardState_ShardNormal, Buckets: []uint64{1}},
		}
		requireRefused(t, check(coll, newSplitShardParam()), "a split retires nothing")
	})

	t.Run("a listed shard the collection does not carry", func(t *testing.T) {
		// The post-image assumes a shard p2_1v3 the meta has never heard of: the
		// planner's meta lacks a split. Retriable, never a pass.
		param := newSplitShardParam()
		param.Routing.RoutingModulus = 4
		param.Routing.ShardInfos[1].GetHashRouting().Buckets = []uint64{0}
		param.Routing.ShardInfos[2].GetHashRouting().Buckets = []uint64{2}
		param.Routing.VirtualChannelNames = append(param.Routing.VirtualChannelNames, "p2_1v3")
		param.Routing.PhysicalChannelNames = append(param.Routing.PhysicalChannelNames, "p2")
		param.Routing.ShardInfos = append(param.Routing.ShardInfos, &schemapb.CollectionShardInfo{
			VchannelName: "p2_1v3",
			State:        schemapb.ShardState_ShardNormal,
			Routing:      &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: []uint64{1, 3}}},
		})
		require.NoError(t, streaming.ValidateSplitShardMessage(splitShardMessageOf(param)))
		err := check(legacy(), param)
		require.True(t, errors.Is(err, routing.ErrCommitAheadOfCollection))
		assert.True(t, merr.IsRetryableErr(err))
	})

	t.Run("genesis properties that disagree with the meta", func(t *testing.T) {
		param := newSplitShardParam()
		param.Schema.Properties = namespacePlacedProperties()
		requireRefused(t, check(legacy(), param), "disagree with the collection meta")
	})

	// Admission agrees with the meta and passes; the namespace deferral (§1.3)
	// still refuses, before the fence.
	t.Run("namespace admission against the meta", func(t *testing.T) {
		param := withNamespaceRouting(newSplitShardParam(), 16, namespacePlacedProperties())
		coll := legacy()
		coll.EnableNamespace = true
		coll.Properties = namespacePlacedProperties()
		requireRefused(t, check(coll, param), namespaceDeferral)
	})

	// The genesis says the collection is not a namespace collection, the meta
	// says it is: the meta wins and the split is refused before the fence.
	t.Run("a namespace collection whose genesis says otherwise", func(t *testing.T) {
		param := newSplitShardParam()
		require.False(t, param.Schema.GetEnableNamespace())
		require.NoError(t, streaming.ValidateSplitShardMessage(splitShardMessageOf(param)))
		coll := legacy()
		coll.EnableNamespace = true
		requireRefused(t, check(coll, param), namespaceDeferral)
		assert.ErrorContains(t, check(coll, param), "the collection meta has enable_namespace set")
	})
}

// TestSplitShardParamRefusesAControlChannelInTheWrongRole pins the three
// name-role confusions Validate must catch before the fence, because none of
// them is caught cleanly after it.
//
// The broadcast result is searched for the control-channel replica BY NAME, so
// a plain vchannel passed as the control channel yields a broadcast with no
// control-channel replica at all: the ack callback finds no tick to commit
// with, and the replica lands on a StreamingNode as a SplitShard of a vchannel
// that is neither source nor target -- unrecoverable, retried forever with the
// collection key held. A control channel named as the source used to reach
// OptBuildBroadcastAppendFirst, which panics rather than returning an error.
func TestSplitShardParamRefusesAControlChannelInTheWrongRole(t *testing.T) {
	enableShardSplit(t)

	// A plain vchannel is not a control channel under another name.
	param := newSplitShardParam()
	param.ControlChannel = "p0_1v9"
	assertSplitShardParamInvalid(t, param)
	assert.Contains(t, param.Validate().Error(), "control channel")

	// A control channel is not a shard to fence: refused, never a panic.
	param = newSplitShardParam()
	param.SourceVChannel = "p0_vcchan"
	assertSplitShardParamInvalid(t, param)
	assert.NotPanics(t, func() {
		msg, err := streaming.NewSplitShardBroadcastMessage(param)
		assert.Nil(t, msg)
		assert.Error(t, err)
	})

	// Nor a shard to create.
	param = newSplitShardParam()
	param.TargetVChannels[1] = "p1_vcchan"
	assertSplitShardParamInvalid(t, param)
	assert.Contains(t, param.Validate().Error(), "control channel")

	// The source and target checks are message-only, so the ack callback's
	// read-only re-run of the same function refuses them too.
	header := &message.SplitShardMessageHeader{
		CollectionId:    1,
		SplitTaskId:     100,
		SourceVchannel:  "p0_vcchan",
		TargetVchannels: []string{"p0_1v1", "p1_1v2"},
		PartitionIds:    []int64{10, 11},
	}
	body := &message.SplitShardMessageBody{
		Genesis: &msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "col"}},
		Routing: newSplitShardPostImage(),
	}
	err := streaming.ValidateSplitShardMessage(header, body)
	require.Error(t, err)
	assert.True(t, errors.Is(err, merr.ErrServiceInternal))
	assert.Contains(t, err.Error(), "control channel")
}
