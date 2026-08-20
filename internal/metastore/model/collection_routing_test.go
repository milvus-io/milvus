package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// pbRangeShard builds a range-routed schemapb.CollectionShardInfo owning a
// single [lower, upper) range, for test fixtures.
func pbRangeShard(state schemapb.ShardState, lastTruncate uint64, lower, upper []byte) *schemapb.CollectionShardInfo {
	return &schemapb.CollectionShardInfo{
		LastTruncateTimeTick: lastTruncate,
		State:                state,
		Routing: &schemapb.CollectionShardInfo_RangeRouting{
			RangeRouting: &schemapb.RangeRouting{Ranges: []*schemapb.RoutingKeyRange{{Lower: lower, Upper: upper}}},
		},
	}
}

func newRoutingCollection() *Collection {
	return &Collection{
		CollectionID:         1,
		Name:                 "routing_col",
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0", "p1"},
		ShardsNum:            2,
		RoutingMode:          schemapb.RoutingMode_RoutingModeRange,
		ShardInfos: map[string]*ShardInfo{
			"v0": {
				PChannelName:         "p0",
				VChannelName:         "v0",
				LastTruncateTimeTick: 7,
				State:                schemapb.ShardState_ShardSplitting,
				Ranges:               []RoutingKeyRange{{Lower: nil, Upper: []byte{0x80}}},
			},
			"v1": {
				PChannelName:         "p1",
				VChannelName:         "v1",
				LastTruncateTimeTick: 0,
				State:                schemapb.ShardState_ShardCreating,
				Ranges:               []RoutingKeyRange{{Lower: []byte{0x80}, Upper: nil}},
			},
		},
	}
}

func TestCollectionRoutingFieldsMarshalRoundTrip(t *testing.T) {
	coll := newRoutingCollection()

	collPb := MarshalCollectionModel(coll)
	assert.Equal(t, schemapb.RoutingMode_RoutingModeRange, collPb.RoutingMode)
	assert.Len(t, collPb.ShardInfos, 2)
	assert.Equal(t, []byte{0x80}, collPb.ShardInfos[0].GetRangeRouting().GetRanges()[0].GetUpper())
	assert.Equal(t, schemapb.ShardState_ShardSplitting, collPb.ShardInfos[0].State)
	assert.Equal(t, []byte{0x80}, collPb.ShardInfos[1].GetRangeRouting().GetRanges()[0].GetLower())
	assert.Equal(t, schemapb.ShardState_ShardCreating, collPb.ShardInfos[1].State)
	assert.Equal(t, uint64(7), collPb.ShardInfos[0].LastTruncateTimeTick)

	restored := UnmarshalCollectionModel(collPb)
	assert.Equal(t, coll.RoutingMode, restored.RoutingMode)
	assert.Equal(t, coll.ShardInfos["v0"], restored.ShardInfos["v0"])
	assert.Equal(t, coll.ShardInfos["v1"], restored.ShardInfos["v1"])
}

func TestCollectionRoutingFieldsLegacyDefaults(t *testing.T) {
	// A legacy collection persisted before shard split has neither
	// routing fields nor shard infos; unmarshalling must yield the
	// hash-mode defaults so that the routing behavior is unchanged.
	legacy := &pb.CollectionInfo{
		ID:                   2,
		Schema:               MarshalCollectionModel(newRoutingCollection()).Schema,
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
	}
	restored := UnmarshalCollectionModel(legacy)
	assert.Equal(t, schemapb.RoutingMode_RoutingModeHash, restored.RoutingMode)
	shard := restored.ShardInfos["v0"]
	assert.Equal(t, schemapb.ShardState_ShardNormal, shard.State)
	assert.Nil(t, shard.Ranges)
}

func TestCollectionRoutingFieldsClone(t *testing.T) {
	coll := newRoutingCollection()

	clone := coll.Clone()
	assert.Equal(t, coll.RoutingMode, clone.RoutingMode)
	assert.Equal(t, coll.ShardInfos, clone.ShardInfos)
	shallow := coll.ShallowClone()
	assert.Equal(t, coll.RoutingMode, shallow.RoutingMode)

	// Clone must deep-copy the routing key bytes: mutating the
	// original must not leak into the clone.
	coll.ShardInfos["v0"].Ranges[0].Upper[0] = 0xff
	coll.ShardInfos["v0"].State = schemapb.ShardState_ShardDropped
	assert.Equal(t, []byte{0x80}, clone.ShardInfos["v0"].Ranges[0].Upper)
	assert.Equal(t, schemapb.ShardState_ShardSplitting, clone.ShardInfos["v0"].State)
}

func TestApplyUpdatesShardSplitRouting(t *testing.T) {
	// A legacy single-shard hash collection that a split grows into three
	// shards: the source becomes Splitting, two range targets are added.
	coll := &Collection{
		CollectionID:         1,
		Name:                 "col",
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
		RoutingMode:          schemapb.RoutingMode_RoutingModeHash,
		ShardInfos: map[string]*ShardInfo{
			"v0": {VChannelName: "v0", PChannelName: "p0", State: schemapb.ShardState_ShardNormal},
		},
	}

	header := &message.AlterCollectionMessageHeader{
		CollectionId: 1,
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
	}
	body := &message.AlterCollectionMessageBody{
		Updates: &message.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{"v0", "v1", "v2"},
			PhysicalChannelNames: []string{"p0", "p1", "p2"},
			RoutingMode:          schemapb.RoutingMode_RoutingModeRange,
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardSplitting},
				pbRangeShard(schemapb.ShardState_ShardCreating, 9, nil, []byte{0x80}),
				pbRangeShard(schemapb.ShardState_ShardCreating, 0, []byte{0x80}, nil),
			},
		},
	}

	coll.ApplyUpdates(header, body)

	// the whole routing topology is replaced atomically.
	assert.Equal(t, []string{"v0", "v1", "v2"}, coll.VirtualChannelNames)
	assert.Equal(t, []string{"p0", "p1", "p2"}, coll.PhysicalChannelNames)
	assert.Equal(t, schemapb.RoutingMode_RoutingModeRange, coll.RoutingMode)
	assert.Len(t, coll.ShardInfos, 3)
	// the source shard is now Splitting; the targets carry their ranges.
	assert.Equal(t, schemapb.ShardState_ShardSplitting, coll.ShardInfos["v0"].State)
	assert.Empty(t, coll.ShardInfos["v0"].Ranges)
	assert.Equal(t, "p1", coll.ShardInfos["v1"].PChannelName)
	assert.Equal(t, "v1", coll.ShardInfos["v1"].VChannelName)
	assert.Equal(t, []byte{0x80}, coll.ShardInfos["v1"].Ranges[0].Upper)
	assert.Equal(t, uint64(9), coll.ShardInfos["v1"].LastTruncateTimeTick)
	assert.Equal(t, []byte{0x80}, coll.ShardInfos["v2"].Ranges[0].Lower)
	assert.Equal(t, schemapb.ShardState_ShardCreating, coll.ShardInfos["v2"].State)
}

func TestApplyUpdatesShardSplitRoutingSetsShardsNum(t *testing.T) {
	// ShardsNum is what a user sees as "how many shards does this collection
	// have". It is a field of its own, so a routing commit that did not write it
	// left a split collection reporting its pre-split count. For a rehash — whose
	// whole purpose is to change that number — it is the operation's semantics.
	//
	// The value is the count of ROUTABLE shards, not of vchannels: during the
	// window the retired sources are still listed, and counting them would report
	// a number the collection never actually has.
	coll := &Collection{
		CollectionID:         1,
		Name:                 "col",
		ShardsNum:            3,
		VirtualChannelNames:  []string{"v0", "v1", "v2"},
		PhysicalChannelNames: []string{"p0", "p1", "p2"},
		RoutingMode:          schemapb.RoutingMode_RoutingModeHash,
	}
	header := &message.AlterCollectionMessageHeader{
		CollectionId: 1,
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
	}

	// Mid-rehash 3 -> 4: every source is fenced, four targets are routable. Seven
	// vchannels are listed, but the collection has four shards.
	coll.ApplyUpdates(header, &message.AlterCollectionMessageBody{
		Updates: &message.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{"v0", "v1", "v2", "t0", "t1", "t2", "t3"},
			PhysicalChannelNames: []string{"p0", "p1", "p2", "p3", "p4", "p5", "p6"},
			RoutingMode:          schemapb.RoutingMode_RoutingModeHash,
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardSplitting},
				{State: schemapb.ShardState_ShardSplitting},
				{State: schemapb.ShardState_ShardSplitting},
				{State: schemapb.ShardState_ShardCreating},
				{State: schemapb.ShardState_ShardCreating},
				{State: schemapb.ShardState_ShardCreating},
				{State: schemapb.ShardState_ShardCreating},
			},
		},
	})
	assert.Equal(t, int32(4), coll.ShardsNum)
	assert.Len(t, coll.VirtualChannelNames, 7)

	// After adoption the sources are Dropped and the count is unchanged.
	coll.ApplyUpdates(header, &message.AlterCollectionMessageBody{
		Updates: &message.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{"v0", "v1", "v2", "t0", "t1", "t2", "t3"},
			PhysicalChannelNames: []string{"p0", "p1", "p2", "p3", "p4", "p5", "p6"},
			RoutingMode:          schemapb.RoutingMode_RoutingModeHash,
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardDropped},
				{State: schemapb.ShardState_ShardDropped},
				{State: schemapb.ShardState_ShardDropped},
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardNormal},
				{State: schemapb.ShardState_ShardNormal},
			},
		},
	})
	assert.Equal(t, int32(4), coll.ShardsNum)
}

func TestApplyUpdatesShardSplitRoutingShardsNumAfterDoubling(t *testing.T) {
	// A doubling: one source fenced, two targets. 3 shards become 4.
	coll := &Collection{CollectionID: 1, Name: "col", ShardsNum: 3}
	coll.ApplyUpdates(
		&message.AlterCollectionMessageHeader{
			CollectionId: 1,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
		},
		&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				VirtualChannelNames:  []string{"v0", "v1", "v2", "t0", "t1"},
				PhysicalChannelNames: []string{"p0", "p1", "p2", "p3", "p4"},
				RoutingMode:          schemapb.RoutingMode_RoutingModeHash,
				ShardInfos: []*schemapb.CollectionShardInfo{
					{State: schemapb.ShardState_ShardSplitting},
					{State: schemapb.ShardState_ShardNormal},
					{State: schemapb.ShardState_ShardNormal},
					{State: schemapb.ShardState_ShardCreating},
					{State: schemapb.ShardState_ShardCreating},
				},
			},
		})
	assert.Equal(t, int32(4), coll.ShardsNum)
}

// pbHashShard builds a hash-routed schemapb.CollectionShardInfo owning a single
// {modulus, remainder} bucket, for test fixtures.
func pbHashShard(state schemapb.ShardState, vchannel string, modulus, remainder uint64) *schemapb.CollectionShardInfo {
	return &schemapb.CollectionShardInfo{
		State:        state,
		VchannelName: vchannel,
		Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{
				Buckets: []*schemapb.HashBucket{{Modulus: modulus, Remainder: remainder}},
			},
		},
	}
}

func TestShardInfoHashRoutingMarshalRoundTrip(t *testing.T) {
	// The hash buckets a split commits are the routing table of a primary-key
	// collection: without them every consumer falls back to "hash(pk) %
	// shardNum", which is exactly the rule the split just invalidated. A model
	// that drops them on the way to etcd silently reverts the whole commit, so
	// the round trip is the contract.
	coll := &Collection{
		CollectionID:         1,
		Name:                 "hash_col",
		VirtualChannelNames:  []string{"v0", "v1", "v2"},
		PhysicalChannelNames: []string{"p0", "p1", "p2"},
		ShardsNum:            2,
		RoutingMode:          schemapb.RoutingMode_RoutingModeHash,
		ShardInfos: map[string]*ShardInfo{
			// the fenced source keeps no bucket: its keys belong to the targets.
			"v0": {VChannelName: "v0", PChannelName: "p0", State: schemapb.ShardState_ShardSplitting},
			"v1": {
				VChannelName: "v1", PChannelName: "p1",
				State:                  schemapb.ShardState_ShardCreating,
				SourceVChannels:        []string{"v0"},
				FrontingSourceVChannel: "v0",
				Buckets:                []HashBucket{{Modulus: 4, Remainder: 0}, {Modulus: 4, Remainder: 2}},
			},
			"v2": {
				VChannelName: "v2", PChannelName: "p2",
				State:                  schemapb.ShardState_ShardCreating,
				SourceVChannels:        []string{"v0"},
				FrontingSourceVChannel: "v0",
				Buckets:                []HashBucket{{Modulus: 4, Remainder: 1}, {Modulus: 4, Remainder: 3}},
			},
		},
	}

	collPb := MarshalCollectionModel(coll)
	assert.Equal(t, schemapb.RoutingMode_RoutingModeHash, collPb.RoutingMode)
	require.Len(t, collPb.ShardInfos, 3)
	assert.Nil(t, collPb.ShardInfos[0].GetHashRouting())
	assert.Equal(t, []*schemapb.HashBucket{{Modulus: 4, Remainder: 0}, {Modulus: 4, Remainder: 2}},
		collPb.ShardInfos[1].GetHashRouting().GetBuckets())
	assert.Equal(t, "v0", collPb.ShardInfos[1].GetFrontingSourceVchannel())
	assert.Equal(t, []string{"v0"}, collPb.ShardInfos[2].GetSourceVchannels())

	back := UnmarshalCollectionModel(collPb)
	assert.Equal(t, []HashBucket{{Modulus: 4, Remainder: 0}, {Modulus: 4, Remainder: 2}},
		back.ShardInfos["v1"].Buckets)
	assert.Equal(t, "v0", back.ShardInfos["v1"].FrontingSourceVChannel)
	assert.Empty(t, back.ShardInfos["v0"].Buckets)
	assert.Empty(t, back.ShardInfos["v0"].FrontingSourceVChannel)
}

func TestShardInfoToPBPrefersRangesOverBuckets(t *testing.T) {
	// The wire form is a oneof, so a shard cannot carry both predicates. A shard
	// that somehow holds both is a bug upstream; picking deterministically keeps
	// it from depending on map order.
	si := &ShardInfo{
		VChannelName: "v0",
		Ranges:       []RoutingKeyRange{{Lower: nil, Upper: []byte{0x80}}},
		Buckets:      []HashBucket{{Modulus: 2, Remainder: 0}},
	}
	pbInfo := si.ToPB()
	assert.NotNil(t, pbInfo.GetRangeRouting())
	assert.Nil(t, pbInfo.GetHashRouting())
}

func TestShardInfoCloneDeepCopiesBuckets(t *testing.T) {
	coll := &Collection{
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
		ShardInfos: map[string]*ShardInfo{
			"v0": {
				VChannelName:           "v0",
				PChannelName:           "p0",
				FrontingSourceVChannel: "src",
				Buckets:                []HashBucket{{Modulus: 2, Remainder: 1}},
			},
		},
	}
	clone := coll.Clone()
	coll.ShardInfos["v0"].Buckets[0].Remainder = 0
	coll.ShardInfos["v0"].FrontingSourceVChannel = ""
	assert.Equal(t, uint64(1), clone.ShardInfos["v0"].Buckets[0].Remainder)
	assert.Equal(t, "src", clone.ShardInfos["v0"].FrontingSourceVChannel)
}

func TestApplyUpdatesShardSplitRoutingKeepsHashBuckets(t *testing.T) {
	// The rehash commit is delivered as an AlterCollection field-mask update.
	// This is the path the running system takes; an apply that read only the
	// range variant persisted a topology with no hash predicate at all, and the
	// write path then silently fell back to legacy modulo routing.
	coll := &Collection{
		CollectionID:         1,
		Name:                 "col",
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0", "p1"},
		RoutingMode:          schemapb.RoutingMode_RoutingModeHash,
		ShardInfos: map[string]*ShardInfo{
			"v0": {VChannelName: "v0", PChannelName: "p0", State: schemapb.ShardState_ShardNormal},
			"v1": {VChannelName: "v1", PChannelName: "p1", State: schemapb.ShardState_ShardNormal},
		},
	}

	header := &message.AlterCollectionMessageHeader{
		CollectionId: 1,
		UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
	}
	target := pbHashShard(schemapb.ShardState_ShardCreating, "v2", 3, 0)
	target.SourceVchannels = []string{"v0", "v1"}
	target.FrontingSourceVchannel = "v0"
	body := &message.AlterCollectionMessageBody{
		Updates: &message.AlterCollectionMessageUpdates{
			VirtualChannelNames:  []string{"v0", "v1", "v2"},
			PhysicalChannelNames: []string{"p0", "p1", "p2"},
			RoutingMode:          schemapb.RoutingMode_RoutingModeHash,
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardSplitting, VchannelName: "v0"},
				{State: schemapb.ShardState_ShardSplitting, VchannelName: "v1"},
				target,
			},
		},
	}

	coll.ApplyUpdates(header, body)

	assert.Equal(t, []HashBucket{{Modulus: 3, Remainder: 0}}, coll.ShardInfos["v2"].Buckets)
	assert.Equal(t, []string{"v0", "v1"}, coll.ShardInfos["v2"].SourceVChannels)
	assert.Equal(t, "v0", coll.ShardInfos["v2"].FrontingSourceVChannel)
	assert.Empty(t, coll.ShardInfos["v0"].Buckets)
	// and the buckets survive the trip out to a DescribeCollection response.
	assert.Equal(t, uint64(3), coll.ShardInfos["v2"].ToPB().GetHashRouting().GetBuckets()[0].GetModulus())
}
