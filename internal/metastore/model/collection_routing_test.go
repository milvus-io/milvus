package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
