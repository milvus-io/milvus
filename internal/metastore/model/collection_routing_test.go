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

// pbShard builds a schemapb.CollectionShardInfo owning the given residues, for
// test fixtures.
func pbShard(state schemapb.ShardState, vchannel string, buckets ...uint64) *schemapb.CollectionShardInfo {
	si := &schemapb.CollectionShardInfo{State: state, VchannelName: vchannel}
	if len(buckets) > 0 {
		si.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: buckets},
		}
	}
	return si
}

func newRoutingCollection() *Collection {
	return &Collection{
		CollectionID:         1,
		Name:                 "routing_col",
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0", "p1"},
		ShardsNum:            2,
		RoutingModulus:       2,
		ShardBy:              "hash(pk)",
		ShardInfos: map[string]*ShardInfo{
			"v0": {
				PChannelName:         "p0",
				VChannelName:         "v0",
				LastTruncateTimeTick: 7,
				State:                schemapb.ShardState_ShardSplitting,
				Buckets:              []uint64{0},
			},
			"v1": {
				PChannelName:         "p1",
				VChannelName:         "v1",
				LastTruncateTimeTick: 0,
				State:                schemapb.ShardState_ShardCreating,
				Buckets:              []uint64{1},
			},
		},
	}
}

func TestCollectionRoutingFieldsMarshalRoundTrip(t *testing.T) {
	coll := newRoutingCollection()

	collPb := MarshalCollectionModel(coll)
	assert.EqualValues(t, 2, collPb.RoutingModulus)
	assert.Equal(t, "hash(pk)", collPb.ShardBy)
	assert.Len(t, collPb.ShardInfos, 2)
	assert.Equal(t, []uint64{0}, collPb.ShardInfos[0].GetHashRouting().GetBuckets())
	assert.Equal(t, schemapb.ShardState_ShardSplitting, collPb.ShardInfos[0].State)
	assert.Equal(t, []uint64{1}, collPb.ShardInfos[1].GetHashRouting().GetBuckets())
	assert.Equal(t, schemapb.ShardState_ShardCreating, collPb.ShardInfos[1].State)
	assert.Equal(t, uint64(7), collPb.ShardInfos[0].LastTruncateTimeTick)

	restored := UnmarshalCollectionModel(collPb)
	assert.Equal(t, coll.RoutingModulus, restored.RoutingModulus)
	assert.Equal(t, coll.ShardBy, restored.ShardBy)
	assert.Equal(t, coll.ShardInfos["v0"], restored.ShardInfos["v0"])
	assert.Equal(t, coll.ShardInfos["v1"], restored.ShardInfos["v1"])
}

func TestCollectionRoutingFieldsLegacyDefaults(t *testing.T) {
	// A legacy collection persisted before shard split has neither routing fields
	// nor shard infos. Unmarshalling must leave the modulus zero and shard_by
	// empty — the two values that mean "never split, never declared" — so the
	// routing behaviour is unchanged.
	legacy := &pb.CollectionInfo{
		ID:                   2,
		Schema:               MarshalCollectionModel(newRoutingCollection()).Schema,
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
	}
	restored := UnmarshalCollectionModel(legacy)
	assert.Zero(t, restored.RoutingModulus)
	assert.Empty(t, restored.ShardBy)
	shard := restored.ShardInfos["v0"]
	assert.Equal(t, schemapb.ShardState_ShardNormal, shard.State)
	assert.Empty(t, shard.Buckets)
}

func TestCollectionRoutingFieldsClone(t *testing.T) {
	coll := newRoutingCollection()

	clone := coll.Clone()
	assert.Equal(t, coll.RoutingModulus, clone.RoutingModulus)
	assert.Equal(t, coll.ShardBy, clone.ShardBy)
	assert.Equal(t, coll.ShardInfos, clone.ShardInfos)
	shallow := coll.ShallowClone()
	assert.Equal(t, coll.RoutingModulus, shallow.RoutingModulus)
	assert.Equal(t, coll.ShardBy, shallow.ShardBy)

	// Clone must deep-copy the residues: mutating the original must not leak into
	// the clone.
	coll.ShardInfos["v0"].Buckets[0] = 9
	coll.ShardInfos["v0"].State = schemapb.ShardState_ShardDropped
	assert.Equal(t, []uint64{0}, clone.ShardInfos["v0"].Buckets)
	assert.Equal(t, schemapb.ShardState_ShardSplitting, clone.ShardInfos["v0"].State)
}

func TestApplyUpdatesShardSplitRouting(t *testing.T) {
	// A never-split single-shard collection that a split grows into three shards:
	// the source becomes Splitting and two targets are added, and the collection
	// gains the modulus its residues are read against.
	coll := &Collection{
		CollectionID:         1,
		Name:                 "col",
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
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
			RoutingModulus:       2,
			ShardBy:              "hash(pk)",
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardSplitting},
				{State: schemapb.ShardState_ShardCreating, LastTruncateTimeTick: 9, Routing: &schemapb.CollectionShardInfo_HashRouting{
					HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
				}},
				pbShard(schemapb.ShardState_ShardCreating, "", 1),
			},
		},
	}

	coll.ApplyUpdates(header, body)

	// the whole routing topology is replaced atomically.
	assert.Equal(t, []string{"v0", "v1", "v2"}, coll.VirtualChannelNames)
	assert.Equal(t, []string{"p0", "p1", "p2"}, coll.PhysicalChannelNames)
	assert.EqualValues(t, 2, coll.RoutingModulus)
	assert.Equal(t, "hash(pk)", coll.ShardBy)
	assert.Len(t, coll.ShardInfos, 3)
	// the source shard is now Splitting; the targets carry their residues.
	assert.Equal(t, schemapb.ShardState_ShardSplitting, coll.ShardInfos["v0"].State)
	assert.Empty(t, coll.ShardInfos["v0"].Buckets)
	assert.Equal(t, "p1", coll.ShardInfos["v1"].PChannelName)
	assert.Equal(t, "v1", coll.ShardInfos["v1"].VChannelName)
	assert.Equal(t, []uint64{0}, coll.ShardInfos["v1"].Buckets)
	assert.Equal(t, uint64(9), coll.ShardInfos["v1"].LastTruncateTimeTick)
	assert.Equal(t, []uint64{1}, coll.ShardInfos["v2"].Buckets)
	assert.Equal(t, schemapb.ShardState_ShardCreating, coll.ShardInfos["v2"].State)
}

// shard_by is written only when the commit carries one. A later split of a
// collection that already declared it leaves the declaration alone rather than
// clearing it, so a commit that has nothing to back-fill cannot erase the
// expression every client routes by.
func TestApplyUpdatesShardSplitRoutingKeepsExistingShardBy(t *testing.T) {
	coll := &Collection{CollectionID: 1, Name: "col", ShardBy: "hash(pk)"}
	coll.ApplyUpdates(
		&message.AlterCollectionMessageHeader{
			CollectionId: 1,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
		},
		&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				VirtualChannelNames:  []string{"v0"},
				PhysicalChannelNames: []string{"p0"},
				RoutingModulus:       1,
				ShardInfos:           []*schemapb.CollectionShardInfo{pbShard(schemapb.ShardState_ShardNormal, "v0", 0)},
			},
		})
	assert.Equal(t, "hash(pk)", coll.ShardBy)
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
			RoutingModulus:       4,
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
			RoutingModulus:       4,
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
				RoutingModulus:       6,
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

func TestShardInfoResiduesMarshalRoundTrip(t *testing.T) {
	// The residues a split commits are the routing table of the collection:
	// without them every consumer falls back to "hash % shardNum", which is
	// exactly the rule the split just invalidated. A model that drops them on the
	// way to etcd silently reverts the whole commit, so the round trip is the
	// contract.
	coll := &Collection{
		CollectionID:         1,
		Name:                 "hash_col",
		VirtualChannelNames:  []string{"v0", "v1", "v2"},
		PhysicalChannelNames: []string{"p0", "p1", "p2"},
		ShardsNum:            2,
		RoutingModulus:       4,
		ShardInfos: map[string]*ShardInfo{
			// the fenced source keeps no residue: its keys belong to the targets.
			"v0": {VChannelName: "v0", PChannelName: "p0", State: schemapb.ShardState_ShardSplitting},
			"v1": {
				VChannelName: "v1", PChannelName: "p1",
				State:   schemapb.ShardState_ShardCreating,
				Buckets: []uint64{0, 2},
			},
			"v2": {
				VChannelName: "v2", PChannelName: "p2",
				State:   schemapb.ShardState_ShardCreating,
				Buckets: []uint64{1, 3},
			},
		},
	}

	collPb := MarshalCollectionModel(coll)
	assert.EqualValues(t, 4, collPb.RoutingModulus)
	require.Len(t, collPb.ShardInfos, 3)
	assert.Nil(t, collPb.ShardInfos[0].GetHashRouting())
	assert.Equal(t, []uint64{0, 2}, collPb.ShardInfos[1].GetHashRouting().GetBuckets())
	assert.Equal(t, []uint64{1, 3}, collPb.ShardInfos[2].GetHashRouting().GetBuckets())

	back := UnmarshalCollectionModel(collPb)
	assert.Equal(t, []uint64{0, 2}, back.ShardInfos["v1"].Buckets)
	assert.Empty(t, back.ShardInfos["v0"].Buckets)
}

// ToPB must not share the residue slice with the model, or a caller mutating the
// wire form would rewrite the collection's routing in place.
func TestShardInfoToPBCopiesResidues(t *testing.T) {
	si := &ShardInfo{VChannelName: "v0", Buckets: []uint64{1, 3}}
	pbInfo := si.ToPB()
	pbInfo.GetHashRouting().Buckets[0] = 9
	assert.Equal(t, []uint64{1, 3}, si.Buckets)
}

func TestShardInfoCloneDeepCopiesResidues(t *testing.T) {
	coll := &Collection{
		VirtualChannelNames:  []string{"v0"},
		PhysicalChannelNames: []string{"p0"},
		ShardInfos: map[string]*ShardInfo{
			"v0": {VChannelName: "v0", PChannelName: "p0", Buckets: []uint64{1}},
		},
	}
	clone := coll.Clone()
	coll.ShardInfos["v0"].Buckets[0] = 0
	assert.Equal(t, []uint64{1}, clone.ShardInfos["v0"].Buckets)
}

func TestApplyUpdatesShardSplitRoutingKeepsResidues(t *testing.T) {
	// The rehash commit is delivered as an AlterCollection field-mask update.
	// This is the path the running system takes; an apply that dropped the
	// residues persisted a topology with no routing at all, and the write path
	// then silently fell back to legacy modulo routing.
	coll := &Collection{
		CollectionID:         1,
		Name:                 "col",
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0", "p1"},
		ShardInfos: map[string]*ShardInfo{
			"v0": {VChannelName: "v0", PChannelName: "p0", State: schemapb.ShardState_ShardNormal},
			"v1": {VChannelName: "v1", PChannelName: "p1", State: schemapb.ShardState_ShardNormal},
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
			RoutingModulus:       3,
			ShardInfos: []*schemapb.CollectionShardInfo{
				{State: schemapb.ShardState_ShardSplitting, VchannelName: "v0"},
				{State: schemapb.ShardState_ShardSplitting, VchannelName: "v1"},
				pbShard(schemapb.ShardState_ShardCreating, "v2", 0, 1, 2),
			},
		},
	}

	coll.ApplyUpdates(header, body)

	assert.Equal(t, []uint64{0, 1, 2}, coll.ShardInfos["v2"].Buckets)
	assert.Empty(t, coll.ShardInfos["v0"].Buckets)
	// and the residues survive the trip out to a DescribeCollection response.
	assert.Equal(t, []uint64{0, 1, 2}, coll.ShardInfos["v2"].ToPB().GetHashRouting().GetBuckets())
}

func TestShardInfoIsRoutable(t *testing.T) {
	// Normal and Creating own keys; a fenced or released split source does not.
	assert.True(t, (&ShardInfo{State: schemapb.ShardState_ShardNormal}).IsRoutable())
	assert.True(t, (&ShardInfo{State: schemapb.ShardState_ShardCreating}).IsRoutable())
	assert.False(t, (&ShardInfo{State: schemapb.ShardState_ShardSplitting}).IsRoutable())
	assert.False(t, (&ShardInfo{State: schemapb.ShardState_ShardDropped}).IsRoutable())
}

// TestUnmarshalToleratesShortPhysicalChannelList covers a malformed record on
// the meta load path. The two lists are written in lockstep, so a mismatch is a
// bug upstream -- but this runs on every start, and reading past the end would
// turn one bad record into a rootcoord that cannot be started at all, including
// to repair it.
func TestUnmarshalToleratesShortPhysicalChannelList(t *testing.T) {
	coll := UnmarshalCollectionModel(&pb.CollectionInfo{
		Schema:               &schemapb.CollectionSchema{Name: "c"},
		VirtualChannelNames:  []string{"v0", "v1"},
		PhysicalChannelNames: []string{"p0"},
	})
	assert.Equal(t, "p0", coll.ShardInfos["v0"].PChannelName)
	assert.Empty(t, coll.ShardInfos["v1"].PChannelName)
}
