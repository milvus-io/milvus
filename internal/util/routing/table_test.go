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
package routing

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func hashRouting(buckets ...uint64) *schemapb.CollectionShardInfo_HashRouting {
	return &schemapb.CollectionShardInfo_HashRouting{
		HashRouting: &schemapb.HashRouting{Buckets: buckets},
	}
}

// owner is the vchannel a valid table assigns to a routing value; every slot of
// a table Derive returned is filled, so an empty one fails the test.
func owner(t *testing.T, table *ResidueTable, value uint64) string {
	t.Helper()
	vchannel := table.slots[value%table.modulus]
	require.NotEmpty(t, vchannel, "value %d is unowned", value)
	return vchannel
}

func TestDeriveWithoutRoutingMetaKeepsModuloBehavior(t *testing.T) {
	// A collection that has never been split carries no residues in its meta;
	// its table is exactly hash(pk) % shardNum.
	channels := []string{"c0", "c1", "c2"}
	tbl, err := Derive(0, channels, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 3, tbl.modulus)

	for h := uint64(0); h < 300; h++ {
		assert.Equal(t, channels[h%3], owner(t, tbl, h), "hash %d", h)
	}
}

func TestDeriveWithResiduesRoutesBySlot(t *testing.T) {
	// After splitting shard 0 of a 2-shard collection: the modulus doubled to 4,
	// the two targets own {0} and {2}, and the untouched shard was rebased to
	// {1, 3}.
	tbl, err := Derive(4, []string{"c0a", "c0b", "c1"}, []Shard{
		{Vchannel: "c0a", Buckets: []uint64{0}},
		{Vchannel: "c0b", Buckets: []uint64{2}},
		{Vchannel: "c1", Buckets: []uint64{1, 3}},
	})
	require.NoError(t, err)

	for _, tc := range []struct {
		hash uint64
		want string
	}{{0, "c0a"}, {1, "c1"}, {2, "c0b"}, {3, "c1"}, {4, "c0a"}} {
		assert.Equal(t, tc.want, owner(t, tbl, tc.hash), "hash %d", tc.hash)
	}
}

func TestDeriveRejectsMalformedMeta(t *testing.T) {
	// A gap: only half the space is claimed.
	_, err := Derive(2, []string{"c0"}, []Shard{{Vchannel: "c0", Buckets: []uint64{0}}})
	require.Error(t, err)

	// Residues present but no modulus to read them against.
	_, err = Derive(0, []string{"c0"}, []Shard{{Vchannel: "c0", Buckets: []uint64{0}}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no routing modulus")
}

// The modulus is what says a collection has been split, so a modulus with no
// residues behind it is malformed meta and must be refused.
//
// Falling back to the legacy modulo here is the worst available answer: by the
// time a collection has a modulus its vchannel list has grown by the split's
// targets, so hash % len(vchannels) re-places every row in the collection.
func TestDeriveRejectsAModulusNoShardBacks(t *testing.T) {
	_, err := Derive(4, []string{"a", "b", "c"}, []Shard{
		{Vchannel: "a"}, {Vchannel: "b"}, {Vchannel: "c"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "routing modulus 4")
	assert.Contains(t, err.Error(), "no shard carries a residue")

	// And with the shards filtered away entirely, which is the same shape.
	_, err = Derive(4, []string{"a"}, nil)
	require.Error(t, err)
}

// Partial residues are already rejected by the tiling check, but the message
// should name the shard that is missing one rather than blame the modulus.
func TestDeriveRejectsResiduesOnOnlySomeShards(t *testing.T) {
	_, err := Derive(2, []string{"a", "b"}, []Shard{
		{Vchannel: "a", Buckets: []uint64{0, 1}},
		{Vchannel: "b"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `shard "b" owns no residue`)
}

func TestDeriveRejectsNoChannels(t *testing.T) {
	_, err := Derive(0, nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at least one vchannel")
}

func TestShardsFromMetaFiltersNonWritableShards(t *testing.T) {
	// A split in flight: the fenced source and a dropped one must be excluded,
	// leaving the targets as an exact cover.
	vchannels := []string{"src", "tgtA", "tgtB", "old"}
	infos := []*schemapb.CollectionShardInfo{
		{State: schemapb.ShardState_ShardSplitting, Routing: hashRouting(0, 1)},
		{State: schemapb.ShardState_ShardCreating, Routing: hashRouting(0)},
		{State: schemapb.ShardState_ShardNormal, Routing: hashRouting(1)},
		{State: schemapb.ShardState_ShardDropped},
	}
	shards, err := ShardsFromMeta(vchannels, infos)
	require.NoError(t, err)
	require.Len(t, shards, 2, "only the writable targets survive")
	assert.Equal(t, "tgtA", shards[0].Vchannel)
	assert.Equal(t, "tgtB", shards[1].Vchannel)

	// And the survivors tile the space, so Derive accepts them.
	tbl, err := Derive(2, vchannels, shards)
	require.NoError(t, err)
	assert.Equal(t, "tgtA", owner(t, tbl, 0))
	assert.Equal(t, "tgtB", owner(t, tbl, 1))
}

// A never-split collection reports every shard as ShardNormal with no routing,
// which Derive must read as "legacy modulo", not as malformed meta.
func TestShardsFromMetaOnANeverSplitCollection(t *testing.T) {
	vchannels := []string{"a", "b"}
	infos := []*schemapb.CollectionShardInfo{
		{State: schemapb.ShardState_ShardNormal},
		{State: schemapb.ShardState_ShardNormal},
	}
	shards, err := ShardsFromMeta(vchannels, infos)
	require.NoError(t, err)
	require.Len(t, shards, 2)

	tbl, err := Derive(0, vchannels, shards)
	require.NoError(t, err)
	assert.Equal(t, "b", owner(t, tbl, 3))
}

func TestShardsFromMetaRejectsLengthMismatch(t *testing.T) {
	_, err := ShardsFromMeta([]string{"a", "b"}, []*schemapb.CollectionShardInfo{{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatches")
}

// No shard info at all is the never-split shape -- the proto says so. It must
// build the legacy table, exactly as Derive(0, channels, nil) does, not be
// refused as a length mismatch.
func TestShardsFromMetaTreatsNoShardInfoAsNeverSplit(t *testing.T) {
	channels := []string{"v0", "v1"}
	shards, err := ShardsFromMeta(channels, nil)
	require.NoError(t, err)
	assert.Nil(t, shards)

	shards, err = ShardsFromMeta(channels, []*schemapb.CollectionShardInfo{})
	require.NoError(t, err)
	assert.Nil(t, shards)

	// and what it returns feeds Derive into the same table the legacy path builds.
	table, err := Derive(0, channels, shards)
	require.NoError(t, err)
	assert.EqualValues(t, 2, table.modulus)
}

// A shard the collection's own channel list does not carry is malformed meta,
// and it has to be refused at derivation, non-retriably: no refresh can clear it.
// The legacy branch has the same gap in a different shape: equal lengths are
// not the same set, and deriving from the channel list alone would hide a shard
// list that names a vchannel the collection does not carry.
func TestDeriveCompatRefusesAShardOutsideTheChannelList(t *testing.T) {
	_, err := Derive(0, []string{"v0", "v1"}, []Shard{{Vchannel: "v0"}, {Vchannel: "v9"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"v9"`)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.False(t, merr.IsRetryableErr(err))

	// same set, any order, is fine on this branch: the shards carry no residues
	// here, so their order carries no information.
	table, err := Derive(0, []string{"v0", "v1"}, []Shard{{Vchannel: "v1"}, {Vchannel: "v0"}})
	require.NoError(t, err)
	assert.Equal(t, "v0", owner(t, table, 0))
}

// A subset of the same length is not the same set. Without a duplicate check
// [v0, v0] against [v0, v1] passes the compat branch's length comparison, and
// on the explicit branch one vchannel could own two residue sets under two
// entries.
func TestDeriveRefusesADuplicatedShard(t *testing.T) {
	_, err := Derive(0, []string{"v0", "v1"}, []Shard{{Vchannel: "v0"}, {Vchannel: "v0"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "appears twice")
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.False(t, merr.IsRetryableErr(err))

	_, err = Derive(2, []string{"v0", "v1"}, []Shard{
		{Vchannel: "v0", Buckets: []uint64{0}},
		{Vchannel: "v0", Buckets: []uint64{1}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "appears twice")
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestDeriveRefusesAShardOutsideTheChannelList(t *testing.T) {
	_, err := Derive(2, []string{"v0", "v1"}, []Shard{
		{Vchannel: "v0", Buckets: []uint64{0}},
		{Vchannel: "v2", Buckets: []uint64{1}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"v2"`)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.False(t, merr.IsRetryableErr(err), "malformed meta must not be retried")
}

// The never-split table is keyed on the vchannel list's order, so a list that
// names no vchannel at some position, or one vchannel twice, has no valid
// assignment.
func TestDeriveCompatRefusesAMalformedChannelList(t *testing.T) {
	_, err := Derive(0, []string{"v0", ""}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "carries no name")

	_, err = Derive(0, []string{"v0", "v0"}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "appears twice")
}

// A shard state a newer server knows and this build does not may own keys.
// Dropping the entry would re-route whatever it owned without saying so, so the
// whole table is refused instead.
func TestShardsFromMetaRejectsAnUnknownShardState(t *testing.T) {
	infos := []*schemapb.CollectionShardInfo{
		{State: schemapb.ShardState(999), Routing: hashRouting(0)},
		{State: schemapb.ShardState_ShardNormal, Routing: hashRouting(1)},
	}
	_, err := ShardsFromMeta([]string{"a", "b"}, infos)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `shard "a" reports shard state 999`)
}

// The legacy rule is "shard i owns residue i at modulus len(channels)", so a
// shorter shard list means some vchannel was declared non-writable while the
// collection reports no modulus. Real meta cannot be in that shape -- retiring a
// shard is what writes residues in the first place -- and deriving anyway would
// route the excluded shard's residue straight back to it.
func TestDeriveRefusesAShortShardListWithoutAModulus(t *testing.T) {
	_, err := Derive(0, []string{"v0", "v1", "v2"}, []Shard{{Vchannel: "v0"}, {Vchannel: "v1"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "only 2 of 3")

	// No shard info at all is the ordinary never-split case and stays legal.
	_, err = Derive(0, []string{"v0", "v1", "v2"}, nil)
	require.NoError(t, err)
}

// The vchannel list only ever grows: a split retires its source but keeps the
// name. The retired vchannels own no key, and the table built over the rest
// still tiles.
func TestDeriveOverAListWithRetiredShards(t *testing.T) {
	vchannels := []string{"v0", "v1", "v2", "v3"}
	infos := []*schemapb.CollectionShardInfo{
		{VchannelName: "v0", State: schemapb.ShardState_ShardDropped},
		{VchannelName: "v1", State: schemapb.ShardState_ShardSplitting},
		{VchannelName: "v2", State: schemapb.ShardState_ShardNormal, Routing: hashRouting(0)},
		{VchannelName: "v3", State: schemapb.ShardState_ShardNormal, Routing: hashRouting(1)},
	}
	shards, err := ShardsFromMeta(vchannels, infos)
	require.NoError(t, err)
	require.Len(t, shards, 2)

	table, err := Derive(2, vchannels, shards)
	require.NoError(t, err)
	assert.Equal(t, []string{"v2", "v3"}, table.slots, "the two retired vchannels own no key")
}

// A Creating shard is admitted so a split's targets are write-routable from the
// routing commit onward. That rests on the commit publishing the entry WITH its
// residues; one published without them takes the whole collection down rather
// than only that shard, so the refusal has to name it.
func TestShardsFromMetaOnACreatingShardWithoutResidues(t *testing.T) {
	vchannels := []string{"v0", "v1"}
	infos := []*schemapb.CollectionShardInfo{
		{VchannelName: "v0", State: schemapb.ShardState_ShardNormal, Routing: hashRouting(0, 1)},
		{VchannelName: "v1", State: schemapb.ShardState_ShardCreating},
	}
	shards, err := ShardsFromMeta(vchannels, infos)
	require.NoError(t, err, "ShardsFromMeta admits it; Derive is where it is caught")

	_, err = Derive(2, vchannels, shards)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"v1"`)
	assert.Contains(t, err.Error(), "owns no residue")
}

// A permuted infos array is the one malformed shape the tiling check accepts:
// the residues still tile [0, M) exactly while every residue ends up bound to a
// shard that does not own it. The name is the only signal.
func TestShardsFromMetaRefusesInfosOutOfOrder(t *testing.T) {
	vchannels := []string{"v0", "v1"}
	shardInfo := func(name string, residue uint64) *schemapb.CollectionShardInfo {
		return &schemapb.CollectionShardInfo{
			VchannelName: name,
			State:        schemapb.ShardState_ShardNormal,
			Routing:      hashRouting(residue),
		}
	}

	// In order: accepted, and the residues land where the names say.
	shards, err := ShardsFromMeta(vchannels, []*schemapb.CollectionShardInfo{
		shardInfo("v0", 0), shardInfo("v1", 1),
	})
	require.NoError(t, err)
	require.Len(t, shards, 2)
	assert.Equal(t, []uint64{0}, shards[0].Buckets)

	// Swapped: same length, still disjoint, still covering. Refused only because
	// the names disagree with the positions.
	_, err = ShardsFromMeta(vchannels, []*schemapb.CollectionShardInfo{
		shardInfo("v1", 1), shardInfo("v0", 0),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "names vchannel")

	// A permutation that moves a retired entry onto a live vchannel is caught by
	// the same check, before the state is even read.
	_, err = ShardsFromMeta(vchannels, []*schemapb.CollectionShardInfo{
		{VchannelName: "v1", State: schemapb.ShardState_ShardDropped},
		shardInfo("v0", 0),
	})
	require.Error(t, err)

	// A collection persisted before the field existed carries no name, and must
	// keep working on positional alignment alone.
	shards, err = ShardsFromMeta(vchannels, []*schemapb.CollectionShardInfo{
		{State: schemapb.ShardState_ShardNormal, Routing: hashRouting(0)},
		{State: schemapb.ShardState_ShardNormal, Routing: hashRouting(1)},
	})
	require.NoError(t, err)
	assert.Len(t, shards, 2)
}

// The legacy table is a residue table too, so it is bound by the same modulus
// cap and a collection with an absurd vchannel list is refused rather than
// allocating for it.
func TestDeriveRejectsMoreChannelsThanTheModulusCap(t *testing.T) {
	channels := make([]string, maxModulus+1)
	for i := range channels {
		channels[i] = "v" + strconv.Itoa(i)
	}
	_, err := Derive(0, channels, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds the cap")
}
