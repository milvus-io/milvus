// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package routing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func hashRouting(buckets ...uint64) *schemapb.CollectionShardInfo_HashRouting {
	return &schemapb.CollectionShardInfo_HashRouting{
		HashRouting: &schemapb.HashRouting{Buckets: buckets},
	}
}

func TestDeriveWithoutRoutingMetaKeepsModuloBehavior(t *testing.T) {
	// A collection that has never been split carries no residues in its meta;
	// it must route exactly like hash(pk) % shardNum.
	channels := []string{"c0", "c1", "c2"}
	tbl, err := Derive(0, channels, nil)
	require.NoError(t, err)
	assert.False(t, tbl.IsExplicit())
	assert.Equal(t, 3, tbl.NumShards())

	for h := uint64(0); h < 300; h++ {
		ch, err := tbl.Route(h)
		require.NoError(t, err)
		assert.Equal(t, channels[h%3], ch, "hash %d", h)
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
	assert.True(t, tbl.IsExplicit())
	assert.Equal(t, 3, tbl.NumShards())

	for _, tc := range []struct {
		hash uint64
		want string
	}{{0, "c0a"}, {1, "c1"}, {2, "c0b"}, {3, "c1"}, {4, "c0a"}} {
		ch, err := tbl.Route(tc.hash)
		require.NoError(t, err)
		assert.Equal(t, tc.want, ch, "hash %d", tc.hash)
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
// targets, so hash % len(vchannels) re-places every row in the collection —
// some onto a fenced source that rejects them, the rest onto targets that never
// received their data. This is also the check the split's own commit gate leans
// on, and it used to pass the largest possible gap.
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

func TestRouteOnANilTableErrors(t *testing.T) {
	var tbl *Table
	_, err := tbl.Route(1)
	require.Error(t, err)
	assert.False(t, tbl.IsExplicit())
	// The predicates stay nil-safe rather than panicking half way through a
	// caller that is already handling a derivation failure.
	assert.Equal(t, 0, tbl.NumShards())
	assert.True(t, tbl.RoutesByPrimaryKey())
}

// A partial table can be reached only through a plan that does not tile the key
// space, which Derive rejects. Route still has to answer rather than guess, so
// the unowned residue surfaces as an error.
func TestRouteErrorsOnAnUnownedResidue(t *testing.T) {
	rt, err := DeriveHashPartial(4, []HashShard{{Vchannel: "left", Buckets: []uint64{0}}})
	require.NoError(t, err)
	tbl := &Table{residues: rt, channels: []string{"left"}, explicit: true}

	ch, err := tbl.Route(0)
	require.NoError(t, err)
	assert.Equal(t, "left", ch)

	_, err = tbl.Route(1)
	require.Error(t, err)
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
	ch, err := tbl.Route(0)
	require.NoError(t, err)
	assert.Equal(t, "tgtA", ch)
	ch, err = tbl.Route(1)
	require.NoError(t, err)
	assert.Equal(t, "tgtB", ch)
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
	assert.False(t, tbl.IsExplicit())
	ch, err := tbl.Route(3)
	require.NoError(t, err)
	assert.Equal(t, "b", ch)
}

func TestShardsFromMetaRejectsLengthMismatch(t *testing.T) {
	_, err := ShardsFromMeta([]string{"a", "b"}, []*schemapb.CollectionShardInfo{{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatches")
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

func TestParseShardBy(t *testing.T) {
	cases := []struct {
		expr  string
		field string
		ok    bool
	}{
		{"", "", true}, // never declared: the primary key applies
		{"hash(pk)", "pk", true},
		{"hash($namespace_id)", NamespaceIDField, true},
		{"hash(a)b)", "", false},     // bytes after the closing parenthesis
		{"hash(pk", "", false},       // unterminated
		{"HASH(pk)", "", false},      // no case folding
		{"hash (pk)", "", false},     // no space
		{"hash('pk')", "'pk'", true}, // taken whole, not unquoted
		{"hash()", "", false},        // names no field
		{"pk", "", false},
		{"murmur(pk)", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			field, err := ParseShardBy(tc.expr)
			if !tc.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.field, field)
		})
	}
}

func TestWithShardByRejectsAMalformedExpression(t *testing.T) {
	_, err := Derive(0, []string{"a"}, nil, WithShardBy("hash(pk", "pk"))
	require.Error(t, err)
}

func TestRoutesByPrimaryKey(t *testing.T) {
	cases := []struct {
		name    string
		shardBy string
		pkField string
		want    bool
	}{
		{"undeclared", "", "pk", true},
		{"names the primary key", "hash(pk)", "pk", true},
		{"names the namespace", "hash($namespace_id)", "pk", false},
		{"names another field", "hash(tenant)", "pk", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tbl, err := Derive(0, []string{"a", "b"}, nil, WithShardBy(tc.shardBy, tc.pkField))
			require.NoError(t, err)
			assert.Equal(t, tc.want, tbl.RoutesByPrimaryKey())
		})
	}
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
	table, err := Derive(0, []string{"v0", "v1", "v2"}, nil)
	require.NoError(t, err)
	assert.False(t, table.IsExplicit())
}

// The vchannel list only ever grows: a split retires its source but keeps the
// name. NumShards has to answer with the shards that actually own keys, or every
// caller sizing anything by it drifts upward with each split.
func TestNumShardsCountsTheShardsThatOwnResidues(t *testing.T) {
	// Four vchannels, two of them retired by an earlier split.
	vchannels := []string{"v0", "v1", "v2", "v3"}
	infos := []*schemapb.CollectionShardInfo{
		{VchannelName: "v0", State: schemapb.ShardState_ShardDropped},
		{VchannelName: "v1", State: schemapb.ShardState_ShardSplitting},
		{VchannelName: "v2", State: schemapb.ShardState_ShardNormal, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
		}},
		{VchannelName: "v3", State: schemapb.ShardState_ShardNormal, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{1}},
		}},
	}
	shards, err := ShardsFromMeta(vchannels, infos)
	require.NoError(t, err)
	require.Len(t, shards, 2)

	table, err := Derive(2, vchannels, shards)
	require.NoError(t, err)
	assert.Equal(t, 2, table.NumShards(), "the two retired vchannels own no key")

	// A collection that has never split has no residues, and there the channel
	// count IS the shard count.
	legacy, err := Derive(0, vchannels, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, legacy.NumShards())
}

// A Creating shard is admitted so a split's targets are write-routable from the
// routing commit onward. That rests on the commit publishing the entry WITH its
// residues; one published without them takes the whole collection down rather
// than only that shard, so the refusal has to name it.
func TestShardsFromMetaOnACreatingShardWithoutResidues(t *testing.T) {
	vchannels := []string{"v0", "v1"}
	infos := []*schemapb.CollectionShardInfo{
		{VchannelName: "v0", State: schemapb.ShardState_ShardNormal, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{0, 1}},
		}},
		{VchannelName: "v1", State: schemapb.ShardState_ShardCreating},
	}
	shards, err := ShardsFromMeta(vchannels, infos)
	require.NoError(t, err, "ShardsFromMeta admits it; Derive is where it is caught")

	_, err = Derive(2, vchannels, shards)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"v1"`)
	assert.Contains(t, err.Error(), "owns no residue")
}

// A permuted infos array is the one malformed shape every check downstream
// accepts: the residues still tile [0, M) exactly, so Derive, DeriveHash and
// DeriveHashPartial are all satisfied while every residue ends up bound to a
// shard that does not own it. Inserts land on the wrong shard, deletes match
// nothing, and nothing raises an error. The name is the only signal.
func TestShardsFromMetaRefusesInfosOutOfOrder(t *testing.T) {
	vchannels := []string{"v0", "v1"}
	shardInfo := func(name string, residue uint64) *schemapb.CollectionShardInfo {
		return &schemapb.CollectionShardInfo{
			VchannelName: name,
			State:        schemapb.ShardState_ShardNormal,
			Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{residue}},
			},
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
		{State: schemapb.ShardState_ShardNormal, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
		}},
		{State: schemapb.ShardState_ShardNormal, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{1}},
		}},
	})
	require.NoError(t, err)
	assert.Len(t, shards, 2)
}

// The never-split convention has exactly one implementation, and it is keyed on
// the vchannel list's order -- a caller enumerating from a map would produce a
// permutation of it that nothing downstream can detect.
func TestLegacyShardsIsTheOneImplementationOfTheConvention(t *testing.T) {
	vchannels := []string{"v0", "v1", "v2"}
	modulus, shards, err := LegacyShards(vchannels)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), modulus)
	require.Len(t, shards, 3)
	for i, s := range shards {
		assert.Equal(t, vchannels[i], s.Vchannel)
		assert.Equal(t, []uint64{uint64(i)}, s.Buckets)
	}

	// It agrees with what Derive builds for the same collection, which is the
	// property that makes it safe to plan a first split from.
	table, err := Derive(0, vchannels, nil)
	require.NoError(t, err)
	for _, s := range shards {
		got, err := table.Route(s.Buckets[0])
		require.NoError(t, err)
		assert.Equal(t, s.Vchannel, got)
	}

	_, _, err = LegacyShards(nil)
	assert.Error(t, err)
	_, _, err = LegacyShards([]string{"v0", ""})
	assert.Error(t, err)
	_, _, err = LegacyShards([]string{"v0", "v0"})
	assert.Error(t, err)
}
