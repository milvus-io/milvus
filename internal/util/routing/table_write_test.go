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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func pkIDs(pks ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}}
}

// doubledTable is the topology a doubling split leaves behind: the collection
// modulus is 4, the untouched shard was rebased onto {1,3}, and the two targets
// carved from the old {0} own {0} and {2}. The fenced source is already filtered
// out by ShardsFromMeta.
func doubledTable(t *testing.T) (*Table, []string) {
	channels := []string{"v1", "v2", "v3"}
	table, err := Derive(4, channels, []Shard{
		{Vchannel: "v1", Buckets: []uint64{1, 3}},
		{Vchannel: "v2", Buckets: []uint64{0}},
		{Vchannel: "v3", Buckets: []uint64{2}},
	})
	require.NoError(t, err)
	require.True(t, table.IsExplicit())
	return table, channels
}

func TestRouteInsertFollowsBucketsNotPosition(t *testing.T) {
	// The reason the write path cannot keep using a modulo over the channel
	// list: after a doubling the modulus no longer equals the shard count, so
	// routing by position sends keys to shards that do not own them.
	table, channels := doubledTable(t)
	pks := make([]int64, 0, 200)
	for i := int64(0); i < 200; i++ {
		pks = append(pks, i)
	}

	offsets, hashValues, err := table.RouteInsert(pkIDs(pks...), channels)
	require.NoError(t, err)
	require.Len(t, hashValues, len(pks))

	placed := 0
	for vchannel, rows := range offsets {
		placed += len(rows)
		for _, row := range rows {
			h, err := typeutil.Hash32Int64(pks[row])
			require.NoError(t, err)
			switch vchannel {
			case "v1":
				assert.Contains(t, []uint32{1, 3}, h%4, "v1 owns {1,3} mod 4")
			case "v2":
				assert.Equal(t, uint32(0), h%4, "v2 owns {0} mod 4")
			case "v3":
				assert.Equal(t, uint32(2), h%4, "v3 owns {2} mod 4")
			default:
				t.Fatalf("row routed to unknown shard %q", vchannel)
			}
		}
	}
	assert.Equal(t, len(pks), placed, "every row is placed exactly once")
}

func TestRouteInsertDisagreesWithTheLegacyModulo(t *testing.T) {
	// Guards the test above against being vacuous: if residue routing happened
	// to agree with hash%3 there would be nothing to fix.
	table, channels := doubledTable(t)
	pks := make([]int64, 0, 200)
	for i := int64(0); i < 200; i++ {
		pks = append(pks, i)
	}

	byBuckets, _, err := table.RouteInsert(pkIDs(pks...), channels)
	require.NoError(t, err)
	byPosition, _ := DeriveCompat(channels).RouteInsert(pkIDs(pks...))
	assert.NotEqual(t, byPosition, byBuckets)
}

func TestRouteInsertKeepsLegacyBitForBit(t *testing.T) {
	// A collection that has never been split carries no residues, and its
	// routing must not move by so much as one row.
	channels := []string{"v0", "v1", "v2"}
	table, err := Derive(0, channels, []Shard{
		{Vchannel: "v0"}, {Vchannel: "v1"}, {Vchannel: "v2"},
	})
	require.NoError(t, err)
	require.False(t, table.IsExplicit())

	pks := pkIDs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	got, gotHashes, err := table.RouteInsert(pks, channels)
	require.NoError(t, err)
	want, wantHashes := DeriveCompat(channels).RouteInsert(pks)
	assert.Equal(t, want, got)
	assert.Equal(t, wantHashes, gotHashes)
}

func TestRouteInsertOnANilTableIsLegacy(t *testing.T) {
	channels := []string{"v0", "v1"}
	pks := pkIDs(1, 2, 3)
	var table *Table
	got, _, err := table.RouteInsert(pks, channels)
	require.NoError(t, err)
	want, _ := DeriveCompat(channels).RouteInsert(pks)
	assert.Equal(t, want, got)
}

func TestRouteInsertVarCharMatchesTheRewritePartitioner(t *testing.T) {
	// The write path and the rewrite must hash a key identically, or a row the
	// proxy sends to a shard is a row the rewrite would have put elsewhere.
	channels := []string{"v0", "v1"}
	table, err := Derive(2, channels, []Shard{
		{Vchannel: "v0", Buckets: []uint64{0}},
		{Vchannel: "v1", Buckets: []uint64{1}},
	})
	require.NoError(t, err)

	keys := []string{"alpha", "beta", "gamma", "delta"}
	pks := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: keys}}}
	offsets, _, err := table.RouteInsert(pks, channels)
	require.NoError(t, err)
	for vchannel, rows := range offsets {
		for _, row := range rows {
			want := "v" + string(rune('0'+typeutil.HashString2Uint32(keys[row])%2))
			assert.Equal(t, want, vchannel)
		}
	}
}

func TestRouteDeleteReturnsChannelPositions(t *testing.T) {
	// The delete repacker indexes into its own channel slice, so the table has
	// to hand back a position, not a name.
	table, channels := doubledTable(t)
	pks := pkIDs(1, 2, 3, 4, 5, 6, 7, 8)

	positions, err := table.RouteDelete(pks, channels)
	require.NoError(t, err)
	require.Len(t, positions, 8)

	offsets, _, err := table.RouteInsert(pks, channels)
	require.NoError(t, err)
	for vchannel, rows := range offsets {
		for _, row := range rows {
			assert.Equal(t, vchannel, channels[positions[row]],
				"a delete must land on the same shard as its insert")
		}
	}
}

func TestRouteDeleteRejectsAShardOutsideTheRequestChannels(t *testing.T) {
	// The caller's channel set and the routing table disagreeing is a real
	// state (a stale cache mid-split); guessing an index would send the
	// tombstone to an unrelated shard.
	table, _ := doubledTable(t)
	_, err := table.RouteDelete(pkIDs(1, 2, 3, 4, 5, 6, 7, 8), []string{"v1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in the request's channel set")
}

func TestRouteInsertErrorsOnAnUnownedHash(t *testing.T) {
	// A partial table (the split plan's own view) leaves residues unowned.
	// Placing such a row anywhere would corrupt the collection silently.
	partial, err := DeriveHashPartial(4, []HashShard{
		{Vchannel: "v0", Buckets: []uint64{0}},
	})
	require.NoError(t, err)
	table := &Table{hashTable: partial, channels: []string{"v0"}}

	pks := make([]int64, 0, 64)
	for i := int64(0); i < 64; i++ {
		pks = append(pks, i)
	}
	_, _, err = table.RouteInsert(pkIDs(pks...), []string{"v0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolved to no shard")
}

func TestRouteInsertWithNoRows(t *testing.T) {
	table, channels := doubledTable(t)
	offsets, hashValues, err := table.RouteInsert(pkIDs(), channels)
	require.NoError(t, err)
	assert.Empty(t, offsets)
	assert.Empty(t, hashValues)
}

func TestRouteDeleteErrorsOnAnUnownedHash(t *testing.T) {
	// Same rule as the insert path: a tombstone whose key no shard claims must
	// not be guessed onto one, or it would delete nothing while reporting
	// success.
	partial, err := DeriveHashPartial(4, []HashShard{{Vchannel: "v0", Buckets: []uint64{0}}})
	require.NoError(t, err)
	table := &Table{hashTable: partial, channels: []string{"v0"}}

	pks := make([]int64, 0, 64)
	for i := int64(0); i < 64; i++ {
		pks = append(pks, i)
	}
	_, err = table.RouteDelete(pkIDs(pks...), []string{"v0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolved to no shard")
}

func TestRouteOnAnUnsupportedIDType(t *testing.T) {
	// An id field neither int64 nor varchar has no hash, matching
	// typeutil.HashPK2Channels. Both paths return nothing rather than placing
	// rows arbitrarily.
	table, channels := doubledTable(t)
	empty := &schemapb.IDs{}

	offsets, hashValues, err := table.RouteInsert(empty, channels)
	require.NoError(t, err)
	assert.Empty(t, offsets)
	assert.Empty(t, hashValues)

	positions, err := table.RouteDelete(empty, channels)
	require.NoError(t, err)
	assert.Empty(t, positions)
}

// A caller that resolved no channels still gets its rows placed by the residues;
// only the opaque HashValues tag, which is taken modulo the channel count, has
// nothing to report.
func TestRouteInsertWithNoChannelsStillPlacesByResidue(t *testing.T) {
	table, _ := doubledTable(t)
	offsets, hashValues, err := table.RouteInsert(pkIDs(1, 2, 3, 4), nil)
	require.NoError(t, err)
	assert.Len(t, hashValues, 4)
	for _, v := range hashValues {
		assert.Zero(t, v)
	}
	placed := 0
	for _, rows := range offsets {
		placed += len(rows)
	}
	assert.Equal(t, 4, placed)
}
