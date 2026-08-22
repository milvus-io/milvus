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

// doubledTable is the topology a doubling split leaves behind: the untouched
// shard keeps {2,1}, and the two targets carved from the old {2,0} own {4,0}
// and {4,2}. The fenced source is already filtered out by ShardsFromMeta.
func doubledTable(t *testing.T) (*Table, []string) {
	channels := []string{"v1", "v2", "v3"}
	table, err := Derive(schemapb.RoutingMode_RoutingModeHash, channels, []Shard{
		{Vchannel: "v1", Buckets: []HashBucket{{Modulus: 2, Remainder: 1}}},
		{Vchannel: "v2", Buckets: []HashBucket{{Modulus: 4, Remainder: 0}}},
		{Vchannel: "v3", Buckets: []HashBucket{{Modulus: 4, Remainder: 2}}},
	})
	require.NoError(t, err)
	require.True(t, table.IsExplicitHash())
	return table, channels
}

func TestRouteInsertFollowsBucketsNotPosition(t *testing.T) {
	// The reason the write path cannot keep using a modulo over the channel
	// list: after a doubling no single modulus describes the topology, so
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
				assert.Equal(t, uint32(1), h%2, "v1 owns {2,1}")
			case "v2":
				assert.Equal(t, uint32(0), h%4, "v2 owns {4,0}")
			case "v3":
				assert.Equal(t, uint32(2), h%4, "v3 owns {4,2}")
			default:
				t.Fatalf("row routed to unknown shard %q", vchannel)
			}
		}
	}
	assert.Equal(t, len(pks), placed, "every row is placed exactly once")
}

func TestRouteInsertDisagreesWithTheLegacyModulo(t *testing.T) {
	// Guards the test above against being vacuous: if bucket routing happened
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
	// A hash collection that has never been split carries no predicate, and its
	// routing must not move by so much as one row.
	channels := []string{"v0", "v1", "v2"}
	table, err := Derive(schemapb.RoutingMode_RoutingModeHash, channels, []Shard{
		{Vchannel: "v0"}, {Vchannel: "v1"}, {Vchannel: "v2"},
	})
	require.NoError(t, err)
	require.False(t, table.IsExplicitHash())

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
	table, err := Derive(schemapb.RoutingMode_RoutingModeHash, channels, []Shard{
		{Vchannel: "v0", Buckets: []HashBucket{{Modulus: 2, Remainder: 0}}},
		{Vchannel: "v1", Buckets: []HashBucket{{Modulus: 2, Remainder: 1}}},
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
	partial, err := DeriveHashPartial([]HashShard{
		{Vchannel: "v0", Buckets: []HashBucket{{Modulus: 4, Remainder: 0}}},
	})
	require.NoError(t, err)
	table := &Table{mode: schemapb.RoutingMode_RoutingModeHash, hashTable: partial, channels: []string{"v0"}}

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
