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
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func pkIDs(pks ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}}
}

func strIDs(data ...string) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: data}}}
}

// legacyAssign mirrors internal/proxy.assignChannelsByPK, the placement the
// write path had before this package existed. Every legacy-mode assertion below
// is against this, not against a restatement of the new code.
func legacyAssign(t *testing.T, pks *schemapb.IDs, channelNames []string) (map[string][]int, []uint32) {
	t.Helper()
	hashValues, err := typeutil.HashPK2Channels(pks, channelNames)
	require.NoError(t, err)
	numChannels := len(channelNames)
	avgCapacity := (len(hashValues) / numChannels) + 1
	out := make(map[string][]int, numChannels)
	for offset, channelID := range hashValues {
		name := channelNames[int(channelID)]
		if _, ok := out[name]; !ok {
			out[name] = make([]int, 0, avgCapacity)
		}
		out[name] = append(out[name], offset)
	}
	return out, hashValues
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
	byPosition, _ := legacyAssign(t, pkIDs(pks...), channels)
	assert.NotEqual(t, byPosition, byBuckets)
}

// The shard index the write path carries is a position in the channel list —
// that is how insertMsg.HashValues is read everywhere else. It must name the
// shard the row was actually placed on, on both routing rules.
func TestHashValuesIndexTheOwningChannel(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(*testing.T) (*Table, []string)
	}{
		{"after a doubling", doubledTable},
		{"never split", func(t *testing.T) (*Table, []string) {
			channels := []string{"v0", "v1", "v2"}
			tbl, err := Derive(0, channels, nil)
			require.NoError(t, err)
			return tbl, channels
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table, channels := tc.build(t)
			pks := make([]int64, 0, 200)
			for i := int64(0); i < 200; i++ {
				pks = append(pks, i)
			}
			offsets, hashValues, err := table.RouteInsert(pkIDs(pks...), channels)
			require.NoError(t, err)
			require.Len(t, hashValues, len(pks))
			for vchannel, rows := range offsets {
				for _, row := range rows {
					assert.Equal(t, vchannel, channels[hashValues[row]],
						"HashValues[%d] must index the shard the row was placed on", row)
				}
			}
		})
	}
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
	want, wantHashes := legacyAssign(t, pks, channels)
	assert.Equal(t, want, got)
	assert.Equal(t, wantHashes, gotHashes)
}

// The equivalence over a wide spread of shard counts, key types and batch sizes,
// so "bit for bit" is measured rather than asserted once.
func TestRouteInsertEquivalentToLegacyRandomized(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for iter := 0; iter < 500; iter++ {
		n := 1 + r.Intn(16)
		ch := make([]string, n)
		for i := range ch {
			ch[i] = "by-dev-rootcoord-dml_" + string(rune('a'+i))
		}
		table, err := Derive(0, ch, nil)
		require.NoError(t, err)

		rows := 1 + r.Intn(50)
		var pks *schemapb.IDs
		if iter%2 == 0 {
			data := make([]int64, rows)
			for i := range data {
				data[i] = r.Int63() - r.Int63()
			}
			pks = pkIDs(data...)
		} else {
			data := make([]string, rows)
			for i := range data {
				data[i] = strings.Repeat("k", r.Intn(8)) + string(rune('0'+r.Intn(10)))
			}
			pks = strIDs(data...)
		}
		wantMap, wantHash := legacyAssign(t, pks, ch)
		gotMap, gotHash, err := table.RouteInsert(pks, ch)
		require.NoError(t, err)
		assert.Equal(t, wantHash, gotHash, "iter=%d hash", iter)
		assert.Equal(t, wantMap, gotMap, "iter=%d map", iter)
	}
}

// A long varchar is truncated before hashing and an empty one still hashes, so
// the legacy equivalence has to cover both explicitly.
func TestRouteInsertVarCharEdgeKeysMatchLegacy(t *testing.T) {
	ch := []string{"v0", "v1", "v2"}
	table, err := Derive(0, ch, nil)
	require.NoError(t, err)
	pks := strIDs("", "a", "namespace-42", strings.Repeat("x", 101), strings.Repeat("y", 100))
	wantMap, wantHash := legacyAssign(t, pks, ch)
	gotMap, gotHash, err := table.RouteInsert(pks, ch)
	require.NoError(t, err)
	assert.Equal(t, wantHash, gotHash)
	assert.Equal(t, wantMap, gotMap)
}

// A nil table is how a caller carries "the routing meta was malformed and I
// refused to derive it". Every entry point must reject the write; falling back
// to the legacy modulo would place a split collection's rows by a rule nobody
// chose, which is the failure this package exists to prevent.
func TestRoutingOnANilTableIsRejected(t *testing.T) {
	channels := []string{"v0", "v1"}
	pks := pkIDs(1, 2, 3)
	var table *Table

	rejected := func(err error) {
		t.Helper()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no routing table")
		// System, not input: the caller could not derive the table, so the write
		// is retriable once the meta it refused is readable again.
		assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(err))
	}

	_, _, err := table.RouteInsert(pks, channels)
	rejected(err)
	_, err = table.RouteDelete(pks, channels)
	rejected(err)
	_, _, err = table.RouteInsertHashes([]uint64{1, 2}, channels)
	rejected(err)
	_, err = table.RouteDeleteHashes([]uint64{1, 2}, channels)
	rejected(err)
	_, err = table.Route(1)
	rejected(err)
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
	pks := strIDs(keys...)
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

// The caller's channel set and the routing table disagreeing is a real state (a
// stale cache mid-split). Both write paths must refuse it: an insert placed on a
// vchannel the caller never resolved is as wrong as a tombstone sent there, and
// the insert path used to accept it silently.
func TestRoutingRejectsAShardOutsideTheRequestChannels(t *testing.T) {
	table, _ := doubledTable(t)
	pks := pkIDs(1, 2, 3, 4, 5, 6, 7, 8)

	_, _, err := table.RouteInsert(pks, []string{"v1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in the request's channel set")

	_, err = table.RouteDelete(pks, []string{"v1"})
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
	table := &Table{residues: partial, channels: []string{"v0"}, explicit: true}

	pks := make([]int64, 0, 64)
	for i := int64(0); i < 64; i++ {
		pks = append(pks, i)
	}
	_, _, err = table.RouteInsert(pkIDs(pks...), []string{"v0"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolved to no shard")
}

func TestRouteDeleteErrorsOnAnUnownedHash(t *testing.T) {
	// Same rule as the insert path: a tombstone whose key no shard claims must
	// not be guessed onto one, or it would delete nothing while reporting
	// success.
	partial, err := DeriveHashPartial(4, []HashShard{{Vchannel: "v0", Buckets: []uint64{0}}})
	require.NoError(t, err)
	table := &Table{residues: partial, channels: []string{"v0"}, explicit: true}

	pks := make([]int64, 0, 64)
	for i := int64(0); i < 64; i++ {
		pks = append(pks, i)
	}
	_, err = table.RouteDelete(pkIDs(pks...), []string{"v0"})
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

// The shard index a write carries is a position in the request's channel set,
// and an empty set has none. Reporting no placement and no error would tell the
// client its rows were written when nothing was.
func TestRoutingRejectsAnEmptyChannelSet(t *testing.T) {
	table, _ := doubledTable(t)
	_, _, err := table.RouteInsert(pkIDs(1, 2, 3, 4), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "carries no channels")

	_, err = table.RouteDelete(pkIDs(1, 2, 3, 4), nil)
	require.Error(t, err)
}

func TestRoutingRejectsADuplicateChannel(t *testing.T) {
	table, _ := doubledTable(t)
	_, _, err := table.RouteInsert(pkIDs(1), []string{"v1", "v2", "v1"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "listed twice")
}

// A namespace-sharded collection places a row by the hash of its namespace id.
// Hashing primary keys for it would send every row to a shard that does not own
// it, so the primary-key entry points refuse rather than do it.
func TestPrimaryKeyRoutingRefusesANamespaceRoutedCollection(t *testing.T) {
	channels := []string{"v0", "v1"}
	table, err := Derive(2, channels, []Shard{
		{Vchannel: "v0", Buckets: []uint64{0}},
		{Vchannel: "v1", Buckets: []uint64{1}},
	}, WithShardBy("hash("+NamespaceIDField+")", "pk"))
	require.NoError(t, err)
	require.False(t, table.RoutesByPrimaryKey())

	_, _, err = table.RouteInsert(pkIDs(1, 2, 3), channels)
	require.Error(t, err)
	assert.Contains(t, err.Error(), NamespaceIDField)

	_, err = table.RouteDelete(pkIDs(1, 2, 3), channels)
	require.Error(t, err)
	assert.Contains(t, err.Error(), NamespaceIDField)
}

// And the hash form places it, through the same residues, so the refusal above
// is a redirection rather than a dead end.
func TestRouteInsertHashesPlacesANamespaceRoutedCollection(t *testing.T) {
	channels := []string{"v0", "v1"}
	table, err := Derive(2, channels, []Shard{
		{Vchannel: "v0", Buckets: []uint64{0}},
		{Vchannel: "v1", Buckets: []uint64{1}},
	}, WithShardBy("hash("+NamespaceIDField+")", "pk"))
	require.NoError(t, err)

	namespaces := []string{"tenant-a", "tenant-b", "tenant-c", "tenant-d"}
	hashes := make([]uint64, 0, len(namespaces))
	for _, ns := range namespaces {
		hashes = append(hashes, HashNamespace(ns))
	}
	offsets, hashValues, err := table.RouteInsertHashes(hashes, channels)
	require.NoError(t, err)
	require.Len(t, hashValues, len(namespaces))
	for vchannel, rows := range offsets {
		for _, row := range rows {
			// The same placement the legacy namespace assignment made, since the
			// collection is at modulus 2 with one residue per shard.
			want := channels[typeutil.HashNamespace2Channels(namespaces[row], channels)]
			assert.Equal(t, want, vchannel)
			assert.Equal(t, vchannel, channels[hashValues[row]])
		}
	}
}

// A collection whose shard_by names the primary key by its own field name still
// routes through the primary-key entry points.
func TestPrimaryKeyRoutingAcceptsShardByNamingThePrimaryKey(t *testing.T) {
	channels := []string{"v0", "v1"}
	table, err := Derive(2, channels, []Shard{
		{Vchannel: "v0", Buckets: []uint64{0}},
		{Vchannel: "v1", Buckets: []uint64{1}},
	}, WithShardBy("hash(id)", "id"))
	require.NoError(t, err)
	require.True(t, table.RoutesByPrimaryKey())

	_, hashValues, err := table.RouteInsert(pkIDs(1, 2, 3), channels)
	require.NoError(t, err)
	assert.Len(t, hashValues, 3)
}

func TestHashPrimaryKeysWidensWithoutReducing(t *testing.T) {
	// The residues are cut on the hash's own bits, so the value must not be
	// reduced by the shard count on its way in.
	hashes, err := HashPrimaryKeys(pkIDs(1, 2, 3))
	require.NoError(t, err)
	require.Len(t, hashes, 3)
	for i, pk := range []int64{1, 2, 3} {
		want, err := typeutil.Hash32Int64(pk)
		require.NoError(t, err)
		assert.Equal(t, uint64(want), hashes[i])
	}

	hashes, err = HashPrimaryKeys(&schemapb.IDs{})
	require.NoError(t, err)
	assert.Empty(t, hashes)
}

// A hash that cannot be computed must surface as an error on every path that
// leads to it. Returning "no rows" instead would report a successful write of a
// batch no shard ever received, which is the one outcome worse than a rejection.
func TestAHashFailureIsNeverSilent(t *testing.T) {
	table, channels := doubledTable(t)
	pks := pkIDs(1, 2, 3)

	mockHash := mockey.Mock(typeutil.Hash32Int64).Return(uint32(0), errors.New("hash unavailable")).Build()
	defer mockHash.UnPatch()

	_, err := HashPrimaryKeys(pks)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot hash primary key")

	_, _, err = table.RouteInsert(pks, channels)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot hash primary key")

	_, err = table.RouteDelete(pks, channels)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot hash primary key")
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
