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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeriveHashCompatMatchesLegacyModulo(t *testing.T) {
	channels := []string{"c0", "c1", "c2", "c3"}
	tbl, err := DeriveHashCompat(channels)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), tbl.NumSlots())

	// An unsplit collection must route bit-for-bit like hash % numShards.
	for h := uint64(0); h < 1000; h++ {
		assert.Equal(t, channels[h%4], tbl.Lookup(h), "hash %d", h)
	}
}

func TestDeriveHashSingleShardOwnsEverything(t *testing.T) {
	tbl, err := DeriveHashCompat([]string{"only"})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), tbl.NumSlots())
	for h := uint64(0); h < 10; h++ {
		assert.Equal(t, "only", tbl.Lookup(h))
	}
}

func TestDeriveHashMixedModuliAfterDoubling(t *testing.T) {
	// Start from 2 shards, then split shard 0 (mod 2, rem 0) into
	// (mod 4, rem 0) and (mod 4, rem 2). Shard 1 keeps mod 2.
	a, b := SplitBuckets(HashBucket{Modulus: 2, Remainder: 0})
	assert.Equal(t, HashBucket{Modulus: 4, Remainder: 0}, a)
	assert.Equal(t, HashBucket{Modulus: 4, Remainder: 2}, b)

	tbl, err := DeriveHash([]HashShard{
		{Vchannel: "c0a", Buckets: []HashBucket{a}},
		{Vchannel: "c0b", Buckets: []HashBucket{b}},
		{Vchannel: "c1", Buckets: []HashBucket{{Modulus: 2, Remainder: 1}}},
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(4), tbl.NumSlots()) // lcm(4,4,2)

	// Every key that used to go to shard 0 (even hash) now splits between the
	// two halves; the untouched shard 1 (odd hash) is unchanged.
	assert.Equal(t, "c0a", tbl.Lookup(0))
	assert.Equal(t, "c1", tbl.Lookup(1))
	assert.Equal(t, "c0b", tbl.Lookup(2))
	assert.Equal(t, "c1", tbl.Lookup(3))
	assert.Equal(t, "c0a", tbl.Lookup(4))
	assert.Equal(t, "c1", tbl.Lookup(5))

	// The untouched shard's routing is preserved exactly: everything that
	// hashed odd still lands on c1.
	for h := uint64(0); h < 200; h++ {
		if h%2 == 1 {
			require.Equal(t, "c1", tbl.Lookup(h), "hash %d", h)
		}
	}
}

func TestDeriveHashRepeatedDoublingStaysConsistent(t *testing.T) {
	// 1 shard -> split -> split one half again: moduli 2,4,4.
	a, b := SplitBuckets(HashBucket{Modulus: 1, Remainder: 0})
	ba, bb := SplitBuckets(b)
	tbl, err := DeriveHash([]HashShard{
		{Vchannel: "A", Buckets: []HashBucket{a}},
		{Vchannel: "BA", Buckets: []HashBucket{ba}},
		{Vchannel: "BB", Buckets: []HashBucket{bb}},
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(4), tbl.NumSlots())

	counts := map[string]int{}
	for h := uint64(0); h < 4000; h++ {
		counts[tbl.Lookup(h)]++
	}
	// A owns half the space, BA and BB a quarter each.
	assert.Equal(t, 2000, counts["A"])
	assert.Equal(t, 1000, counts["BA"])
	assert.Equal(t, 1000, counts["BB"])
}

func TestDeriveHashMultiBucketShard(t *testing.T) {
	// A shard may own more than one bucket (e.g. after a consolidation).
	tbl, err := DeriveHash([]HashShard{
		{Vchannel: "wide", Buckets: []HashBucket{
			{Modulus: 4, Remainder: 0},
			{Modulus: 4, Remainder: 2},
		}},
		{Vchannel: "narrow", Buckets: []HashBucket{{Modulus: 2, Remainder: 1}}},
	})
	require.NoError(t, err)
	assert.Equal(t, "wide", tbl.Lookup(0))
	assert.Equal(t, "wide", tbl.Lookup(2))
	assert.Equal(t, "narrow", tbl.Lookup(1))
	assert.Equal(t, "narrow", tbl.Lookup(3))
}

func TestDeriveHashRejectsMalformed(t *testing.T) {
	cases := []struct {
		name   string
		shards []HashShard
		errStr string
	}{
		{
			name:   "empty",
			shards: nil,
			errStr: "at least one shard",
		},
		{
			name:   "zero modulus",
			shards: []HashShard{{Vchannel: "c", Buckets: []HashBucket{{Modulus: 0}}}},
			errStr: "zero-modulus",
		},
		{
			name:   "remainder out of range",
			shards: []HashShard{{Vchannel: "c", Buckets: []HashBucket{{Modulus: 2, Remainder: 2}}}},
			errStr: "remainder",
		},
		{
			name: "gap leaves a residue unowned",
			shards: []HashShard{
				{Vchannel: "c0", Buckets: []HashBucket{{Modulus: 2, Remainder: 0}}},
			},
			errStr: "gap",
		},
		{
			name: "overlap claims a residue twice",
			shards: []HashShard{
				{Vchannel: "c0", Buckets: []HashBucket{{Modulus: 1, Remainder: 0}}},
				{Vchannel: "c1", Buckets: []HashBucket{{Modulus: 2, Remainder: 1}}},
			},
			errStr: "overlap",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DeriveHash(tc.shards)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errStr)
		})
	}
}

func TestDeriveHashRejectsOversizedModulus(t *testing.T) {
	_, err := DeriveHash([]HashShard{
		{Vchannel: "c", Buckets: []HashBucket{{Modulus: maxNormalizedModulus * 2, Remainder: 0}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cap")
}

func TestDeriveHashRejectsLcmOverflow(t *testing.T) {
	// Two coprime large moduli whose lcm blows past the cap.
	_, err := DeriveHash([]HashShard{
		{Vchannel: "a", Buckets: []HashBucket{{Modulus: 65521, Remainder: 0}}},
		{Vchannel: "b", Buckets: []HashBucket{{Modulus: 65519, Remainder: 0}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cap")
}

func TestSplitBucketsPartitionsExactly(t *testing.T) {
	for _, src := range []HashBucket{
		{Modulus: 1, Remainder: 0},
		{Modulus: 2, Remainder: 1},
		{Modulus: 8, Remainder: 5},
	} {
		t.Run(fmt.Sprintf("mod%d_rem%d", src.Modulus, src.Remainder), func(t *testing.T) {
			a, b := SplitBuckets(src)
			assert.Equal(t, src.Modulus*2, a.Modulus)
			assert.Equal(t, src.Modulus*2, b.Modulus)
			// Every key of the source goes to exactly one of the two halves.
			for h := uint64(0); h < 512; h++ {
				if h%src.Modulus != src.Remainder {
					continue // not the source's key
				}
				inA := h%a.Modulus == a.Remainder
				inB := h%b.Modulus == b.Remainder
				assert.True(t, inA != inB, "hash %d must land in exactly one half", h)
			}
		})
	}
}

func TestGcdLcm(t *testing.T) {
	assert.Equal(t, uint64(4), gcd(8, 12))
	assert.Equal(t, uint64(1), gcd(9, 8))
	v, err := lcm(4, 6)
	require.NoError(t, err)
	assert.Equal(t, uint64(12), v)
	v, err = lcm(1, 7)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), v)
}

func TestDeriveHashPartialAllowsAnUncoveredKeySpace(t *testing.T) {
	// A split's targets tile only their source's bucket while the split is in
	// flight. That is a legitimate partial cover, not a routing gap, so
	// DeriveHashPartial accepts it where DeriveHash must not.
	shards := []HashShard{
		{Vchannel: "t0", Buckets: []HashBucket{{Modulus: 4, Remainder: 0}}},
		{Vchannel: "t1", Buckets: []HashBucket{{Modulus: 4, Remainder: 2}}},
	}

	_, err := DeriveHash(shards)
	require.Error(t, err, "a whole-space table must still reject the gap")
	assert.Contains(t, err.Error(), "gap")

	table, err := DeriveHashPartial(shards)
	require.NoError(t, err)

	owner, ok := table.LookupOK(4) // 4%4 == 0
	assert.True(t, ok)
	assert.Equal(t, "t0", owner)

	// An unclaimed residue is reported as unclaimed rather than resolved to some
	// shard: a key that belongs to no target means a malformed plan, and
	// guessing an owner would misplace the row silently.
	_, ok = table.LookupOK(1)
	assert.False(t, ok)
}

func TestDeriveHashPartialStillRejectsOverlap(t *testing.T) {
	// Dropping the cover requirement must not drop the disjointness one: an
	// overlap would write one key to two shards.
	_, err := DeriveHashPartial([]HashShard{
		{Vchannel: "a", Buckets: []HashBucket{{Modulus: 4, Remainder: 0}}},
		{Vchannel: "b", Buckets: []HashBucket{{Modulus: 8, Remainder: 4}}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "overlap")
}
