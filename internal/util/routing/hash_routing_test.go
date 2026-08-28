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
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeriveHashCompatMatchesLegacyModulo(t *testing.T) {
	channels := []string{"ch0", "ch1", "ch2"}
	table, err := DeriveHashCompat(channels)
	require.NoError(t, err)
	assert.EqualValues(t, 3, table.NumSlots())

	for hash := uint64(0); hash < 30; hash++ {
		assert.Equal(t, channels[hash%3], table.Lookup(hash), "hash %d", hash)
	}
}

func TestDeriveHashCompatRejectsNoChannels(t *testing.T) {
	_, err := DeriveHashCompat(nil)
	assert.Error(t, err)
}

func TestDeriveHashSingleShardOwnsEverything(t *testing.T) {
	table, err := DeriveHash(1, []HashShard{{Vchannel: "only", Buckets: []uint64{0}}})
	require.NoError(t, err)
	assert.EqualValues(t, 1, table.NumSlots())
	for hash := uint64(0); hash < 10; hash++ {
		assert.Equal(t, "only", table.Lookup(hash))
	}
}

// After one doubling of a two-shard collection, the untouched shard has been
// rebased onto the new modulus and the two targets own the source's keys. No
// modulo over a channel list reproduces this placement, which is the whole point
// of carrying residues in the meta.
func TestDeriveHashAfterOneDoubling(t *testing.T) {
	table, err := DeriveHash(4, []HashShard{
		{Vchannel: "survivor", Buckets: []uint64{1, 3}},
		{Vchannel: "left", Buckets: []uint64{0}},
		{Vchannel: "right", Buckets: []uint64{2}},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 4, table.NumSlots())

	assert.Equal(t, "left", table.Lookup(0))
	assert.Equal(t, "survivor", table.Lookup(1))
	assert.Equal(t, "right", table.Lookup(2))
	assert.Equal(t, "survivor", table.Lookup(3))
	// And it wraps: 4 is residue 0 again.
	assert.Equal(t, "left", table.Lookup(4))
	assert.Equal(t, "survivor", table.Lookup(101))
}

func TestDeriveHashMultiResidueShard(t *testing.T) {
	table, err := DeriveHash(8, []HashShard{
		{Vchannel: "wide", Buckets: []uint64{0, 1, 2, 3, 4, 5}},
		{Vchannel: "narrow", Buckets: []uint64{6, 7}},
	})
	require.NoError(t, err)
	for r := uint64(0); r < 6; r++ {
		assert.Equal(t, "wide", table.Lookup(r))
	}
	assert.Equal(t, "narrow", table.Lookup(6))
	assert.Equal(t, "narrow", table.Lookup(7))
}

func TestDeriveHashRejectsMalformed(t *testing.T) {
	cases := []struct {
		name    string
		modulus uint64
		shards  []HashShard
	}{
		{
			name:    "zero modulus",
			modulus: 0,
			shards:  []HashShard{{Vchannel: "a", Buckets: []uint64{0}}},
		},
		{
			name:    "modulus above the cap",
			modulus: maxModulus + 1,
			shards:  []HashShard{{Vchannel: "a", Buckets: []uint64{0}}},
		},
		{
			name:    "no shards",
			modulus: 2,
			shards:  nil,
		},
		{
			name:    "shard owning no residue",
			modulus: 2,
			shards: []HashShard{
				{Vchannel: "a", Buckets: []uint64{0, 1}},
				{Vchannel: "dead", Buckets: nil},
			},
		},
		{
			name:    "residue not below the modulus",
			modulus: 2,
			shards:  []HashShard{{Vchannel: "a", Buckets: []uint64{0, 2}}},
		},
		{
			name:    "gap",
			modulus: 4,
			shards: []HashShard{
				{Vchannel: "a", Buckets: []uint64{0, 1}},
				{Vchannel: "b", Buckets: []uint64{2}},
			},
		},
		{
			name:    "overlap between shards",
			modulus: 2,
			shards: []HashShard{
				{Vchannel: "a", Buckets: []uint64{0, 1}},
				{Vchannel: "b", Buckets: []uint64{1}},
			},
		},
		{
			name:    "residue listed twice by one shard",
			modulus: 2,
			shards: []HashShard{
				{Vchannel: "a", Buckets: []uint64{0, 0}},
				{Vchannel: "b", Buckets: []uint64{1}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DeriveHash(tc.modulus, tc.shards)
			assert.Error(t, err)
		})
	}
}

// A split's targets deliberately cover only their source's keys, so the partial
// table must accept the gap the fenced source left behind — while still refusing
// an overlap, which would send one key to two shards.
func TestDeriveHashPartialAllowsAnUncoveredKeySpace(t *testing.T) {
	table, err := DeriveHashPartial(4, []HashShard{
		{Vchannel: "left", Buckets: []uint64{0}},
		{Vchannel: "right", Buckets: []uint64{2}},
	})
	require.NoError(t, err)

	vchannel, ok := table.LookupOK(0)
	assert.True(t, ok)
	assert.Equal(t, "left", vchannel)

	vchannel, ok = table.LookupOK(2)
	assert.True(t, ok)
	assert.Equal(t, "right", vchannel)

	// Residues 1 and 3 still belong to shards outside this plan.
	_, ok = table.LookupOK(1)
	assert.False(t, ok)
	_, ok = table.LookupOK(3)
	assert.False(t, ok)
	assert.Equal(t, "", table.Lookup(3))
}

func TestDeriveHashPartialStillRejectsOverlap(t *testing.T) {
	_, err := DeriveHashPartial(4, []HashShard{
		{Vchannel: "left", Buckets: []uint64{0, 2}},
		{Vchannel: "right", Buckets: []uint64{2}},
	})
	assert.Error(t, err)
}

func TestPlanSplitDividesAMultiResidueShard(t *testing.T) {
	plan, err := PlanSplit(8, []uint64{6, 0, 4, 2})
	require.NoError(t, err)
	// The modulus does not move: there was still a set to divide.
	assert.EqualValues(t, 8, plan.Modulus)
	assert.False(t, plan.DoublesModulus(8))
	assert.Equal(t, []uint64{0, 2}, plan.Left)
	assert.Equal(t, []uint64{4, 6}, plan.Right)
}

func TestPlanSplitGivesTheOddResidueToTheLeft(t *testing.T) {
	plan, err := PlanSplit(8, []uint64{1, 3, 5})
	require.NoError(t, err)
	assert.EqualValues(t, 8, plan.Modulus)
	assert.Equal(t, []uint64{1, 3}, plan.Left)
	assert.Equal(t, []uint64{5}, plan.Right)
}

// The left half is cut with a full slice expression, so appending to it cannot
// reach into the right half's backing array.
func TestPlanSplitHalvesDoNotAlias(t *testing.T) {
	plan, err := PlanSplit(8, []uint64{0, 2, 4, 6})
	require.NoError(t, err)
	plan.Left = append(plan.Left, 99)
	assert.Equal(t, []uint64{4, 6}, plan.Right)
}

func TestPlanSplitDoublesTheModulusForASingleResidue(t *testing.T) {
	plan, err := PlanSplit(2, []uint64{1})
	require.NoError(t, err)
	assert.EqualValues(t, 4, plan.Modulus)
	assert.True(t, plan.DoublesModulus(2))
	assert.Equal(t, []uint64{1}, plan.Left)
	assert.Equal(t, []uint64{3}, plan.Right)

	// The two halves cover exactly what the source covered, and nothing else.
	for hash := uint64(0); hash < 40; hash++ {
		wasSource := hash%2 == 1
		isTarget := hash%4 == 1 || hash%4 == 3
		assert.Equal(t, wasSource, isTarget, "hash %d", hash)
	}
}

func TestPlanSplitRejectsMalformed(t *testing.T) {
	cases := []struct {
		name    string
		modulus uint64
		buckets []uint64
	}{
		{"zero modulus", 0, []uint64{0}},
		{"no residue to split", 4, nil},
		{"residue not below the modulus", 4, []uint64{4}},
		{"duplicate residue", 4, []uint64{1, 1}},
		{"doubling would exceed the cap", maxModulus, []uint64{0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := PlanSplit(tc.modulus, tc.buckets)
			assert.Error(t, err)
		})
	}
}

func TestRebaseCarriesAShardOntoALargerModulus(t *testing.T) {
	rebased, err := Rebase([]uint64{1}, 2, 8)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 3, 5, 7}, rebased)

	// Rebasing onto the same modulus is a sorted copy.
	rebased, err = Rebase([]uint64{3, 1}, 4, 4)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 3}, rebased)
}

// Rebasing preserves exactly which keys the shard owns — that is the property
// that makes a doubling safe for the shards the split never touched.
func TestRebasePreservesOwnership(t *testing.T) {
	rebased, err := Rebase([]uint64{1, 2}, 4, 16)
	require.NoError(t, err)
	for hash := uint64(0); hash < 200; hash++ {
		before := hash%4 == 1 || hash%4 == 2
		after := false
		for _, r := range rebased {
			if hash%16 == r {
				after = true
			}
		}
		assert.Equal(t, before, after, "hash %d", hash)
	}
}

func TestRebaseRejectsMalformed(t *testing.T) {
	cases := []struct {
		name       string
		buckets    []uint64
		oldModulus uint64
		newModulus uint64
	}{
		{"zero old modulus", []uint64{0}, 0, 4},
		{"zero new modulus", []uint64{0}, 2, 0},
		{"new modulus above the cap", []uint64{0}, 2, maxModulus * 2},
		{"not a multiple", []uint64{0}, 3, 4},
		{"shrinking", []uint64{0}, 4, 2},
		{"residue not below the old modulus", []uint64{4}, 4, 8},
		{"no residue", nil, 4, 8},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Rebase(tc.buckets, tc.oldModulus, tc.newModulus)
			assert.Error(t, err)
		})
	}
}

// The end-to-end invariant of the model: whatever sequence of splits a collection
// goes through, the residues still tile the key space exactly, and a key only
// ever changes owner when the shard owning it is the one being split. A key that
// moved because some unrelated shard split would be a key the rewrite never
// carried — silently lost on read.
func TestRepeatedSplitsPreserveOwnership(t *testing.T) {
	const numKeys = 4096
	rng := rand.New(rand.NewSource(20260828))

	channels := []string{"ch0", "ch1", "ch2"}
	modulus := uint64(len(channels))
	owners := map[string][]uint64{
		"ch0": {0},
		"ch1": {1},
		"ch2": {2},
	}

	ownerOf := func() []string {
		shards := make([]HashShard, 0, len(owners))
		for vchannel, buckets := range owners {
			shards = append(shards, HashShard{Vchannel: vchannel, Buckets: buckets})
		}
		table, err := DeriveHash(modulus, shards)
		require.NoError(t, err, "modulus %d owners %v", modulus, owners)

		out := make([]string, numKeys)
		for hash := 0; hash < numKeys; hash++ {
			out[hash] = table.Lookup(uint64(hash))
		}
		return out
	}

	before := ownerOf()
	for round := 0; round < 12; round++ {
		names := make([]string, 0, len(owners))
		for vchannel := range owners {
			names = append(names, vchannel)
		}
		// Iteration order of a map is unspecified, so sort before picking to keep
		// the round reproducible under the fixed seed.
		sort.Strings(names)
		source := names[rng.Intn(len(names))]

		plan, err := PlanSplit(modulus, owners[source])
		require.NoError(t, err)

		next := map[string][]uint64{}
		if plan.DoublesModulus(modulus) {
			for vchannel, buckets := range owners {
				if vchannel == source {
					continue
				}
				rebased, err := Rebase(buckets, modulus, plan.Modulus)
				require.NoError(t, err)
				next[vchannel] = rebased
			}
		} else {
			for vchannel, buckets := range owners {
				if vchannel != source {
					next[vchannel] = buckets
				}
			}
		}
		next[source] = plan.Left
		next[fmt.Sprintf("%s-t%d", source, round)] = plan.Right

		modulus, owners = plan.Modulus, next

		after := ownerOf()
		for hash := 0; hash < numKeys; hash++ {
			if before[hash] == source {
				// A key of the split source lands on one of the two halves.
				assert.Contains(t, []string{source, fmt.Sprintf("%s-t%d", source, round)}, after[hash],
					"round %d hash %d", round, hash)
				continue
			}
			assert.Equal(t, before[hash], after[hash], "round %d hash %d moved off an untouched shard", round, hash)
		}
		before = after
	}
}
