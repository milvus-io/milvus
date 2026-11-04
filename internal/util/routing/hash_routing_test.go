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
)

func TestDeriveCompatMatchesLegacyModulo(t *testing.T) {
	channels := []string{"ch0", "ch1", "ch2"}
	table, err := deriveCompat(channels)
	require.NoError(t, err)
	assert.EqualValues(t, 3, table.modulus)

	for hash := uint64(0); hash < 30; hash++ {
		assert.Equal(t, channels[hash%3], owner(t, table, hash), "hash %d", hash)
	}
}

func TestDeriveCompatRejectsNoChannels(t *testing.T) {
	_, err := deriveCompat(nil)
	assert.Error(t, err)
}

func TestDeriveHashSingleShardOwnsEverything(t *testing.T) {
	table, err := deriveHash(1, []Shard{{Vchannel: "only", Buckets: []uint64{0}}})
	require.NoError(t, err)
	assert.EqualValues(t, 1, table.modulus)
	for hash := uint64(0); hash < 10; hash++ {
		assert.Equal(t, "only", owner(t, table, hash))
	}
}

// After one doubling of a two-shard collection, the untouched shard has been
// rebased onto the new modulus and the two targets own the source's keys. No
// modulo over a channel list reproduces this placement, which is the whole point
// of carrying residues in the meta.
func TestDeriveHashAfterOneDoubling(t *testing.T) {
	table, err := deriveHash(4, []Shard{
		{Vchannel: "survivor", Buckets: []uint64{1, 3}},
		{Vchannel: "left", Buckets: []uint64{0}},
		{Vchannel: "right", Buckets: []uint64{2}},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 4, table.modulus)

	assert.Equal(t, "left", owner(t, table, 0))
	assert.Equal(t, "survivor", owner(t, table, 1))
	assert.Equal(t, "right", owner(t, table, 2))
	assert.Equal(t, "survivor", owner(t, table, 3))
	// And it wraps: 4 is residue 0 again.
	assert.Equal(t, "left", owner(t, table, 4))
	assert.Equal(t, "survivor", owner(t, table, 101))
}

func TestDeriveHashMultiResidueShard(t *testing.T) {
	table, err := deriveHash(8, []Shard{
		{Vchannel: "wide", Buckets: []uint64{0, 1, 2, 3, 4, 5}},
		{Vchannel: "narrow", Buckets: []uint64{6, 7}},
	})
	require.NoError(t, err)
	for r := uint64(0); r < 6; r++ {
		assert.Equal(t, "wide", owner(t, table, r))
	}
	assert.Equal(t, "narrow", owner(t, table, 6))
	assert.Equal(t, "narrow", owner(t, table, 7))
}

func TestDeriveHashRejectsMalformed(t *testing.T) {
	cases := []struct {
		name    string
		modulus uint64
		shards  []Shard
		errText string
	}{
		{
			name:    "zero modulus",
			modulus: 0,
			shards:  []Shard{{Vchannel: "a", Buckets: []uint64{0}}},
			errText: "must be positive",
		},
		{
			name:    "modulus above the cap",
			modulus: maxModulus + 1,
			shards:  []Shard{{Vchannel: "a", Buckets: []uint64{0}}},
			errText: "exceeds the cap",
		},
		{
			name:    "no shards",
			modulus: 2,
			shards:  nil,
			errText: "at least one shard",
		},
		{
			name:    "shard owning no residue",
			modulus: 2,
			shards: []Shard{
				{Vchannel: "a", Buckets: []uint64{0, 1}},
				{Vchannel: "dead", Buckets: nil},
			},
			errText: `shard "dead" owns no residue`,
		},
		{
			name:    "residue not below the modulus",
			modulus: 2,
			shards:  []Shard{{Vchannel: "a", Buckets: []uint64{0, 2}}},
			errText: "not below the modulus",
		},
		{
			name:    "gap",
			modulus: 4,
			shards: []Shard{
				{Vchannel: "a", Buckets: []uint64{0, 1}},
				{Vchannel: "b", Buckets: []uint64{2}},
			},
			errText: "gap: residue 3",
		},
		{
			name:    "overlap between shards",
			modulus: 2,
			shards: []Shard{
				{Vchannel: "a", Buckets: []uint64{0, 1}},
				{Vchannel: "b", Buckets: []uint64{1}},
			},
			errText: `overlap at residue 1 (mod 2): shards "a" and "b"`,
		},
		{
			// Reported as one shard's malformed entry, not as an overlap with a
			// second shard that does not exist.
			name:    "residue listed twice by one shard",
			modulus: 2,
			shards: []Shard{
				{Vchannel: "a", Buckets: []uint64{0, 0}},
				{Vchannel: "b", Buckets: []uint64{1}},
			},
			errText: `shard "a" lists residue 0 (mod 2) twice`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := deriveHash(tc.modulus, tc.shards)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errText)
		})
	}
}

// The cap matches the bound schemapb states for routing_modulus, so a value the
// meta can legitimately carry is never rejected and a corrupt one cannot make
// the slot array enormous.
func TestModulusCapMatchesTheMetaBound(t *testing.T) {
	assert.EqualValues(t, 1<<15, maxModulus)
}

// The empty name is the marker for "this residue is unowned", so a shard
// carrying it would be indistinguishable from a gap: its residues would read
// back as unowned and every other shard's claim on them would go unchecked.
func TestDeriveHashRejectsAShardWithoutAName(t *testing.T) {
	_, err := deriveHash(2, []Shard{
		{Vchannel: "", Buckets: []uint64{0}},
		{Vchannel: "b", Buckets: []uint64{1}},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no vchannel name")
}
