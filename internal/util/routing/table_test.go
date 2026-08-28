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

func TestDeriveWithoutRoutingMetaKeepsModuloBehaviour(t *testing.T) {
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
}

func TestRouteErrorsWithoutChannels(t *testing.T) {
	tbl, err := Derive(0, nil, nil)
	require.NoError(t, err)
	_, err = tbl.Route(1)
	require.Error(t, err)
}

func TestRouteOnANilTableErrors(t *testing.T) {
	var tbl *Table
	_, err := tbl.Route(1)
	require.Error(t, err)
	assert.False(t, tbl.IsExplicit())
}

// A partial table can be reached only through a plan that does not tile the key
// space, which Derive rejects. Route still has to answer rather than guess, so
// the unowned residue surfaces as an error.
func TestRouteErrorsOnAnUnownedResidue(t *testing.T) {
	ht, err := DeriveHashPartial(4, []HashShard{{Vchannel: "left", Buckets: []uint64{0}}})
	require.NoError(t, err)
	tbl := &Table{hashTable: ht, channels: []string{"left"}}

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
		{State: schemapb.ShardState_ShardSplitting, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{0, 1}},
		}},
		{State: schemapb.ShardState_ShardCreating, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
		}},
		{State: schemapb.ShardState_ShardNormal, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []uint64{1}},
		}},
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
