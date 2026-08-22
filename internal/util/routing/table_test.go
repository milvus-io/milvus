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

func TestDeriveLegacyHashKeepsModuloBehaviour(t *testing.T) {
	// A collection that has never been split carries no per-shard predicate;
	// it must route exactly like hash(pk) % shardNum.
	channels := []string{"c0", "c1", "c2"}
	tbl, err := Derive(schemapb.RoutingMode_RoutingModeHash, channels, nil)
	require.NoError(t, err)
	assert.False(t, tbl.IsExplicit())
	assert.Equal(t, 3, tbl.NumShards())

	for h := uint64(0); h < 300; h++ {
		ch, err := tbl.Route(Key{Hash: h})
		require.NoError(t, err)
		assert.Equal(t, channels[h%3], ch, "hash %d", h)
	}
}

func TestDeriveHashWithBucketsRoutesBySlot(t *testing.T) {
	// After splitting shard 0 of a 2-shard collection.
	tbl, err := Derive(schemapb.RoutingMode_RoutingModeHash,
		[]string{"c0a", "c0b", "c1"},
		[]Shard{
			{Vchannel: "c0a", Buckets: []HashBucket{{Modulus: 4, Remainder: 0}}},
			{Vchannel: "c0b", Buckets: []HashBucket{{Modulus: 4, Remainder: 2}}},
			{Vchannel: "c1", Buckets: []HashBucket{{Modulus: 2, Remainder: 1}}},
		})
	require.NoError(t, err)
	assert.True(t, tbl.IsExplicit())

	for _, tc := range []struct {
		hash uint64
		want string
	}{{0, "c0a"}, {1, "c1"}, {2, "c0b"}, {3, "c1"}, {4, "c0a"}} {
		ch, err := tbl.Route(Key{Hash: tc.hash})
		require.NoError(t, err)
		assert.Equal(t, tc.want, ch, "hash %d", tc.hash)
	}
}

func TestDeriveRangeRoutesByNamespace(t *testing.T) {
	mid := EncodeNamespace("m")
	tbl, err := Derive(schemapb.RoutingMode_RoutingModeRange,
		[]string{"low", "high"},
		[]Shard{
			{Vchannel: "low", Ranges: []*schemapb.RoutingKeyRange{{Lower: nil, Upper: mid}}},
			{Vchannel: "high", Ranges: []*schemapb.RoutingKeyRange{{Lower: mid, Upper: nil}}},
		})
	require.NoError(t, err)
	assert.True(t, tbl.IsExplicit())
	assert.Equal(t, 2, tbl.NumShards())

	// Whichever side each namespace lands on, it must be one of the two and be
	// stable — the table tiles the key space.
	for _, ns := range []string{"a", "m", "z", "tenant-1"} {
		ch, err := tbl.Route(Key{Namespace: ns})
		require.NoError(t, err)
		assert.Contains(t, []string{"low", "high"}, ch, "ns %s", ns)
		again, err := tbl.Route(Key{Namespace: ns})
		require.NoError(t, err)
		assert.Equal(t, ch, again)
	}
	// The namespace at the boundary belongs to the upper (half-open) shard.
	ch, err := tbl.Route(Key{Namespace: "m"})
	require.NoError(t, err)
	assert.Equal(t, "high", ch)
}

func TestDeriveRejectsMalformedMeta(t *testing.T) {
	// A hash gap: only half the space is claimed.
	_, err := Derive(schemapb.RoutingMode_RoutingModeHash, []string{"c0"},
		[]Shard{{Vchannel: "c0", Buckets: []HashBucket{{Modulus: 2, Remainder: 0}}}})
	require.Error(t, err)

	// A range gap.
	_, err = Derive(schemapb.RoutingMode_RoutingModeRange, []string{"a"},
		[]Shard{{Vchannel: "a", Ranges: []*schemapb.RoutingKeyRange{{Lower: []byte("m"), Upper: nil}}}})
	require.Error(t, err)

	// An unknown mode.
	_, err = Derive(schemapb.RoutingMode(99), []string{"a"}, nil)
	require.Error(t, err)
}

func TestRouteErrorsWithoutChannels(t *testing.T) {
	tbl, err := Derive(schemapb.RoutingMode_RoutingModeHash, nil, nil)
	require.NoError(t, err)
	_, err = tbl.Route(Key{Hash: 1})
	require.Error(t, err)
}

func TestShardsFromMetaFiltersNonWritableShards(t *testing.T) {
	// A split in flight: the fenced source and a dropped one must be excluded,
	// leaving the targets as an exact cover.
	vchannels := []string{"src", "tgtA", "tgtB", "old"}
	infos := []*schemapb.CollectionShardInfo{
		{State: schemapb.ShardState_ShardSplitting, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []*schemapb.HashBucket{{Modulus: 1, Remainder: 0}}},
		}},
		{State: schemapb.ShardState_ShardCreating, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []*schemapb.HashBucket{{Modulus: 2, Remainder: 0}}},
		}},
		{State: schemapb.ShardState_ShardNormal, Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: []*schemapb.HashBucket{{Modulus: 2, Remainder: 1}}},
		}},
		{State: schemapb.ShardState_ShardDropped},
	}
	shards, err := ShardsFromMeta(vchannels, infos)
	require.NoError(t, err)
	require.Len(t, shards, 2, "only the writable targets survive")
	assert.Equal(t, "tgtA", shards[0].Vchannel)
	assert.Equal(t, "tgtB", shards[1].Vchannel)

	// And the survivors tile the space, so Derive accepts them.
	tbl, err := Derive(schemapb.RoutingMode_RoutingModeHash, vchannels, shards)
	require.NoError(t, err)
	ch, err := tbl.Route(Key{Hash: 0})
	require.NoError(t, err)
	assert.Equal(t, "tgtA", ch)
	ch, err = tbl.Route(Key{Hash: 1})
	require.NoError(t, err)
	assert.Equal(t, "tgtB", ch)
}

func TestShardsFromMetaRejectsLengthMismatch(t *testing.T) {
	_, err := ShardsFromMeta([]string{"a", "b"}, []*schemapb.CollectionShardInfo{{}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mismatches")
}

func TestModeIsReported(t *testing.T) {
	tbl, err := Derive(schemapb.RoutingMode_RoutingModeRange, []string{"a"},
		[]Shard{{Vchannel: "a", Ranges: []*schemapb.RoutingKeyRange{{}}}})
	require.NoError(t, err)
	assert.Equal(t, schemapb.RoutingMode_RoutingModeRange, tbl.Mode())
}
