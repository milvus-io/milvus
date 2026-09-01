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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// splitTable is the topology a doubling split leaves behind: modulus 4, the
// untouched shard rebased onto {1,3}, and the two targets owning {0} and {2}.
func splitTable(t *testing.T, channels []string) *routing.Table {
	t.Helper()
	table, err := routing.Derive(4, channels, []routing.Shard{
		{Vchannel: channels[0], Buckets: []uint64{1, 3}},
		{Vchannel: channels[1], Buckets: []uint64{0}},
		{Vchannel: channels[2], Buckets: []uint64{2}},
	})
	require.NoError(t, err)
	require.True(t, table.IsExplicit())
	return table
}

func int64IDs(pks ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}}
}

func insertMsgOf(numRows int) *msgstream.InsertMsg {
	return &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{NumRows: uint64(numRows)}}
}

// The reason the write path cannot keep using a modulo over the channel list:
// after a split the modulus no longer equals the shard count, so routing by
// position sends keys to shards that do not own them.
func TestAssignChannelsByPKFollowsResiduesNotPosition(t *testing.T) {
	channels := []string{"v0", "v1", "v2"}
	table := splitTable(t, channels)

	pks := make([]int64, 0, 200)
	for i := int64(0); i < 200; i++ {
		pks = append(pks, i)
	}
	msg := insertMsgOf(len(pks))

	offsets, err := assignChannelsByPK(table, int64IDs(pks...), channels, msg)
	require.NoError(t, err)

	placed := 0
	for vchannel, rows := range offsets {
		placed += len(rows)
		for _, row := range rows {
			h, err := typeutil.Hash32Int64(pks[row])
			require.NoError(t, err)
			switch vchannel {
			case "v0":
				assert.Contains(t, []uint32{1, 3}, h%4, "v0 owns {1,3} mod 4")
			case "v1":
				assert.Equal(t, uint32(0), h%4, "v1 owns {0} mod 4")
			case "v2":
				assert.Equal(t, uint32(2), h%4, "v2 owns {2} mod 4")
			default:
				t.Fatalf("row routed to unknown shard %q", vchannel)
			}
		}
	}
	assert.Equal(t, len(pks), placed, "every row is placed exactly once")
}

// Guards the test above against being vacuous: if residue routing happened to
// agree with hash%3 there would be nothing to fix.
func TestAssignChannelsByPKDisagreesWithTheLegacyModulo(t *testing.T) {
	channels := []string{"v0", "v1", "v2"}
	pks := make([]int64, 0, 200)
	for i := int64(0); i < 200; i++ {
		pks = append(pks, i)
	}

	byResidues, err := assignChannelsByPK(splitTable(t, channels), int64IDs(pks...), channels, insertMsgOf(len(pks)))
	require.NoError(t, err)
	byPosition, err := assignChannelsByPK(nil, int64IDs(pks...), channels, insertMsgOf(len(pks)))
	require.NoError(t, err)
	assert.NotEqual(t, byPosition, byResidues)
}

// A collection that has never been split carries no residues, and its routing
// must not move by so much as one row.
func TestAssignChannelsByPKKeepsLegacyBitForBit(t *testing.T) {
	channels := []string{"v0", "v1", "v2"}
	pks := int64IDs(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	legacyTable, err := routing.Derive(0, channels, nil)
	require.NoError(t, err)
	require.False(t, legacyTable.IsExplicit())

	msgA, msgB := insertMsgOf(10), insertMsgOf(10)
	got, err := assignChannelsByPK(legacyTable, pks, channels, msgA)
	require.NoError(t, err)
	want, err := assignChannelsByPK(nil, pks, channels, msgB)
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Equal(t, msgB.HashValues, msgA.HashValues)
}

func TestAssignChannelsByPKRejectsAnEmptyChannelSet(t *testing.T) {
	_, err := assignChannelsByPK(splitTable(t, []string{"v0", "v1", "v2"}), int64IDs(1), nil, insertMsgOf(1))
	assert.Error(t, err)
}

// A collection whose shards do not tile the key space is refused at derivation,
// so the write path never sees one. Derive is the guard, and it is asserted here
// because a table that silently accepted a gap would let the proxy place rows on
// a shard that does not own them.
func TestDeriveRefusesATopologyThatDoesNotTile(t *testing.T) {
	_, err := routing.Derive(4, []string{"v0", "v1"}, []routing.Shard{
		{Vchannel: "v0", Buckets: []uint64{0}},
		{Vchannel: "v1", Buckets: []uint64{2}},
	})
	assert.Error(t, err, "residues 1 and 3 are unowned")
}

// A namespace request lands wholly on one shard either way; what the routing
// table changes is WHICH one.
func TestAssignChannelsByNamespaceFollowsTheTable(t *testing.T) {
	channels := []string{"v0", "v1", "v2"}
	table := splitTable(t, channels)

	const ns = "tenant-42"
	msg := insertMsgOf(5)
	offsets, err := assignChannelsByNamespace(table, ns, channels, msg)
	require.NoError(t, err)
	require.Len(t, offsets, 1, "one namespace lands on one shard")

	var got string
	for vchannel := range offsets {
		got = vchannel
	}
	want, err := table.Route(uint64(typeutil.HashString2Uint32(ns)))
	require.NoError(t, err)
	assert.Equal(t, want, got)
	assert.Len(t, offsets[got], 5, "every row of the request goes there")
}

// The two paths hash the same value with the same function, so a namespace
// collection that has never split places exactly where it always did.
func TestAssignChannelsByNamespaceKeepsLegacyBitForBit(t *testing.T) {
	channels := []string{"v0", "v1", "v2"}
	for _, ns := range []string{"a", "tenant-1", "zzz", ""} {
		msgA, msgB := insertMsgOf(3), insertMsgOf(3)
		got, err := assignChannelsByNamespace(nil, ns, channels, msgA)
		require.NoError(t, err)

		legacy := typeutil.HashNamespace2Channels(ns, channels)
		want := assignChannelsByChannel(legacy, channels, msgB)
		assert.Equal(t, want, got, "namespace %q", ns)
	}
}

func TestAssignChannelsByNamespaceRejectsAnEmptyChannelSet(t *testing.T) {
	_, err := assignChannelsByNamespace(nil, "ns", nil, insertMsgOf(1))
	assert.Error(t, err)
}

// The routing table naming a shard the caller did not resolve means the two
// views of the topology disagree; placing the request anywhere else would put a
// whole namespace on a shard that does not own it.
func TestAssignChannelsByNamespaceRejectsAShardOutsideTheChannelSet(t *testing.T) {
	channels := []string{"v0", "v1", "v2"}
	table := splitTable(t, channels)

	const ns = "tenant-42"
	owner, err := table.Route(uint64(typeutil.HashString2Uint32(ns)))
	require.NoError(t, err)

	narrowed := make([]string, 0, 2)
	for _, ch := range channels {
		if ch != owner {
			narrowed = append(narrowed, ch)
		}
	}
	_, err = assignChannelsByNamespace(table, ns, narrowed, insertMsgOf(1))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in the request's channel set")
}

func TestRoutingOfToleratesANilCollectionInfo(t *testing.T) {
	assert.Nil(t, routingOf(nil))
	assert.False(t, routingOf(nil).IsExplicit())
}

// A delete must land on the shard holding the row: a tombstone placed by
// position would delete nothing, and one sent to a fenced source is rejected.
func TestDeleteHashValuesFollowTheSameShardAsTheInsert(t *testing.T) {
	channels := []string{"v0", "v1", "v2"}
	table := splitTable(t, channels)

	pks := make([]int64, 0, 64)
	for i := int64(0); i < 64; i++ {
		pks = append(pks, i)
	}
	ids := int64IDs(pks...)

	positions, err := deleteHashValues(table, ids, channels)
	require.NoError(t, err)
	require.Len(t, positions, len(pks))

	offsets, err := assignChannelsByPK(table, ids, channels, insertMsgOf(len(pks)))
	require.NoError(t, err)
	for vchannel, rows := range offsets {
		for _, row := range rows {
			assert.Equal(t, vchannel, channels[positions[row]],
				"a delete must land on the same shard as its insert")
		}
	}
}

func TestDeleteHashValuesKeepLegacyBitForBit(t *testing.T) {
	channels := []string{"v0", "v1", "v2"}
	ids := int64IDs(1, 2, 3, 4, 5)

	got, err := deleteHashValues(nil, ids, channels)
	require.NoError(t, err)
	want, err := typeutil.HashPK2Channels(ids, channels)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}
