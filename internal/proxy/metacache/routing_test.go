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

package metacache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func shardInfo(state schemapb.ShardState, vchannel string, buckets ...uint64) *schemapb.CollectionShardInfo {
	si := &schemapb.CollectionShardInfo{State: state, VchannelName: vchannel}
	if len(buckets) > 0 {
		si.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: buckets},
		}
	}
	return si
}

// A collection that has never been split carries no shard infos; the table
// derived for it is the legacy hash % shardNum by position.
func TestBuildRoutingTableWithoutShardInfos(t *testing.T) {
	table, err := buildRoutingTable(0, []string{"v0", "v1"}, nil)
	require.NoError(t, err)
	require.NotNil(t, table)
	assert.False(t, table.IsExplicit())

	for hash := uint64(0); hash < 10; hash++ {
		got, err := table.Route(hash)
		require.NoError(t, err)
		assert.Equal(t, []string{"v0", "v1"}[hash%2], got)
	}
}

func TestBuildRoutingTableFromResidues(t *testing.T) {
	vchannels := []string{"src", "t0", "t1"}
	infos := []*schemapb.CollectionShardInfo{
		// the fenced source is excluded, so what remains must still tile
		shardInfo(schemapb.ShardState_ShardSplitting, "src", 0, 1),
		shardInfo(schemapb.ShardState_ShardCreating, "t0", 0),
		shardInfo(schemapb.ShardState_ShardCreating, "t1", 1),
	}

	table, err := buildRoutingTable(2, vchannels, infos)
	require.NoError(t, err)
	require.True(t, table.IsExplicit())

	got, err := table.Route(0)
	require.NoError(t, err)
	assert.Equal(t, "t0", got)
	got, err = table.Route(1)
	require.NoError(t, err)
	assert.Equal(t, "t1", got)
}

func TestBuildRoutingTableRejectsMalformedMeta(t *testing.T) {
	vchannels := []string{"v0", "v1"}

	// a gap: residues 1 and 3 are unowned at modulus 4.
	_, err := buildRoutingTable(4, vchannels, []*schemapb.CollectionShardInfo{
		shardInfo(schemapb.ShardState_ShardNormal, "v0", 0),
		shardInfo(schemapb.ShardState_ShardNormal, "v1", 2),
	})
	assert.Error(t, err)

	// residues present but no modulus to read them against.
	_, err = buildRoutingTable(0, vchannels, []*schemapb.CollectionShardInfo{
		shardInfo(schemapb.ShardState_ShardNormal, "v0", 0),
		shardInfo(schemapb.ShardState_ShardNormal, "v1", 1),
	})
	assert.Error(t, err)

	// shard infos not parallel to the vchannel list.
	_, err = buildRoutingTable(2, vchannels, []*schemapb.CollectionShardInfo{
		shardInfo(schemapb.ShardState_ShardNormal, "v0", 0),
	})
	assert.Error(t, err)
}

func TestAttachRoutingSetsTheTable(t *testing.T) {
	collection := &milvuspb.DescribeCollectionResponse{
		CollectionID:        1,
		RoutingModulus:      2,
		ShardBy:             "hash(pk)",
		VirtualChannelNames: []string{"v0", "v1"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardNormal, "v0", 0),
			shardInfo(schemapb.ShardState_ShardNormal, "v1", 1),
		},
	}
	info := attachRouting(context.Background(), &CollectionInfo{CollID: 1}, collection)
	require.NotNil(t, info.RoutingTable)
	assert.True(t, info.RoutingTable.IsExplicit())
}

// Malformed routing meta must not fail the describe: the entry is served with a
// nil table, and the write path then keeps the legacy modulo rather than
// dereferencing it.
func TestAttachRoutingLeavesTheTableNilOnMalformedMeta(t *testing.T) {
	collection := &milvuspb.DescribeCollectionResponse{
		CollectionID:        1,
		RoutingModulus:      4,
		VirtualChannelNames: []string{"v0", "v1"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardNormal, "v0", 0),
			shardInfo(schemapb.ShardState_ShardNormal, "v1", 2),
		},
	}
	info := attachRouting(context.Background(), &CollectionInfo{CollID: 1}, collection)
	assert.Nil(t, info.RoutingTable)
}

// The two fields ride verbatim from DescribeCollection, and their zero values
// are exactly "never split, never declared".
func TestNewCollectionInfoCarriesRoutingFacts(t *testing.T) {
	schemaInfo, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "col"})
	require.NoError(t, err)

	info := newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		CollectionID:   7,
		RoutingModulus: 8,
		ShardBy:        "hash(pk)",
		Schema:         &schemapb.CollectionSchema{Name: "col"},
	}, schemaInfo, false, "")
	assert.EqualValues(t, 8, info.RoutingModulus)
	assert.Equal(t, "hash(pk)", info.ShardBy)

	legacy := newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		CollectionID: 7,
		Schema:       &schemapb.CollectionSchema{Name: "col"},
	}, schemaInfo, false, "")
	assert.Zero(t, legacy.RoutingModulus)
	assert.Empty(t, legacy.ShardBy)
}
