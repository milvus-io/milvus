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

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type reliefShard struct {
	vchannel string
	residues []uint64
	size     int64
}

func TestSiblingResidue(t *testing.T) {
	// The two halves a doubling produces: r and r+M/2 at modulus M.
	sibling, ok := siblingResidue(4, 0)
	require.True(t, ok)
	assert.EqualValues(t, 2, sibling)

	sibling, ok = siblingResidue(4, 2)
	require.True(t, ok)
	assert.EqualValues(t, 0, sibling)

	// The relation is symmetric, which is what lets either half ask about the
	// other without knowing which one was the original.
	for _, r := range []uint64{0, 1, 2, 3, 4, 5, 6, 7} {
		other, ok := siblingResidue(8, r)
		require.True(t, ok)
		back, ok := siblingResidue(8, other)
		require.True(t, ok)
		assert.Equal(t, r, back)
	}

	// An odd modulus has no doubling in its ancestry: a rehash to an odd shard
	// count carves every shard from every source, so there is no parent half to
	// compare against.
	_, ok = siblingResidue(3, 1)
	assert.False(t, ok)
	_, ok = siblingResidue(1, 0)
	assert.False(t, ok)
	_, ok = siblingResidue(0, 0)
	assert.False(t, ok)
}

// reliefMeta builds a collection whose shards own the given buckets, each
// holding one segment of the given size.
func reliefMeta(t *testing.T, modulus uint64, shards []reliefShard) (*shardSplitManager, *collectionInfo) {
	m := &meta{
		segments:    NewSegmentsInfo(),
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		channelCPs:  newChannelCps(),
	}
	collection := &collectionInfo{
		ID: 1, RoutingModulus: modulus,
		VChannelNames: []string{}, ShardInfos: map[string]*schemapb.CollectionShardInfo{},
	}
	for i, shard := range shards {
		collection.VChannelNames = append(collection.VChannelNames, shard.vchannel)
		collection.ShardInfos[shard.vchannel] = residueShardInfoTestPB(shard.vchannel, shard.residues)
		if shard.size > 0 {
			m.segments.SetSegment(int64(1000+i), &SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					ID: int64(1000 + i), CollectionID: 1, InsertChannel: shard.vchannel,
					State: commonpb.SegmentState_Flushed, NumOfRows: shard.size,
					Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: shard.size}}}},
				},
			})
		}
	}
	m.collections.Insert(1, collection)
	mgr, _ := newHashSplitTestManager(t, m)
	return mgr, collection
}

func residueShardInfoTestPB(vchannel string, residues []uint64) *schemapb.CollectionShardInfo {
	return &schemapb.CollectionShardInfo{
		VchannelName: vchannel,
		State:        schemapb.ShardState_ShardNormal,
		Routing: &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: residues},
		},
	}
}

func TestDoublingRelievedNothingWhenTheSiblingHalfIsEmpty(t *testing.T) {
	// The pathological workload: one key inserted enough times to dominate the
	// shard. Every copy hashes the same, so the doubling put all of it on one
	// half and left the other empty. Doubling again does the same.
	mgr, collection := reliefMeta(t, 4, []reliefShard{
		{"hot-v2", []uint64{0}, 1_000_000},
		{"empty-v3", []uint64{2}, 0},
		{"other-v0", []uint64{1, 3}, 10},
	})
	assert.True(t, mgr.doublingRelievedNothing(collection, "hot-v2", 1_000_000))
	// and the empty half is not itself refused — it is not over any threshold,
	// but the predicate must not claim otherwise about a shard with no data.
	assert.False(t, mgr.doublingRelievedNothing(collection, "empty-v3", 0))
}

func TestDoublingIsAllowedWhenTheHalvesAreComparable(t *testing.T) {
	// An ordinary over-threshold shard: its sibling holds a comparable share, so
	// the last doubling worked and the next one will too.
	mgr, collection := reliefMeta(t, 4, []reliefShard{
		{"v2", []uint64{0}, 1_000_000},
		{"v3", []uint64{2}, 900_000},
		{"other", []uint64{1, 3}, 10},
	})
	assert.False(t, mgr.doublingRelievedNothing(collection, "v2", 1_000_000))
}

func TestDoublingIsAllowedWhenTheSiblingWasItselfSplit(t *testing.T) {
	// The sibling half may have been doubled again since, so its share is held
	// by several shards. Summing them is what keeps this from reading as "the
	// sibling is empty" and refusing a perfectly good split.
	mgr, collection := reliefMeta(t, 8, []reliefShard{
		{"v2", []uint64{0, 4}, 1_000_000},
		// the residue-2 half of modulus 4, doubled again into 2 and 6 at 8
		{"v4", []uint64{2}, 500_000},
		{"v5", []uint64{6}, 400_000},
		{"other", []uint64{1, 3, 5, 7}, 10},
	})
	// v2 owns two residues, so it is halved by dividing that set rather than by
	// doubling -- the runaway this guard exists for cannot happen, and it must
	// not block the split.
	assert.False(t, mgr.doublingRelievedNothing(collection, "v2", 1_000_000))
}

func TestDoublingIsAllowedWithoutADoublingAncestry(t *testing.T) {
	// A rehash to an odd shard count: no parent half exists to compare against,
	// so the guard has nothing to say and must not block the split.
	mgr, collection := reliefMeta(t, 3, []reliefShard{
		{"v0", []uint64{0}, 1_000_000},
		{"v1", []uint64{1}, 10},
		{"v2", []uint64{2}, 10},
	})
	assert.False(t, mgr.doublingRelievedNothing(collection, "v0", 1_000_000))
}

func TestDoublingGuardCanBeDisabled(t *testing.T) {
	mgr, collection := reliefMeta(t, 4, []reliefShard{
		{"hot-v2", []uint64{0}, 1_000_000},
		{"empty-v3", []uint64{2}, 0},
		{"other", []uint64{1, 3}, 10},
	})
	key := Params.DataCoordCfg.ShardSplitMinSiblingRatio.Key
	Params.Save(key, "0")
	defer Params.Reset(key)
	assert.False(t, mgr.doublingRelievedNothing(collection, "hot-v2", 1_000_000))
}
