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
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func int64IDs(pks ...int64) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}}
}

func strIDs(pks ...string) *schemapb.IDs {
	return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: pks}}}
}

func insertMsgOf(numRows int) *msgstream.InsertMsg {
	return &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{NumRows: uint64(numRows)}}
}

func splitShardInfo(state schemapb.ShardState, vchannel string, buckets ...uint64) *schemapb.CollectionShardInfo {
	info := &schemapb.CollectionShardInfo{State: state, VchannelName: vchannel}
	if len(buckets) > 0 {
		info.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: buckets},
		}
	}
	return info
}

// splitCollectionInfo is the proxy's cache entry of a collection as a describe
// returns it: never split when modulus is 0, split otherwise.
func splitCollectionInfo(collectionID int64, modulus uint64, vchannels []string, infos ...*schemapb.CollectionShardInfo) *collectionInfo {
	resp := &milvuspb.DescribeCollectionResponse{
		CollectionID:        collectionID,
		VirtualChannelNames: vchannels,
		RoutingModulus:      modulus,
		ShardInfos:          infos,
	}
	pchannels := make([]string, len(vchannels))
	for i, vchannel := range vchannels {
		pchannels[i] = vchannel + "-p"
	}
	return &collectionInfo{
		CollID:         collectionID,
		VChannels:      vchannels,
		PChannels:      pchannels,
		ShardInfos:     infos,
		RoutingModulus: modulus,
		SplitRouting:   metacache.NewSplitRouting(resp),
	}
}

// neverSplitRoutingCache is a cache whose collections have never been split
// and have vchannels, for write-path tests that are not about routing: the
// write path reads the collection's routing and channels on every attempt.
func neverSplitRoutingCache(t *testing.T, vchannels ...string) *MockCache {
	cache := NewMockCache(t)
	cache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&collectionInfo{VChannels: vchannels}, nil).Maybe()
	return cache
}

// A split of v0 of a two-shard collection: v0 owned residue 0 of 2, and its
// targets own 0 and 2 of 4 while the untouched v1 is re-expressed as {1, 3}.
func twoShardSplitInfo() *collectionInfo {
	return splitCollectionInfo(1, 4, []string{"v0", "v1", "v2", "v3"},
		splitShardInfo(schemapb.ShardState_ShardSplitting, "v0"),
		splitShardInfo(schemapb.ShardState_ShardNormal, "v1", 1, 3),
		splitShardInfo(schemapb.ShardState_ShardCreating, "v2", 0),
		splitShardInfo(schemapb.ShardState_ShardCreating, "v3", 2),
	)
}

func TestAssignChannelsByPKFollowsResiduesNotPosition(t *testing.T) {
	info := twoShardSplitInfo()
	table := info.SplitRouting.Table
	pks := make([]int64, 256)
	for i := range pks {
		pks[i] = int64(i*7919 + 1)
	}
	insertMsg := insertMsgOf(len(pks))

	got, err := assignChannelsByPK(table, int64IDs(pks...), info.VChannels, insertMsg)
	require.NoError(t, err)

	assert.NotContains(t, got, "v0", "the fenced source owns no residue")
	seen := 0
	for vchannel, offsets := range got {
		for _, offset := range offsets {
			owner, err := table.VChannelOfPK(pks[offset])
			require.NoError(t, err)
			assert.Equal(t, owner, vchannel, "pk %d", pks[offset])
			assert.Equal(t, vchannel, info.VChannels[insertMsg.HashValues[offset]])
			seen++
		}
	}
	assert.Equal(t, len(pks), seen)
}

// A never-split collection places every key exactly where the legacy modulo
// does, whether the write path takes the legacy branch (nil table) or routes
// through the residue table derived for it.
func TestAssignChannelsByPKKeepsTheLegacyPlacementOfANeverSplitCollection(t *testing.T) {
	rng := rand.New(rand.NewSource(20260918))
	for _, n := range []int{1, 2, 3, 16} {
		channels := make([]string, n)
		for i := range channels {
			channels[i] = fmt.Sprintf("by-dev-rootcoord-dml_%d_1v%d", i, i)
		}
		legacyTable, err := routing.TableFromMeta(channels, nil, 0)
		require.NoError(t, err)

		ints := make([]int64, 2000)
		strs := make([]string, 2000)
		for i := range ints {
			ints[i] = rng.Int63() - rng.Int63()
			strs[i] = fmt.Sprintf("key-%d", rng.Int63())
		}
		for _, ids := range []*schemapb.IDs{int64IDs(ints...), strIDs(strs...)} {
			want, err := typeutil.HashPK2Channels(ids, channels)
			require.NoError(t, err)

			viaNil, err := pkChannelIndexes(nil, ids, channels)
			require.NoError(t, err)
			assert.Equal(t, want, viaNil)

			viaTable, err := pkChannelIndexes(legacyTable, ids, channels)
			require.NoError(t, err)
			assert.Equal(t, want, viaTable, "n=%d", n)

			legacyMsg, tableMsg := insertMsgOf(len(want)), insertMsgOf(len(want))
			legacyOffsets, err := assignChannelsByPK(nil, ids, channels, legacyMsg)
			require.NoError(t, err)
			tableOffsets, err := assignChannelsByPK(legacyTable, ids, channels, tableMsg)
			require.NoError(t, err)
			assert.Equal(t, legacyOffsets, tableOffsets)
			assert.Equal(t, legacyMsg.HashValues, tableMsg.HashValues)
		}
	}
}

// Tombstones follow the same placement as the rows they delete, for a split
// collection and, bit for bit with the legacy modulo, for a never-split one.
func TestRepackDeleteMsgByHashFollowsTheInsertPlacement(t *testing.T) {
	info := twoShardSplitInfo()
	pks := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}

	insertOffsets, err := assignChannelsByPK(info.SplitRouting.Table, int64IDs(pks...), info.VChannels, insertMsgOf(len(pks)))
	require.NoError(t, err)
	result, rows, err := repackDeleteMsgByHash(context.Background(), info.SplitRouting.Table, int64IDs(pks...), info.VChannels,
		allocator.NewLocalAllocator(100, 200), 1000, 1, "collection", 2, "partition", "default", nil, nil)
	require.NoError(t, err)
	assert.EqualValues(t, len(pks), rows)
	for key, msgs := range result {
		vchannel := info.VChannels[key]
		for _, msg := range msgs {
			for _, pk := range msg.PrimaryKeys.GetIntId().GetData() {
				var owner string
				for channel, offsets := range insertOffsets {
					for _, offset := range offsets {
						if pks[offset] == pk {
							owner = channel
						}
					}
				}
				assert.Equal(t, owner, vchannel, "tombstone of pk %d", pk)
			}
		}
	}

	channels := []string{"a", "b", "c"}
	legacyTable, err := routing.TableFromMeta(channels, nil, 0)
	require.NoError(t, err)
	viaNil, _, err := repackDeleteMsgByHash(context.Background(), nil, int64IDs(pks...), channels,
		allocator.NewLocalAllocator(100, 200), 1000, 1, "collection", 2, "partition", "default", nil, nil)
	require.NoError(t, err)
	viaTable, _, err := repackDeleteMsgByHash(context.Background(), legacyTable, int64IDs(pks...), channels,
		allocator.NewLocalAllocator(100, 200), 1000, 1, "collection", 2, "partition", "default", nil, nil)
	require.NoError(t, err)
	require.Len(t, viaTable, len(viaNil))
	for key, msgs := range viaNil {
		require.Len(t, viaTable[key], len(msgs))
		for i := range msgs {
			assert.Equal(t, msgs[i].PrimaryKeys.GetIntId().GetData(), viaTable[key][i].PrimaryKeys.GetIntId().GetData())
		}
	}
}

func TestRepackPendingDeleteMsgsReportsTheKeysOfEachMessage(t *testing.T) {
	info := twoShardSplitInfo()
	pks := []int64{1, 2, 3, 4, 5, 6, 7, 8}
	pending := newRowSet([]int{0, 2, 4, 6})
	result, offsets, rows, err := repackPendingDeleteMsgs(context.Background(), info.SplitRouting.Table, int64IDs(pks...), pending,
		info.VChannels, allocator.NewLocalAllocator(100, 200), 1000, 1, "collection", 2, "partition", "default", nil, nil)
	require.NoError(t, err)
	assert.EqualValues(t, 4, rows)
	var packed []int
	for _, msgs := range result {
		for _, msg := range msgs {
			carried := offsets[msg]
			require.Len(t, carried, int(msg.NumRows))
			for i, offset := range carried {
				assert.Equal(t, pks[offset], msg.PrimaryKeys.GetIntId().GetData()[i])
			}
			packed = append(packed, carried...)
		}
	}
	assert.ElementsMatch(t, []int{0, 2, 4, 6}, packed)
}

func TestPKChannelIndexesRefusesWhatItCannotPlace(t *testing.T) {
	info := twoShardSplitInfo()
	_, err := pkChannelIndexes(info.SplitRouting.Table, int64IDs(1), nil)
	assert.ErrorIs(t, err, common.ErrRoutingTableNoValues)

	// The table names a vchannel the list does not carry: a Milvus bug, since
	// both come from one describe.
	_, err = pkChannelIndexes(info.SplitRouting.Table, int64IDs(1, 2, 3, 4, 5, 6, 7, 8), []string{"v0", "v1"})
	assert.ErrorIs(t, err, merr.ErrServiceInternal)

	_, err = pkChannelIndexes(splitCollectionInfo(1, 2, []string{"v0", "v1"},
		splitShardInfo(schemapb.ShardState_ShardNormal, "v0", 0),
		splitShardInfo(schemapb.ShardState_ShardNormal, "v1", 1),
	).SplitRouting.Table, &schemapb.IDs{}, []string{"v0", "v1"})
	assert.NoError(t, err, "a batch with no key field set places nothing")
}

func TestSplitRoutingOf(t *testing.T) {
	split, err := splitRoutingOf(nil)
	assert.NoError(t, err)
	assert.Nil(t, split)

	split, err = splitRoutingOf(splitCollectionInfo(1, 0, []string{"v0"}))
	assert.NoError(t, err)
	assert.Nil(t, split, "a never-split collection has no split routing")

	split, err = splitRoutingOf(twoShardSplitInfo())
	assert.NoError(t, err)
	require.NotNil(t, split)
	assert.Equal(t, []string{"v0"}, split.Fenced)

	malformed := splitCollectionInfo(1, 4, []string{"v0", "v1"},
		splitShardInfo(schemapb.ShardState_ShardNormal, "v0", 0),
		splitShardInfo(schemapb.ShardState_ShardNormal, "v1", 1))
	_, err = splitRoutingOf(malformed)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.False(t, merr.IsRetryableErr(err))

	noCause := &collectionInfo{CollID: 1, RoutingModulus: 2, SplitRouting: &metacache.SplitRouting{}}
	_, err = splitRoutingOf(noCause)
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestResolveWriteRoute(t *testing.T) {
	ctx := context.Background()
	legacyInfo := splitCollectionInfo(1, 0, []string{"v0", "v1"})
	cache := NewMockCache(t)

	t.Run("never split routes by the channel list of the same lookup", func(t *testing.T) {
		cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "c", int64(1)).Return(legacyInfo, nil).Once()
		route, err := resolveWriteRoute(ctx, cache, "db", "c", 1)
		require.NoError(t, err)
		assert.False(t, route.split())
		assert.Equal(t, []string{"v0", "v1"}, route.vchannels)
		assert.Equal(t, []string{"v0", "v1"}, route.writable)
		assert.Empty(t, route.fenced)
	})

	t.Run("a split collection routes by the list its table was derived from", func(t *testing.T) {
		cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "c", int64(1)).Return(twoShardSplitInfo(), nil).Once()
		route, err := resolveWriteRoute(ctx, cache, "db", "c", 1)
		require.NoError(t, err)
		assert.True(t, route.split())
		assert.Equal(t, []string{"v0", "v1", "v2", "v3"}, route.vchannels)
		assert.Equal(t, []string{"v1", "v2", "v3"}, route.writable)
		assert.Equal(t, []string{"v0"}, route.fenced)
	})

	t.Run("errors", func(t *testing.T) {
		cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "c", int64(1)).Return(nil, errors.New("describe failed")).Once()
		_, err := resolveWriteRoute(ctx, cache, "db", "c", 1)
		assert.ErrorContains(t, err, "describe failed")

		cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "c", int64(1)).Return(&collectionInfo{}, nil).Once()
		_, err = resolveWriteRoute(ctx, cache, "db", "c", 1)
		assert.ErrorIs(t, err, merr.ErrServiceInternal, "a collection always has a vchannel")

		cache.EXPECT().GetCollectionInfo(mock.Anything, "db", "c", int64(1)).Return(
			splitCollectionInfo(1, 4, []string{"v0"}, splitShardInfo(schemapb.ShardState_ShardNormal, "v0", 0)), nil).Once()
		_, err = resolveWriteRoute(ctx, cache, "db", "c", 1)
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}

// A route places rows by the table's modulus once the collection is split, by
// the shard count before.
func TestWriteRouteModulus(t *testing.T) {
	assert.EqualValues(t, 3, legacyWriteRoute([]string{"a", "b", "c"}).modulus())
	info := twoShardSplitInfo()
	assert.EqualValues(t, 4, newSplitWriteRoute(info.VChannels, info.SplitRouting).modulus())
}

// stabilizeAutoIDs draws int64 auto ids for route the way an idempotent
// auto-id insert does.
func stabilizeAutoIDs(ids []int64, route *writeRoute, alloc func(uint32) (int64, int64, error)) error {
	return reassignAutoIDByResidue(ids, schemapb.DataType_Int64, newAutoIDPlacement(route), 0, alloc)
}

// Row offset i lands on the shard owning residue i % M -- and a split, which
// only refines residues, leaves the offsets of the shards it did not touch
// where they were.
func TestReassignAutoIDByResidueKeepsTheOffsetsOfAnUntouchedShard(t *testing.T) {
	info := twoShardSplitInfo()
	route := newSplitWriteRoute(info.VChannels, info.SplitRouting)
	rowIDs := make([]int64, 40)
	next := int64(1 << 20)
	alloc := func(count uint32) (int64, int64, error) {
		begin := next
		next += int64(count)
		return begin, next, nil
	}
	require.NoError(t, stabilizeAutoIDs(rowIDs, route, alloc))
	for i, id := range rowIDs {
		owner, err := route.table.VChannelOfPK(id)
		require.NoError(t, err)
		if i%2 == 1 {
			assert.Equal(t, "v1", owner, "the untouched shard keeps offset %d", i)
		} else {
			assert.Contains(t, []string{"v2", "v3"}, owner, "the split shard's offset %d goes to a target", i)
		}
	}
}

// The placement of a never-split collection is one residue per shard, in
// channel order; a split one follows its table.
func TestNewAutoIDPlacement(t *testing.T) {
	legacy := newAutoIDPlacement(legacyWriteRoute([]string{"a", "b", "c"}))
	assert.EqualValues(t, 3, legacy.modulus)
	assert.Equal(t, []int{0, 1, 2}, legacy.ownerOf)
	assert.Equal(t, []int{1, 1, 1}, legacy.share)

	info := twoShardSplitInfo()
	split := newAutoIDPlacement(newSplitWriteRoute(info.VChannels, info.SplitRouting))
	assert.EqualValues(t, 4, split.modulus)
	assert.Equal(t, 3, split.owners)
	assert.Equal(t, split.ownerOf[1], split.ownerOf[3], "v1 owns residues 1 and 3")
	assert.Equal(t, 2, split.share[split.ownerOf[1]])
}

// hotShardSplitInfo is a collection whose single hot shard was split six
// times: M = 64, seven shards. Residue r belongs to the shard named by its
// lowest set bit, so the shares are 1/2, 1/4, ..., 1/64 and 1/64 (residue 0).
func hotShardSplitInfo() *collectionInfo {
	const modulus = 64
	vchannels := make([]string, 7)
	buckets := make([][]uint64, 7)
	for i := range vchannels {
		vchannels[i] = fmt.Sprintf("hot-v%d", i)
	}
	for r := uint64(0); r < modulus; r++ {
		owner := 0
		if r != 0 {
			for r>>owner&1 == 0 {
				owner++
			}
			owner++
		}
		buckets[owner] = append(buckets[owner], r)
	}
	infos := make([]*schemapb.CollectionShardInfo, len(vchannels))
	for i, vchannel := range vchannels {
		infos[i] = splitShardInfo(schemapb.ShardState_ShardNormal, vchannel, buckets[i]...)
	}
	return splitCollectionInfo(1, modulus, vchannels, infos...)
}

// An id only has to route to the owner of residue i % M, so a row costs about
// 1/share draws: a 100-row insert into a collection whose hot shard was split
// six times allocates about the sum over its rows of 1/share, not 100 x 64.
func TestAutoIDBucketingAllocatesByTheOwnersShare(t *testing.T) {
	info := hotShardSplitInfo()
	route := newSplitWriteRoute(info.VChannels, info.SplitRouting)
	const rows = 100
	shares := make(map[string]int)
	for r := uint64(0); r < route.modulus(); r++ {
		owner, _ := route.table.Lookup(r)
		shares[owner]++
	}
	expected := 0.0
	for i := 0; i < rows; i++ {
		owner, _ := route.table.Lookup(uint64(i) % route.modulus())
		expected += float64(route.modulus()) / float64(shares[owner])
	}

	rowIDs := make([]int64, rows)
	next := int64(1 << 30)
	for i := range rowIDs {
		rowIDs[i] = next
		next++
	}
	allocated := 0
	alloc := func(count uint32) (int64, int64, error) {
		allocated += int(count)
		begin := next
		next += int64(count)
		return begin, next, nil
	}
	require.NoError(t, stabilizeAutoIDs(rowIDs, route, alloc))

	t.Logf("allocated %d ids, sum of 1/share over the rows is %.0f", allocated, expected)
	// expected is a loose upper bound (it assumes every row still needs a fresh
	// draw, ignoring the free candidates the rowIDs above already supply), so
	// allocated is normally well under it. The bound below is tighter than that
	// slack on purpose: a mutant that drops the /share divisor in allocCount --
	// charging a short owner the full modulus per missing row instead of
	// modulus/share -- inflates this run's allocation from 140 to 256 ids
	// (owners with share > 1 no longer draw at a discount), which this bound
	// catches while leaving the correct implementation's 140 comfortably inside
	// it.
	assert.Less(t, float64(allocated), expected/4)
	for i, id := range rowIDs {
		owner, err := route.table.VChannelOfPK(id)
		require.NoError(t, err)
		want, _ := route.table.Lookup(uint64(i) % route.modulus())
		assert.Equal(t, want, owner, "offset %d", i)
	}
}
