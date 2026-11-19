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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func shardInfo(state schemapb.ShardState, vchannel string, buckets ...uint64) *schemapb.CollectionShardInfo {
	info := &schemapb.CollectionShardInfo{State: state, VchannelName: vchannel}
	if len(buckets) > 0 {
		info.Routing = &schemapb.CollectionShardInfo_HashRouting{
			HashRouting: &schemapb.HashRouting{Buckets: buckets},
		}
	}
	return info
}

// A collection that has never been split has no split routing: the write path
// keeps the legacy placement for it.
func TestNewSplitRoutingOfANeverSplitCollection(t *testing.T) {
	assert.Nil(t, NewSplitRouting(&milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1"},
	}))
	assert.Nil(t, NewSplitRouting(&milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1"},
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardNormal, "v0"),
			shardInfo(schemapb.ShardState_ShardNormal, "v1"),
		},
	}))
}

// After a split the collection lists the shards that take DML, each with its
// residues: here an unsplit shard next to the two targets of a split.
func TestNewSplitRoutingOfASplitCollection(t *testing.T) {
	split := NewSplitRouting(&milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v1", "v2", "v3"},
		RoutingModulus:      4,
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardNormal, "v1", 1, 3),
			shardInfo(schemapb.ShardState_ShardNormal, "v2", 0),
			shardInfo(schemapb.ShardState_ShardNormal, "v3", 2),
		},
	})
	require.NotNil(t, split)
	require.NoError(t, split.Err)
	require.NotNil(t, split.Table)
	assert.EqualValues(t, 4, split.Table.Modulus())
	for residue, want := range []string{"v2", "v1", "v3", "v1"} {
		got, ok := split.Table.Lookup(uint64(residue))
		require.True(t, ok)
		assert.Equal(t, want, got, "residue %d", residue)
	}
}

// The Normal shard infos are parallel to the vchannel list. A list that names
// other vchannels, a shard in another state than Normal or Splitting, a split
// source listed in the vchannel list, twice, or with no name, is malformed: the
// source of a split is not a shard of the collection any more.
func TestNewSplitRoutingRefusesShardInfosThatDoNotMatchTheVChannelList(t *testing.T) {
	for name, resp := range map[string]*milvuspb.DescribeCollectionResponse{
		"another vchannel": {
			VirtualChannelNames: []string{"v1", "v2"},
			RoutingModulus:      2,
			ShardInfos: []*schemapb.CollectionShardInfo{
				shardInfo(schemapb.ShardState_ShardNormal, "v1", 0),
				shardInfo(schemapb.ShardState_ShardNormal, "v9", 1),
			},
		},
		"another order": {
			VirtualChannelNames: []string{"v1", "v2"},
			RoutingModulus:      2,
			ShardInfos: []*schemapb.CollectionShardInfo{
				shardInfo(schemapb.ShardState_ShardNormal, "v2", 1),
				shardInfo(schemapb.ShardState_ShardNormal, "v1", 0),
			},
		},
		"a shard that is not normal": {
			VirtualChannelNames: []string{"v0", "v1", "v2"},
			RoutingModulus:      2,
			ShardInfos: []*schemapb.CollectionShardInfo{
				shardInfo(schemapb.ShardState_ShardSplitting, "v0"),
				shardInfo(schemapb.ShardState_ShardNormal, "v1", 0),
				shardInfo(schemapb.ShardState_ShardNormal, "v2", 1),
			},
		},
		"a shard that is being created": {
			VirtualChannelNames: []string{"v1", "v2"},
			RoutingModulus:      2,
			ShardInfos: []*schemapb.CollectionShardInfo{
				shardInfo(schemapb.ShardState_ShardCreating, "v1", 0),
				shardInfo(schemapb.ShardState_ShardNormal, "v2", 1),
			},
		},
		"a split source with no vchannel": {
			VirtualChannelNames: []string{"v1", "v2"},
			RoutingModulus:      2,
			ShardInfos: []*schemapb.CollectionShardInfo{
				shardInfo(schemapb.ShardState_ShardNormal, "v1", 0),
				shardInfo(schemapb.ShardState_ShardNormal, "v2", 1),
				shardInfo(schemapb.ShardState_ShardSplitting, ""),
			},
		},
		"a split source listed twice": {
			VirtualChannelNames: []string{"v1", "v2"},
			RoutingModulus:      2,
			ShardInfos: []*schemapb.CollectionShardInfo{
				shardInfo(schemapb.ShardState_ShardNormal, "v1", 0),
				shardInfo(schemapb.ShardState_ShardNormal, "v2", 1),
				shardInfo(schemapb.ShardState_ShardSplitting, "v0"),
				shardInfo(schemapb.ShardState_ShardSplitting, "v0"),
			},
		},
		"a split source that also takes DML": {
			VirtualChannelNames: []string{"v1", "v2"},
			RoutingModulus:      2,
			ShardInfos: []*schemapb.CollectionShardInfo{
				shardInfo(schemapb.ShardState_ShardNormal, "v1", 0),
				shardInfo(schemapb.ShardState_ShardNormal, "v2", 1),
				shardInfo(schemapb.ShardState_ShardSplitting, "v1"),
			},
		},
		"no shard info": {
			VirtualChannelNames: []string{"v1", "v2"},
			RoutingModulus:      2,
		},
	} {
		t.Run(name, func(t *testing.T) {
			split := NewSplitRouting(resp)
			require.NotNil(t, split)
			assert.Nil(t, split.Table)
			assert.ErrorIs(t, split.Err, merr.ErrServiceInternal)
		})
	}
}

// RootCoord lists every split source of the collection as a shard info in
// state ShardSplitting next to the shards that take DML. The sources never
// take part in the routing; they are what a keyed insert asks. A chain of
// splits -- v0 into v1 and v2, then v1 into v3 and v4 -- lists both v0 and v1,
// each with the residues it owned at the current modulus when RootCoord says.
func TestNewSplitRoutingSeparatesTheSplitSourcesFromTheShards(t *testing.T) {
	split := NewSplitRouting(&milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v2", "v3", "v4"},
		RoutingModulus:      4,
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardSplitting, "v0"),
			shardInfo(schemapb.ShardState_ShardNormal, "v2", 1, 3),
			shardInfo(schemapb.ShardState_ShardSplitting, "v1", 0, 2),
			shardInfo(schemapb.ShardState_ShardNormal, "v3", 0),
			shardInfo(schemapb.ShardState_ShardNormal, "v4", 2),
		},
	})
	require.NotNil(t, split)
	require.NoError(t, split.Err)
	require.NotNil(t, split.Table)
	for residue, want := range []string{"v3", "v2", "v4", "v2"} {
		got, ok := split.Table.Lookup(uint64(residue))
		require.True(t, ok)
		assert.Equal(t, want, got, "residue %d is routed to a shard, never to a source", residue)
	}
	assert.Equal(t, []SplitSource{{VChannel: "v0"}, {VChannel: "v1", Buckets: []uint64{0, 2}}}, split.Sources)

	v0, v1 := split.Sources[0], split.Sources[1]
	for residue := uint64(0); residue < 4; residue++ {
		assert.True(t, v0.MayHold(residue), "a source with no residues may hold any key")
	}
	assert.True(t, v1.MayHold(0))
	assert.False(t, v1.MayHold(1))
	assert.True(t, v1.MayHold(2))
	assert.False(t, v1.MayHold(3))
}

// Residues of a source that are not below the modulus only stop narrowing
// which sources are asked; they do not refuse the collection.
func TestNewSplitRoutingIgnoresTheResiduesOfASourceThatAreOutOfRange(t *testing.T) {
	split := NewSplitRouting(&milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v1", "v2"},
		RoutingModulus:      2,
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardNormal, "v1", 0),
			shardInfo(schemapb.ShardState_ShardNormal, "v2", 1),
			shardInfo(schemapb.ShardState_ShardSplitting, "v0", 0, 7),
		},
	})
	require.NoError(t, split.Err)
	assert.Equal(t, []SplitSource{{VChannel: "v0"}}, split.Sources)
}

// Malformed routing meta leaves no table and says why; the write path refuses
// the collection instead of routing it by position.
func TestNewSplitRoutingKeepsTheErrorOfMalformedMeta(t *testing.T) {
	split := NewSplitRouting(&milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1"},
		RoutingModulus:      4,
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardNormal, "v0", 0, 1),
			shardInfo(schemapb.ShardState_ShardNormal, "v1", 2),
		},
	})
	require.NotNil(t, split)
	assert.Nil(t, split.Table)
	assert.ErrorIs(t, split.Err, merr.ErrServiceInternal)
}

func TestNewCollectionInfoCarriesTheSplitRouting(t *testing.T) {
	schemaInfo, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c"})
	require.NoError(t, err)
	info := newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		Schema:              &schemapb.CollectionSchema{Name: "c"},
		VirtualChannelNames: []string{"v1", "v2"},
		RoutingModulus:      2,
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardNormal, "v1", 0),
			shardInfo(schemapb.ShardState_ShardNormal, "v2", 1),
		},
	}, schemaInfo, false, "")
	require.NotNil(t, info.SplitRouting)
	require.NotNil(t, info.SplitRouting.Table)
	assert.EqualValues(t, 2, info.RoutingModulus)

	legacy := newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		Schema:              &schemapb.CollectionSchema{Name: "c"},
		VirtualChannelNames: []string{"v0"},
	}, schemaInfo, false, "")
	assert.Nil(t, legacy.SplitRouting)
}

// RefreshCollectionByID refreshes its own collection only. Every
// attempt of every write a shard split's fence refused calls it, so it must not
// wait for the fills of every other collection in flight on the proxy, as
// RemoveCollectionsByID (an invalidation, which takes fillMu) does.
func TestRefreshCollectionByIDDoesNotWaitForAnotherCollectionsFill(t *testing.T) {
	MockPublishedSchemaForTest(t)
	ctx := context.Background()
	var describes101 atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	mix := mocks.NewMockMixCoordClient(t)
	mix.EXPECT().DescribeCollection(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, _ ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
			switch req.GetCollectionID() {
			case 101:
				describes101.Add(1)
			case 202:
				close(started)
				<-release
			}
			return &milvuspb.DescribeCollectionResponse{
				Status:              merr.Success(),
				CollectionID:        req.GetCollectionID(),
				DbName:              "db",
				VirtualChannelNames: []string{fmt.Sprintf("v%d", req.GetCollectionID())},
				Schema:              &schemapb.CollectionSchema{Name: fmt.Sprintf("c%d", req.GetCollectionID())},
			}, nil
		}).Maybe()
	mix.EXPECT().ShowPartitions(mock.Anything, mock.Anything, mock.Anything).Return(nil, errors.New("not used")).Maybe()

	cache, err := NewMetaCache(mix)
	require.NoError(t, err)
	defer cache.Close()
	_, err = cache.GetCollectionName(ctx, "", 101)
	require.NoError(t, err)
	require.EqualValues(t, 1, describes101.Load())

	// A describe of another collection is in flight. It must end before the
	// test does: its fill reads the published-schema mock this test's cleanup
	// removes.
	other := make(chan struct{})
	go func() {
		defer close(other)
		_, _ = cache.GetCollectionName(ctx, "", 202)
	}()
	defer func() { <-other }()
	<-started

	refreshed := make(chan struct{})
	go func() {
		defer close(refreshed)
		cache.RefreshCollectionByID(ctx, 101)
	}()
	select {
	case <-refreshed:
	case <-time.After(5 * time.Second):
		releaseOnce.Do(func() { close(release) })
		<-refreshed
		t.Fatal("the refresh waited for another collection's fill")
	}
	releaseOnce.Do(func() { close(release) })

	_, err = cache.GetCollectionName(ctx, "", 101)
	require.NoError(t, err)
	assert.EqualValues(t, 2, describes101.Load(), "the refresh evicted the collection")
}
