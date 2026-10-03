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

// During a split window the source is listed as Splitting with no residues and
// the targets as Creating with theirs.
func TestNewSplitRoutingDuringASplitWindow(t *testing.T) {
	split := NewSplitRouting(&milvuspb.DescribeCollectionResponse{
		VirtualChannelNames: []string{"v0", "v1", "v2", "v3"},
		RoutingModulus:      4,
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardSplitting, "v0"),
			shardInfo(schemapb.ShardState_ShardNormal, "v1", 1, 3),
			shardInfo(schemapb.ShardState_ShardCreating, "v2", 0),
			shardInfo(schemapb.ShardState_ShardCreating, "v3", 2),
		},
	})
	require.NotNil(t, split)
	require.NoError(t, split.Err)
	require.NotNil(t, split.Table)
	assert.Equal(t, []string{"v0"}, split.Fenced)
	assert.EqualValues(t, 4, split.Table.Modulus())
	for residue, want := range []string{"v2", "v1", "v3", "v1"} {
		got, ok := split.Table.Lookup(uint64(residue))
		require.True(t, ok)
		assert.Equal(t, want, got, "residue %d", residue)
	}
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
		VirtualChannelNames: []string{"v0", "v1", "v2"},
		RoutingModulus:      2,
		ShardInfos: []*schemapb.CollectionShardInfo{
			shardInfo(schemapb.ShardState_ShardSplitting, "v0"),
			shardInfo(schemapb.ShardState_ShardCreating, "v1", 0),
			shardInfo(schemapb.ShardState_ShardCreating, "v2", 1),
		},
	}, schemaInfo, false, "", false, false)
	require.NotNil(t, info.SplitRouting)
	assert.Equal(t, []string{"v0"}, info.SplitRouting.Fenced)
	assert.EqualValues(t, 2, info.RoutingModulus)

	legacy := newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		Schema:              &schemapb.CollectionSchema{Name: "c"},
		VirtualChannelNames: []string{"v0"},
	}, schemaInfo, false, "", false, false)
	assert.Nil(t, legacy.SplitRouting)
}

// AV-L2-M2: RefreshCollectionByID refreshes its own collection only. Every
// attempt of every write a shard split's fence refused calls it, so it must not
// wait for the fills of every other collection in flight on the proxy, as
// RemoveCollectionsByID (an invalidation, which takes fillMu) does.
func TestRefreshCollectionByIDDoesNotWaitForAnotherCollectionsFill(t *testing.T) {
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

	// A describe of another collection is in flight.
	go func() { _, _ = cache.GetCollectionName(ctx, "", 202) }()
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
