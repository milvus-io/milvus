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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type servingLineageCache struct {
	Cache
	info *collectionInfo
}

func (c servingLineageCache) GetCollectionInfo(context.Context, string, string, int64) (*collectionInfo, error) {
	return c.info, nil
}

func TestHasShardSplitRequiresLocalLineage(t *testing.T) {
	r := routing.Routing{Modulus: 4, ShardBy: "hash(pk)", Shards: []routing.Shard{{VChannel: "a", Buckets: []uint64{0}}, {VChannel: "b", Buckets: []uint64{1, 3}}, {VChannel: "c", Buckets: []uint64{2}}}}
	description := &milvuspb.DescribeCollectionResponse{RoutingModulus: 4, ShardBy: r.ShardBy, VirtualChannelNames: r.VChannels(), ShardInfos: r.ShardInfos()}
	cache := servingLineageCache{info: &collectionInfo{RoutingModulus: 4, SplitRouting: metacache.NewSplitRouting(description)}}
	node := &Proxy{metaCache: cache}
	required, err := node.HasShardSplit(t.Context(), 1)
	require.NoError(t, err)
	require.False(t, required, "restored static owners use their initial physical membership")
	description.ShardInfos = append(description.ShardInfos, &schemapb.CollectionShardInfo{VchannelName: "source", State: schemapb.ShardState_ShardSplitting})
	cache.info.SplitRouting = metacache.NewSplitRouting(description)
	required, err = node.HasShardSplit(t.Context(), 1)
	require.NoError(t, err)
	require.True(t, required, "actual local split history keeps strict publication")
	cache.info.SplitRouting.Err = merr.ErrDataIntegrity
	_, err = node.HasShardSplit(t.Context(), 1)
	require.ErrorIs(t, err, merr.ErrDataIntegrity, "malformed routing must not bypass the fence")
	cache.info.SplitRouting = nil
	_, err = node.HasShardSplit(t.Context(), 1)
	require.ErrorIs(t, err, merr.ErrServiceInternal, "missing derived routing must not bypass the fence")
	cache.info.RoutingModulus = 0
	required, err = node.HasShardSplit(t.Context(), 1)
	require.NoError(t, err)
	require.False(t, required)
}

type servingSnapshotCache struct {
	Cache
	infos []*collectionInfo
	err   error
	calls int
}

func (c *servingSnapshotCache) GetCollectionInfo(context.Context, string, string, int64) (*collectionInfo, error) {
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	index := min(c.calls-1, len(c.infos)-1)
	return c.infos[index], nil
}

func TestCollectionVChannelsReturnsOneRoutingSnapshot(t *testing.T) {
	r := routing.Routing{Modulus: 2, ShardBy: "hash(pk)", Shards: []routing.Shard{{VChannel: "t1", Buckets: []uint64{0}}, {VChannel: "t2", Buckets: []uint64{1}}}}
	description := &milvuspb.DescribeCollectionResponse{RoutingModulus: 2, ShardBy: r.ShardBy, VirtualChannelNames: r.VChannels(), ShardInfos: r.ShardInfos()}
	description.ShardInfos = append(description.ShardInfos, &schemapb.CollectionShardInfo{VchannelName: "s", State: schemapb.ShardState_ShardSplitting})
	info := &collectionInfo{VChannels: []string{"t1", "t2"}, RoutingModulus: 2, SplitRouting: metacache.NewSplitRouting(description)}
	cache := &servingSnapshotCache{infos: []*collectionInfo{info, {VChannels: []string{"s"}}}}
	node := &Proxy{metaCache: cache}
	owners, split, err := node.GetCollectionVChannels(t.Context(), 1)
	require.NoError(t, err)
	require.True(t, split)
	require.Equal(t, []string{"t1", "t2"}, owners)
	require.Equal(t, 1, cache.calls, "one immutable metadata read must supply both values")
	owners[0] = "mutated"
	require.Equal(t, []string{"t1", "t2"}, info.VChannels)
	cache.err = merr.ErrCollectionNotFound
	_, _, err = node.GetCollectionVChannels(t.Context(), 1)
	require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	node = &Proxy{}
	_, _, err = node.GetCollectionVChannels(t.Context(), 1)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
