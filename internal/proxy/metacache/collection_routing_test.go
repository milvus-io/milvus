// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package metacache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestNewCollectionInfoCarriesShardRouting(t *testing.T) {
	schema, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c"})
	require.NoError(t, err)
	shardInfos := []*schemapb.CollectionShardInfo{
		{VchannelName: "p1_1v1", Routing: &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}}}},
		{VchannelName: "p2_1v2", Routing: &schemapb.CollectionShardInfo_HashRouting{HashRouting: &schemapb.HashRouting{Buckets: []uint64{1}}}},
	}
	info := newCollectionInfo(&milvuspb.DescribeCollectionResponse{
		CollectionID:        1,
		VirtualChannelNames: []string{"p1_1v1", "p2_1v2"},
		ShardInfos:          shardInfos,
		RoutingModulus:      2,
		ShardBy:             "hash(pk)",
	}, schema, false, "")
	require.Equal(t, shardInfos, info.ShardInfos)
	require.Equal(t, uint64(2), info.RoutingModulus)
	require.Equal(t, "hash(pk)", info.ShardBy)

	// A collection that was never split carries no routing.
	info = newCollectionInfo(&milvuspb.DescribeCollectionResponse{CollectionID: 1, VirtualChannelNames: []string{"p0_1v0"}}, schema, false, "")
	require.Empty(t, info.ShardInfos)
	require.Zero(t, info.RoutingModulus)
	require.Empty(t, info.ShardBy)
}
