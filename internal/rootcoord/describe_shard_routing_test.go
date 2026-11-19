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
package rootcoord

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/routing"
)

func TestConvertModelToDescShardRouting(t *testing.T) {
	// A collection that was never split reports no routing, as before.
	neverSplit := &model.Collection{
		CollectionID:         1,
		Name:                 "c",
		VirtualChannelNames:  []string{"p0_1v0", "p1_1v1"},
		PhysicalChannelNames: []string{"p0", "p1"},
		ShardsNum:            2,
		ShardInfos: map[string]*model.ShardInfo{
			"p0_1v0": {VChannelName: "p0_1v0", PChannelName: "p0", LastTruncateTimeTick: 5},
			"p1_1v1": {VChannelName: "p1_1v1", PChannelName: "p1"},
		},
	}
	desc := convertModelToDesc(neverSplit, nil, "default")
	require.Empty(t, desc.GetShardInfos())
	require.Zero(t, desc.GetRoutingModulus())
	require.Empty(t, desc.GetShardBy())
	require.Equal(t, int32(2), desc.GetShardsNum())

	// A collection that was split reports the residues of every shard that
	// accepts DML, parallel to its vchannel list, and then every split source
	// in state ShardSplitting with the residues it owned.
	split := &model.Collection{
		CollectionID:         1,
		Name:                 "c",
		VirtualChannelNames:  []string{"p2_1v2", "p3_1v3", "p1_1v1"},
		PhysicalChannelNames: []string{"p2", "p3", "p1"},
		ShardsNum:            3,
		RoutingModulus:       4,
		ShardBy:              "hash(pk)",
		ShardInfos: map[string]*model.ShardInfo{
			"p2_1v2": {VChannelName: "p2_1v2", PChannelName: "p2", Buckets: []uint64{0}},
			"p3_1v3": {VChannelName: "p3_1v3", PChannelName: "p3", Buckets: []uint64{2}},
			"p1_1v1": {VChannelName: "p1_1v1", PChannelName: "p1", Buckets: []uint64{1, 3}, LastTruncateTimeTick: 9},
		},
		SplitLineages: []*model.ShardSplitLineage{{SourceVChannel: "p0_1v0", TargetVChannels: []string{"p2_1v2", "p3_1v3"}, SwitchTimeTick: 100}},
	}
	desc = convertModelToDesc(split, nil, "default")
	require.Equal(t, []string{"p2_1v2", "p3_1v3", "p1_1v1"}, desc.GetVirtualChannelNames())
	require.Equal(t, uint64(4), desc.GetRoutingModulus())
	require.Equal(t, "hash(pk)", desc.GetShardBy())
	require.Equal(t, int32(3), desc.GetShardsNum())
	require.Len(t, desc.GetShardInfos(), 4)
	for i, vchannel := range desc.GetVirtualChannelNames() {
		info := desc.GetShardInfos()[i]
		require.Equal(t, vchannel, info.GetVchannelName())
		require.Equal(t, schemapb.ShardState_ShardNormal, info.GetState())
		require.Equal(t, split.ShardInfos[vchannel].Buckets, info.GetHashRouting().GetBuckets())
		require.Equal(t, split.ShardInfos[vchannel].LastTruncateTimeTick, info.GetLastTruncateTimeTick())
	}
	source := desc.GetShardInfos()[3]
	require.Equal(t, "p0_1v0", source.GetVchannelName())
	require.Equal(t, schemapb.ShardState_ShardSplitting, source.GetState())
	require.Equal(t, []uint64{0, 2}, source.GetHashRouting().GetBuckets())
	require.Zero(t, source.GetLastTruncateTimeTick())

	// The raw list routes exactly as the shards that take DML do: no residue
	// reaches the source.
	r, err := routing.RoutingFromShardInfos(desc.GetRoutingModulus(), desc.GetShardBy(), desc.GetShardInfos())
	require.NoError(t, err)
	require.Equal(t, desc.GetVirtualChannelNames(), r.VChannels())
}

// A chain of splits lists every source, for ever, each with the residues it
// owned at the current modulus. A source whose descendants cannot be resolved
// is listed without residues.
func TestConvertModelToDescListsEverySplitSource(t *testing.T) {
	// v0 owned {0} at modulus 2 and was split into v2 {0} and v3 {2} at 4; then
	// v3 was split into v4 {2} and v5 {6} at 8.
	chain := &model.Collection{
		CollectionID:         1,
		Name:                 "c",
		VirtualChannelNames:  []string{"p2_1v2", "p4_1v4", "p5_1v5", "p1_1v1"},
		PhysicalChannelNames: []string{"p2", "p4", "p5", "p1"},
		ShardsNum:            4,
		RoutingModulus:       8,
		ShardBy:              "hash(pk)",
		ShardInfos: map[string]*model.ShardInfo{
			"p2_1v2": {VChannelName: "p2_1v2", PChannelName: "p2", Buckets: []uint64{0, 4}},
			"p4_1v4": {VChannelName: "p4_1v4", PChannelName: "p4", Buckets: []uint64{2}},
			"p5_1v5": {VChannelName: "p5_1v5", PChannelName: "p5", Buckets: []uint64{6}},
			"p1_1v1": {VChannelName: "p1_1v1", PChannelName: "p1", Buckets: []uint64{1, 3, 5, 7}},
		},
		SplitLineages: []*model.ShardSplitLineage{
			{SourceVChannel: "p0_1v0", TargetVChannels: []string{"p2_1v2", "p3_1v3"}, SwitchTimeTick: 100},
			{SourceVChannel: "p3_1v3", TargetVChannels: []string{"p4_1v4", "p5_1v5"}, SwitchTimeTick: 200},
		},
	}
	desc := convertModelToDesc(chain, nil, "default")
	require.Len(t, desc.GetShardInfos(), 6)
	sources := desc.GetShardInfos()[4:]
	require.Equal(t, "p0_1v0", sources[0].GetVchannelName())
	require.Equal(t, schemapb.ShardState_ShardSplitting, sources[0].GetState())
	require.Equal(t, []uint64{0, 2, 4, 6}, sources[0].GetHashRouting().GetBuckets())
	require.Equal(t, "p3_1v3", sources[1].GetVchannelName())
	require.Equal(t, schemapb.ShardState_ShardSplitting, sources[1].GetState())
	require.Equal(t, []uint64{2, 6}, sources[1].GetHashRouting().GetBuckets())

	r, err := routing.RoutingFromShardInfos(desc.GetRoutingModulus(), desc.GetShardBy(), desc.GetShardInfos())
	require.NoError(t, err)
	require.Equal(t, desc.GetVirtualChannelNames(), r.VChannels())
	_, err = r.Table()
	require.NoError(t, err)

	// A target that is neither a shard nor a split source: no residues.
	chain.SplitLineages[1].TargetVChannels = []string{"p4_1v4", "unknown"}
	desc = convertModelToDesc(chain, nil, "default")
	require.Len(t, desc.GetShardInfos(), 6)
	for _, source := range desc.GetShardInfos()[4:] {
		require.Equal(t, schemapb.ShardState_ShardSplitting, source.GetState())
		require.Nil(t, source.GetRouting())
	}
}
