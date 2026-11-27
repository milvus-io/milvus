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
package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/routing"
	pb "github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

func newNeverSplitCollection() *Collection {
	return &Collection{
		CollectionID:         1,
		Name:                 "c",
		VirtualChannelNames:  []string{"by-dev-dml_0_1v0", "by-dev-dml_1_1v1"},
		PhysicalChannelNames: []string{"by-dev-dml_0", "by-dev-dml_1"},
		ShardsNum:            2,
		ShardInfos: map[string]*ShardInfo{
			"by-dev-dml_0_1v0": {VChannelName: "by-dev-dml_0_1v0", PChannelName: "by-dev-dml_0", LastTruncateTimeTick: 7},
			"by-dev-dml_1_1v1": {VChannelName: "by-dev-dml_1_1v1", PChannelName: "by-dev-dml_1", LastTruncateTimeTick: 9},
		},
	}
}

func TestNeverSplitCollectionRecordIsUnchanged(t *testing.T) {
	coll := newNeverSplitCollection()
	marshaled := MarshalCollectionModel(coll)
	// The record of a collection that was never split carries what it carried
	// before shard split existed: one entry per shard, with the truncate tick
	// and nothing else.
	require.Zero(t, marshaled.GetRoutingModulus())
	require.Empty(t, marshaled.GetShardBy())
	require.Empty(t, marshaled.GetSplitLineages())
	require.Len(t, marshaled.GetShardInfos(), 2)
	for i, tick := range []uint64{7, 9} {
		require.True(t, proto.Equal(&schemapb.CollectionShardInfo{LastTruncateTimeTick: tick}, marshaled.GetShardInfos()[i]))
	}

	// A record written by the old one-field message decodes into the new one.
	// The old message is gone, so its wire form is built by hand: field 1,
	// varint.
	legacy := &pb.CollectionInfo{
		ID:                   1,
		Schema:               &schemapb.CollectionSchema{Name: "c"},
		VirtualChannelNames:  coll.VirtualChannelNames,
		PhysicalChannelNames: coll.PhysicalChannelNames,
	}
	legacyBytes, err := proto.Marshal(legacy)
	require.NoError(t, err)
	for _, tick := range []byte{7, 9} {
		// field 17, length-delimited, length 2: {field 1 varint tick}
		legacyBytes = append(legacyBytes, 0x8a, 0x01, 0x02, 0x08, tick)
	}
	decoded := &pb.CollectionInfo{}
	require.NoError(t, proto.Unmarshal(legacyBytes, decoded))
	got := UnmarshalCollectionModel(decoded)
	require.Equal(t, uint64(7), got.ShardInfos["by-dev-dml_0_1v0"].LastTruncateTimeTick)
	require.Equal(t, uint64(9), got.ShardInfos["by-dev-dml_1_1v1"].LastTruncateTimeTick)
	require.Empty(t, got.ShardInfos["by-dev-dml_0_1v0"].Buckets)
	require.Zero(t, got.RoutingModulus)
	require.Nil(t, got.SplitLineages)

	// A record with no shard infos at all decodes too.
	legacy.ShardInfos = nil
	got = UnmarshalCollectionModel(legacy)
	require.Len(t, got.ShardInfos, 2)
	require.Equal(t, "by-dev-dml_1", got.ShardInfos["by-dev-dml_1_1v1"].PChannelName)

	// The routing of a never-split collection is the legacy one.
	table, err := coll.Routing().Table()
	require.NoError(t, err)
	require.Equal(t, uint64(2), table.Modulus())
}

func TestApplyShardSplit(t *testing.T) {
	coll := newNeverSplitCollection()
	source := "by-dev-dml_0_1v0"
	targets := [2]string{"by-dev-dml_2_1v2", "by-dev-dml_3_1v3"}
	split, err := routing.PlanSplit(coll.Routing(), source, targets)
	require.NoError(t, err)
	split.ShardBy = "hash(pk)"
	next, err := coll.Routing().ApplySplit(split)
	require.NoError(t, err)

	before := coll.Clone()
	lineage := &ShardSplitLineage{
		SourceVChannel:  source,
		SourcePChannel:  "by-dev-dml_0",
		TargetVChannels: targets[:],
		SwitchTimeTick:  100,
		SplitTaskID:     55,
	}
	coll.ApplyShardSplit(next, lineage)

	// The source left every list a DML or a DDL broadcast is taken from.
	require.Equal(t, []string{"by-dev-dml_2_1v2", "by-dev-dml_3_1v3", "by-dev-dml_1_1v1"}, coll.VirtualChannelNames)
	require.Equal(t, []string{"by-dev-dml_2", "by-dev-dml_3", "by-dev-dml_1"}, coll.PhysicalChannelNames)
	require.NotContains(t, coll.ShardInfos, source)
	require.Equal(t, int32(3), coll.ShardsNum)
	require.Equal(t, uint64(4), coll.RoutingModulus)
	require.Equal(t, "hash(pk)", coll.ShardBy)
	// A shard that stays keeps its truncate tick; a new shard starts at zero.
	require.Equal(t, uint64(9), coll.ShardInfos["by-dev-dml_1_1v1"].LastTruncateTimeTick)
	require.Equal(t, []uint64{1, 3}, coll.ShardInfos["by-dev-dml_1_1v1"].Buckets)
	require.Zero(t, coll.ShardInfos["by-dev-dml_2_1v2"].LastTruncateTimeTick)
	require.Equal(t, []uint64{0}, coll.ShardInfos["by-dev-dml_2_1v2"].Buckets)
	require.Equal(t, []uint64{2}, coll.ShardInfos["by-dev-dml_3_1v3"].Buckets)
	// The lineage is recorded, as a copy.
	require.Equal(t, lineage, coll.SplitLineageOf(source))
	require.NotSame(t, lineage, coll.SplitLineageOf(source))
	require.Nil(t, coll.SplitLineageOf("by-dev-dml_1_1v1"))
	require.Equal(t, []string{source}, coll.SplitSourceVChannels())
	require.True(t, next.Equal(coll.Routing()))

	// The clone taken before is untouched.
	require.Equal(t, []string{"by-dev-dml_0_1v0", "by-dev-dml_1_1v1"}, before.VirtualChannelNames)
	require.Nil(t, before.SplitLineages)

	// Everything survives the catalog round trip.
	raw, err := proto.Marshal(MarshalCollectionModel(coll))
	require.NoError(t, err)
	decoded := &pb.CollectionInfo{}
	require.NoError(t, proto.Unmarshal(raw, decoded))
	require.Equal(t, "by-dev-dml_2_1v2", decoded.GetShardInfos()[0].GetVchannelName())
	got := UnmarshalCollectionModel(decoded)
	require.Equal(t, coll.VirtualChannelNames, got.VirtualChannelNames)
	require.Equal(t, coll.PhysicalChannelNames, got.PhysicalChannelNames)
	require.Equal(t, coll.RoutingModulus, got.RoutingModulus)
	require.Equal(t, coll.ShardBy, got.ShardBy)
	require.Equal(t, coll.SplitLineages, got.SplitLineages)
	require.True(t, coll.Routing().Equal(got.Routing()))
	require.Equal(t, uint64(9), got.ShardInfos["by-dev-dml_1_1v1"].LastTruncateTimeTick)

	// Clone is deep; ShallowClone shares.
	cloned := coll.Clone()
	cloned.SplitLineages[0].TargetVChannels[0] = "changed"
	cloned.ShardInfos["by-dev-dml_1_1v1"].Buckets[0] = 99
	require.Equal(t, targets[0], coll.SplitLineages[0].TargetVChannels[0])
	require.Equal(t, uint64(1), coll.ShardInfos["by-dev-dml_1_1v1"].Buckets[0])
	shallow := coll.ShallowClone()
	require.Equal(t, coll.RoutingModulus, shallow.RoutingModulus)
	require.Equal(t, coll.ShardBy, shallow.ShardBy)
	require.Equal(t, coll.SplitLineages, shallow.SplitLineages)

	// A second split appends a second lineage and leaves the first one as it is.
	split, err = routing.PlanSplit(coll.Routing(), "by-dev-dml_1_1v1", [2]string{"by-dev-dml_4_1v4", "by-dev-dml_5_1v5"})
	require.NoError(t, err)
	next, err = coll.Routing().ApplySplit(split)
	require.NoError(t, err)
	coll.ApplyShardSplit(next, &ShardSplitLineage{SourceVChannel: "by-dev-dml_1_1v1", TargetVChannels: []string{"by-dev-dml_4_1v4", "by-dev-dml_5_1v5"}, SwitchTimeTick: 200, SplitTaskID: 56})
	require.Len(t, coll.SplitLineages, 2)
	require.Equal(t, lineage, coll.SplitLineages[0])
	require.Equal(t, uint64(4), coll.RoutingModulus)
	require.Equal(t, int32(4), coll.ShardsNum)
}

func TestSplitSourceBuckets(t *testing.T) {
	c := &Collection{
		RoutingModulus: 4,
		ShardInfos: map[string]*ShardInfo{
			"t1": {VChannelName: "t1", Buckets: []uint64{2}},
			"t2": {VChannelName: "t2", Buckets: []uint64{0}},
			"b":  {VChannelName: "b", Buckets: []uint64{1, 3}},
		},
		SplitLineages: []*ShardSplitLineage{{SourceVChannel: "s", TargetVChannels: []string{"t1", "t2"}}},
	}
	// The union of the targets, sorted.
	require.Equal(t, []uint64{0, 2}, c.SplitSourceBuckets("s"))
	// A shard that takes DML, or an unknown vchannel, is not a split source.
	require.Nil(t, c.SplitSourceBuckets("b"))
	require.Nil(t, c.SplitSourceBuckets("x"))

	// A target that owns no residue, a lineage without targets, and a cycle
	// are malformed: no residues.
	c.ShardInfos["t1"].Buckets = nil
	require.Nil(t, c.SplitSourceBuckets("s"))
	c.ShardInfos["t1"].Buckets = []uint64{2}
	c.SplitLineages[0].TargetVChannels = nil
	require.Nil(t, c.SplitSourceBuckets("s"))
	c.SplitLineages = []*ShardSplitLineage{
		{SourceVChannel: "s", TargetVChannels: []string{"u"}},
		{SourceVChannel: "u", TargetVChannels: []string{"s"}},
	}
	require.Nil(t, c.SplitSourceBuckets("s"))

	// A collection that was never split has no residues at all.
	c.RoutingModulus = 0
	c.SplitLineages = []*ShardSplitLineage{{SourceVChannel: "s", TargetVChannels: []string{"t1", "t2"}}}
	require.Nil(t, c.SplitSourceBuckets("s"))
}
