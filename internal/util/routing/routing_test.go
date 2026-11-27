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
package routing

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func neverSplit(vchannels ...string) Routing {
	r := Routing{}
	for _, vchannel := range vchannels {
		r.Shards = append(r.Shards, Shard{VChannel: vchannel})
	}
	return r
}

func requireSystemError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	// A routing refusal is never the fault of a request.
	require.Equal(t, merr.SystemError, merr.GetErrorType(err), err.Error())
}

func TestTableOfNeverSplitCollectionIsLegacyPlacement(t *testing.T) {
	vchannels := []string{"p0_1v0", "p1_1v1", "p2_1v2"}
	table, err := neverSplit(vchannels...).Table()
	require.NoError(t, err)
	require.Equal(t, uint64(3), table.Modulus())

	intPKs := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}
	strPKs := &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}}
	for i := int64(-500); i < 500; i++ {
		intPKs.GetIntId().Data = append(intPKs.GetIntId().Data, i*7919)
		strPKs.GetStrId().Data = append(strPKs.GetStrId().Data, fmt.Sprintf("key-%d", i))
	}
	for _, pks := range []*schemapb.IDs{intPKs, strPKs} {
		legacy, err := typeutil.HashPK2Channels(pks, vchannels)
		require.NoError(t, err)
		placed, err := table.VChannelsOfPKs(pks)
		require.NoError(t, err)
		require.Len(t, placed, len(legacy))
		for i := range legacy {
			require.Equal(t, vchannels[legacy[i]], placed[i])
		}
	}

	// A batch with no id field places nothing, as HashPK2Channels does.
	placed, err := table.VChannelsOfPKs(&schemapb.IDs{})
	require.NoError(t, err)
	require.Empty(t, placed)
}

func TestTableRefusesMalformedRouting(t *testing.T) {
	for name, r := range map[string]Routing{
		"no shard":                    {},
		"unnamed shard":               {Shards: []Shard{{VChannel: ""}}},
		"shard listed twice":          neverSplit("a", "a"),
		"residues without a modulus":  {Shards: []Shard{{VChannel: "a", Buckets: []uint64{0}}}},
		"modulus without residues":    {Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0, 1}}, {VChannel: "b"}}},
		"modulus above the bound":     {Modulus: MaxModulus + 1, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0}}}},
		"residue not below modulus":   {Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0, 2}}}},
		"residue owned twice":         {Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0, 1}}, {VChannel: "b", Buckets: []uint64{1}}}},
		"residue listed twice":        {Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0, 0}}}},
		"residue owned by no shard":   {Modulus: 4, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0, 1, 2}}}},
		"explicit of a broken layout": {Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0}}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := r.Table()
			requireSystemError(t, err)
			_, err = r.Explicit()
			requireSystemError(t, err)
		})
	}
}

func TestTableLookup(t *testing.T) {
	r := Routing{Modulus: 4, Shards: []Shard{
		{VChannel: "a", Buckets: []uint64{2, 0}},
		{VChannel: "b", Buckets: []uint64{1}},
		{VChannel: "c", Buckets: []uint64{3}},
	}}
	table, err := r.Table()
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 2}, table.BucketsOf("a"))
	require.Empty(t, table.BucketsOf("unknown"))
	owner, ok := table.Lookup(3)
	require.True(t, ok)
	require.Equal(t, "c", owner)
	_, ok = table.Lookup(4)
	require.False(t, ok)
	require.Equal(t, []string{"a", "b", "c"}, r.VChannels())

	cloned := r.Clone()
	cloned.Shards[0].Buckets[0] = 99
	require.Equal(t, uint64(2), r.Shards[0].Buckets[0])
}

func TestResidues(t *testing.T) {
	_, err := PKResidueInt64(1, 0)
	requireSystemError(t, err)
	_, err = PKResidueVarChar("a", MaxModulus+1)
	requireSystemError(t, err)
	_, err = PKResidues(&schemapb.IDs{}, 0)
	requireSystemError(t, err)

	hash, err := typeutil.Hash32Int64(42)
	require.NoError(t, err)
	residue, err := PKResidueInt64(42, 8)
	require.NoError(t, err)
	require.Equal(t, uint64(hash)%8, residue)
	residue, err = PKResidueVarChar("k", 8)
	require.NoError(t, err)
	require.Equal(t, uint64(typeutil.HashString2Uint32("k"))%8, residue)

	// Doubling the modulus keeps a key on its residue or moves it up by the
	// old modulus. A split relies on this: a key never changes its residue
	// class modulo the old modulus.
	for pk := int64(0); pk < 1000; pk++ {
		low, err := PKResidueInt64(pk, 4)
		require.NoError(t, err)
		high, err := PKResidueInt64(pk, 8)
		require.NoError(t, err)
		require.Equal(t, low, high%4)
	}
}

func TestShardInfosRoundTrip(t *testing.T) {
	r := Routing{Modulus: 2, ShardBy: "hash(pk)", Shards: []Shard{
		{VChannel: "a", Buckets: []uint64{0}},
		{VChannel: "b", Buckets: []uint64{1}},
	}}
	infos := r.ShardInfos()
	require.Len(t, infos, 2)
	require.Equal(t, "a", infos[0].GetVchannelName())
	require.Equal(t, schemapb.ShardState_ShardNormal, infos[0].GetState())
	require.Equal(t, []uint64{0}, infos[0].GetHashRouting().GetBuckets())

	back, err := RoutingFromShardInfos(2, "hash(pk)", infos)
	require.NoError(t, err)
	require.True(t, r.Equal(back))

	// A never-split shard carries no routing predicate.
	require.Nil(t, neverSplit("a").ShardInfos()[0].GetRouting())

	_, err = RoutingFromShardInfos(2, "", []*schemapb.CollectionShardInfo{{}})
	requireSystemError(t, err)
	_, err = RoutingFromShardInfos(2, "", []*schemapb.CollectionShardInfo{{VchannelName: "a", State: schemapb.ShardState_ShardCreating}})
	requireSystemError(t, err)
	_, err = RoutingFromShardInfos(2, "", []*schemapb.CollectionShardInfo{{VchannelName: "a", State: schemapb.ShardState_ShardDropped}})
	requireSystemError(t, err)
}

// DescribeCollection lists every split source after the shards that take DML,
// in state ShardSplitting, with the residues it owned. The routing skips them,
// so the raw list of a response gives the same routing and table, and no
// residue routes to a source.
func TestRoutingFromShardInfosSkipsSplitSources(t *testing.T) {
	live := Routing{Modulus: 4, ShardBy: "hash(pk)", Shards: []Shard{
		{VChannel: "t1", Buckets: []uint64{0}},
		{VChannel: "t2", Buckets: []uint64{2}},
		{VChannel: "b", Buckets: []uint64{1, 3}},
	}}
	infos := append(live.ShardInfos(),
		&schemapb.CollectionShardInfo{
			State:        schemapb.ShardState_ShardSplitting,
			VchannelName: "s",
			Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{0, 2}},
			},
		},
		&schemapb.CollectionShardInfo{State: schemapb.ShardState_ShardSplitting, VchannelName: "older-source"},
	)
	got, err := RoutingFromShardInfos(4, "hash(pk)", infos)
	require.NoError(t, err)
	require.True(t, live.Equal(got))
	require.Equal(t, []string{"t1", "t2", "b"}, got.VChannels())

	table, err := got.Table()
	require.NoError(t, err)
	for residue := uint64(0); residue < table.Modulus(); residue++ {
		owner, ok := table.Lookup(residue)
		require.True(t, ok)
		require.NotEqual(t, "s", owner)
		require.NotEqual(t, "older-source", owner)
	}

	// A source still needs a name: a nameless entry is malformed whatever its state.
	_, err = RoutingFromShardInfos(4, "", append(live.ShardInfos(), &schemapb.CollectionShardInfo{State: schemapb.ShardState_ShardSplitting}))
	requireSystemError(t, err)
}

func TestPlanSplitDoublesTheModulusOfASingleResidue(t *testing.T) {
	current := neverSplit("p0_1v0", "p1_1v1")
	split, err := PlanSplit(current, "p0_1v0", [2]string{"p2_1v2", "p3_1v3"})
	require.NoError(t, err)
	require.Equal(t, uint64(4), split.Modulus)
	require.Equal(t, []Shard{
		{VChannel: "p2_1v2", Buckets: []uint64{0}},
		{VChannel: "p3_1v3", Buckets: []uint64{2}},
	}, split.Targets)

	next, err := current.ApplySplit(split)
	require.NoError(t, err)
	// The targets take the place of the source; the untouched shard keeps
	// every key it had, expressed at the new modulus.
	require.True(t, next.Equal(Routing{Modulus: 4, Shards: []Shard{
		{VChannel: "p2_1v2", Buckets: []uint64{0}},
		{VChannel: "p3_1v3", Buckets: []uint64{2}},
		{VChannel: "p1_1v1", Buckets: []uint64{1, 3}},
	}}))
	require.NoError(t, CheckSplitPostImage(current, next, "p0_1v0", []string{"p2_1v2", "p3_1v3"}))

	// No key of the untouched shard moves, and every key of the source lands
	// on one of the two targets.
	before, err := current.Table()
	require.NoError(t, err)
	after, err := next.Table()
	require.NoError(t, err)
	pks := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}
	for i := int64(0); i < 2000; i++ {
		pks.GetIntId().Data = append(pks.GetIntId().Data, i)
	}
	was, err := before.VChannelsOfPKs(pks)
	require.NoError(t, err)
	is, err := after.VChannelsOfPKs(pks)
	require.NoError(t, err)
	for i := range was {
		if was[i] == "p0_1v0" {
			require.Contains(t, []string{"p2_1v2", "p3_1v3"}, is[i])
		} else {
			require.Equal(t, was[i], is[i])
		}
	}
}

func TestPlanSplitKeepsTheModulusOfSeveralResidues(t *testing.T) {
	current := Routing{Modulus: 4, ShardBy: "hash(pk)", Shards: []Shard{
		{VChannel: "a", Buckets: []uint64{3, 1}},
		{VChannel: "b", Buckets: []uint64{0, 2}},
	}}
	split, err := PlanSplit(current, "a", [2]string{"c", "d"})
	require.NoError(t, err)
	require.Equal(t, uint64(4), split.Modulus)
	require.Equal(t, []uint64{1}, split.Targets[0].Buckets)
	require.Equal(t, []uint64{3}, split.Targets[1].Buckets)
	next, err := current.ApplySplit(split)
	require.NoError(t, err)
	require.Equal(t, "hash(pk)", next.ShardBy)
	require.Equal(t, []string{"c", "d", "b"}, next.VChannels())

	// A second split of a target, which now owns one residue, doubles again.
	split, err = PlanSplit(next, "c", [2]string{"e", "f"})
	require.NoError(t, err)
	require.Equal(t, uint64(8), split.Modulus)
	require.Equal(t, []uint64{1}, split.Targets[0].Buckets)
	require.Equal(t, []uint64{5}, split.Targets[1].Buckets)

	_, err = PlanSplit(current, "unknown", [2]string{"c", "d"})
	requireSystemError(t, err)
	_, err = PlanSplit(Routing{}, "a", [2]string{"c", "d"})
	requireSystemError(t, err)
	// The plan is validated through ApplySplit.
	_, err = PlanSplit(current, "a", [2]string{"b", "d"})
	requireSystemError(t, err)
}

func TestApplySplitRefusals(t *testing.T) {
	current := Routing{Modulus: 2, ShardBy: "hash(pk)", Shards: []Shard{
		{VChannel: "a", Buckets: []uint64{0}},
		{VChannel: "b", Buckets: []uint64{1}},
	}}
	valid := func() Split {
		return Split{Source: "a", Modulus: 4, Targets: []Shard{
			{VChannel: "c", Buckets: []uint64{0}},
			{VChannel: "d", Buckets: []uint64{2}},
		}}
	}
	_, err := current.ApplySplit(valid())
	require.NoError(t, err)

	for name, mutate := range map[string]func(*Split){
		"zero modulus":                 func(s *Split) { s.Modulus = 0 },
		"modulus not a multiple":       func(s *Split) { s.Modulus = 3 },
		"modulus above the bound":      func(s *Split) { s.Modulus = MaxModulus * 2 },
		"routing key changed":          func(s *Split) { s.ShardBy = "hash(other)" },
		"unknown source":               func(s *Split) { s.Source = "z" },
		"one target":                   func(s *Split) { s.Targets = s.Targets[:1] },
		"three targets":                func(s *Split) { s.Targets = append(s.Targets, Shard{VChannel: "e", Buckets: []uint64{2}}) },
		"unnamed target":               func(s *Split) { s.Targets[0].VChannel = "" },
		"same target twice":            func(s *Split) { s.Targets[1].VChannel = "c" },
		"target is an existing shard":  func(s *Split) { s.Targets[1].VChannel = "b" },
		"target owns nothing":          func(s *Split) { s.Targets[1].Buckets = nil },
		"target takes a foreign key":   func(s *Split) { s.Targets[1].Buckets = []uint64{2, 1} },
		"targets share a residue":      func(s *Split) { s.Targets[1].Buckets = []uint64{0} },
		"a residue of the source lost": func(s *Split) { s.Modulus = 8; s.Targets[1].Buckets = []uint64{2, 4} },
	} {
		t.Run(name, func(t *testing.T) {
			split := valid()
			mutate(&split)
			_, err := current.ApplySplit(split)
			requireSystemError(t, err)
		})
	}

	// A first split may declare the routing key; the same key is accepted.
	undeclared := current.Clone()
	undeclared.ShardBy = ""
	split := valid()
	split.ShardBy = "hash(pk)"
	next, err := undeclared.ApplySplit(split)
	require.NoError(t, err)
	require.Equal(t, "hash(pk)", next.ShardBy)
	_, err = current.ApplySplit(split)
	require.NoError(t, err)

	// A malformed current routing is refused before anything else.
	_, err = Routing{Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0}}}}.ApplySplit(valid())
	requireSystemError(t, err)
}

func TestCheckSplitPostImage(t *testing.T) {
	current := neverSplit("a", "b")
	next := Routing{Modulus: 4, Shards: []Shard{
		{VChannel: "c", Buckets: []uint64{0}},
		{VChannel: "d", Buckets: []uint64{2}},
		{VChannel: "b", Buckets: []uint64{3, 1}},
	}}
	require.NoError(t, CheckSplitPostImage(current, next, "a", []string{"c", "d"}))

	// A target the routing does not list.
	requireSystemError(t, CheckSplitPostImage(current, next, "a", []string{"c", "z"}))
	// The split itself is invalid.
	requireSystemError(t, CheckSplitPostImage(current, next, "b", []string{"c", "d"}))
	// The routing moves a key of an untouched shard: b lost residue 3 to c.
	moved := next.Clone()
	moved.Shards[0].Buckets = []uint64{0, 3}
	moved.Shards[2].Buckets = []uint64{1}
	requireSystemError(t, CheckSplitPostImage(current, moved, "a", []string{"c", "d"}))
	// The routing reorders the shards.
	reordered := next.Clone()
	reordered.Shards[1], reordered.Shards[2] = reordered.Shards[2], reordered.Shards[1]
	requireSystemError(t, CheckSplitPostImage(current, reordered, "a", []string{"c", "d"}))
}

func TestRoutingEqual(t *testing.T) {
	a := Routing{Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0, 1}}}}
	require.True(t, a.Equal(Routing{Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{1, 0}}}}))
	require.False(t, a.Equal(Routing{Modulus: 4, Shards: a.Shards}))
	require.False(t, a.Equal(Routing{Modulus: 2, ShardBy: "x", Shards: a.Shards}))
	require.False(t, a.Equal(Routing{Modulus: 2}))
	require.False(t, a.Equal(Routing{Modulus: 2, Shards: []Shard{{VChannel: "b", Buckets: []uint64{0, 1}}}}))
	require.False(t, a.Equal(Routing{Modulus: 2, Shards: []Shard{{VChannel: "a", Buckets: []uint64{0}}}}))
}

func TestCheckSplitChannels(t *testing.T) {
	shards := []string{"by-dev-dml_0_1v0", "by-dev-dml_1_1v1"}
	sources := []string{"by-dev-dml_2_1v2"}
	ok := []string{"by-dev-dml_3_1v3", "by-dev-dml_4_1v4"}
	require.NoError(t, CheckSplitChannels(shards, sources, "by-dev-dml_0_1v0", ok))

	for name, tc := range map[string]struct {
		source  string
		targets []string
	}{
		"source is not a shard":             {"by-dev-dml_9_1v9", ok},
		"source was split before":           {"by-dev-dml_2_1v2", ok},
		"source is the control channel":     {"by-dev-dml_0_vcchan", ok},
		"one target":                        {"by-dev-dml_0_1v0", ok[:1]},
		"empty target":                      {"by-dev-dml_0_1v0", []string{"", ok[1]}},
		"target is the control channel":     {"by-dev-dml_0_1v0", []string{"by-dev-dml_5_vcchan", ok[1]}},
		"target is a shard":                 {"by-dev-dml_0_1v0", []string{"by-dev-dml_1_1v1", ok[1]}},
		"target was split before":           {"by-dev-dml_0_1v0", []string{"by-dev-dml_2_1v2", ok[1]}},
		"target on the pchannel of source":  {"by-dev-dml_0_1v0", []string{"by-dev-dml_0_1v5", ok[1]}},
		"target on the pchannel of a shard": {"by-dev-dml_0_1v0", []string{"by-dev-dml_1_1v5", ok[1]}},
		"target on a split shard pchannel":  {"by-dev-dml_0_1v0", []string{"by-dev-dml_2_1v5", ok[1]}},
		"targets share a pchannel":          {"by-dev-dml_0_1v0", []string{"by-dev-dml_3_1v3", "by-dev-dml_3_1v4"}},
	} {
		t.Run(name, func(t *testing.T) {
			requireSystemError(t, CheckSplitChannels(shards, sources, tc.source, tc.targets))
		})
	}
}

func TestCheckSplitAdmission(t *testing.T) {
	require.NoError(t, CheckSplitAdmission(nil))
	require.NoError(t, CheckSplitAdmission([]*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "false"}}))
	requireSystemError(t, CheckSplitAdmission([]*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "true"}}))
	// The property parser returns an Input error; read from metadata it is not
	// the fault of a request.
	requireSystemError(t, CheckSplitAdmission([]*commonpb.KeyValuePair{{Key: common.NamespaceShardingEnabledKey, Value: "maybe"}}))
}
