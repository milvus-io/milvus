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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestIsHashSplittable(t *testing.T) {
	cases := []struct {
		name string
		coll *collectionInfo
		want bool
	}{
		{
			// Every collection routes the same way now, so what decides this is
			// only which value gets hashed: a primary key, or a namespace.
			name: "primary-key collection is size-splittable",
			coll: &collectionInfo{Schema: &schemapb.CollectionSchema{}},
			want: true,
		},
		{
			name: "an already-split primary-key collection still is",
			coll: &collectionInfo{
				Schema:         &schemapb.CollectionSchema{},
				RoutingModulus: 8,
			},
			want: true,
		},
		{
			name: "a collection placed by namespace takes the relabel path instead",
			coll: &collectionInfo{
				Schema: &schemapb.CollectionSchema{EnableNamespace: true},
				Properties: map[string]string{
					common.NamespaceShardingEnabledKey: "true",
					common.NamespaceModeKey:            common.NamespaceModePartitionKey,
				},
			},
			want: false,
		},
		{
			// The default namespace collection: sharding.enabled is written as
			// false at create time, so its rows are placed by primary key and it
			// is the hash trigger's, like any other primary-key collection.
			name: "a namespace collection placed by primary key is hash-split",
			coll: &collectionInfo{
				Schema:     &schemapb.CollectionSchema{EnableNamespace: true},
				Properties: map[string]string{common.NamespaceShardingEnabledKey: "false"},
			},
			want: true,
		},
		{name: "nil collection", coll: nil, want: false},
		{name: "no schema", coll: &collectionInfo{}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isHashSplittable(tc.coll))
		})
	}
}

func TestResiduesOfALegacyCollection(t *testing.T) {
	// A collection that has never been split carries no explicit residues:
	// shard i owns residue i at modulus N, exactly the legacy hash%N. This
	// equivalence is what lets an existing collection be split with no
	// migration.
	coll := &collectionInfo{
		ID:            1,
		VChannelNames: []string{"ch0", "ch1", "ch2"},
	}
	residues, err := residuesOf(coll)
	require.NoError(t, err)
	assert.EqualValues(t, 3, residues.modulus)
	for i, vchannel := range coll.VChannelNames {
		own, err := residues.of(vchannel)
		require.NoError(t, err)
		assert.Equal(t, []uint64{uint64(i)}, own)
	}
}

func TestResiduesOfASplitCollection(t *testing.T) {
	coll := &collectionInfo{
		ID:             1,
		RoutingModulus: 8,
		VChannelNames:  []string{"ch0", "ch1"},
		ShardInfos: map[string]*schemapb.CollectionShardInfo{
			// deliberately unsorted, to prove residuesOf normalizes
			"ch0": {Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{5, 1, 3, 7}},
			}},
			"ch1": {Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{0, 2, 4, 6}},
			}},
		},
	}
	residues, err := residuesOf(coll)
	require.NoError(t, err)
	assert.EqualValues(t, 8, residues.modulus)

	own, err := residues.of("ch0")
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 3, 5, 7}, own)

	// Exactly one shard owns each residue, which is what makes ownerOf a lookup.
	for r := uint64(0); r < 8; r++ {
		owner, ok := residues.ownerOf(r)
		require.True(t, ok, "residue %d", r)
		if r%2 == 0 {
			assert.Equal(t, "ch1", owner)
		} else {
			assert.Equal(t, "ch0", owner)
		}
	}
	_, ok := residues.ownerOf(8)
	assert.False(t, ok)
}

func TestResiduesOfRejectsMalformedMeta(t *testing.T) {
	// Residues present but no modulus to read them against.
	_, err := residuesOf(&collectionInfo{
		ID:            1,
		VChannelNames: []string{"ch0"},
		ShardInfos: map[string]*schemapb.CollectionShardInfo{
			"ch0": {Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{0}},
			}},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no routing modulus")

	// A shard with no residues among shards that have them is a hole: it can
	// never be written to and can never be split.
	_, err = residuesOf(&collectionInfo{
		ID:             1,
		RoutingModulus: 2,
		VChannelNames:  []string{"ch0", "hole"},
		ShardInfos: map[string]*schemapb.CollectionShardInfo{
			"ch0": {Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{0, 1}},
			}},
			"hole": {State: schemapb.ShardState_ShardNormal},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "owns no residue")

	// A collection with no routable shard at all.
	_, err = residuesOf(&collectionInfo{ID: 1})
	require.Error(t, err)

	// A vchannel absent from its collection.
	residues, err := residuesOf(&collectionInfo{ID: 1, VChannelNames: []string{"a"}})
	require.NoError(t, err)
	_, err = residues.of("missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not a routable shard")
}

func TestPlanHashSplitDoublesTheModulusForASingleResidue(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	coll := &collectionInfo{ID: 1, VChannelNames: []string{"ch0", "ch1"}}

	targets, modulus, err := mgr.planHashSplit(coll, "ch0")
	require.NoError(t, err)
	require.Len(t, targets, 2)

	// ch0 owned residue 0 at modulus 2 and had nothing left to divide, so the
	// modulus doubles and the residue is cut on one more hash bit into 0 and 2,
	// which together cover exactly what ch0 covered.
	assert.EqualValues(t, 4, modulus)
	assert.Equal(t, []uint64{0}, targets[0].GetBuckets())
	assert.Equal(t, []uint64{2}, targets[1].GetBuckets())
	for hash := uint64(0); hash < 40; hash++ {
		wasSource := hash%2 == 0
		isTarget := hash%4 == 0 || hash%4 == 2
		assert.Equal(t, wasSource, isTarget, "hash %d", hash)
	}
	// The vchannels are allocated later, in Preparing.
	assert.Empty(t, targets[0].GetVchannel())
	assert.Empty(t, targets[1].GetVchannel())
}

func TestPlanHashSplitDividesAMultiResidueShardWithoutMovingTheModulus(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	coll := &collectionInfo{
		ID:             1,
		RoutingModulus: 8,
		VChannelNames:  []string{"wide", "rest"},
		ShardInfos: map[string]*schemapb.CollectionShardInfo{
			"wide": {Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{0, 2, 4, 6}},
			}},
			"rest": {Routing: &schemapb.CollectionShardInfo_HashRouting{
				HashRouting: &schemapb.HashRouting{Buckets: []uint64{1, 3, 5, 7}},
			}},
		},
	}

	targets, modulus, err := mgr.planHashSplit(coll, "wide")
	require.NoError(t, err)
	require.Len(t, targets, 2)
	// The set is divided; the modulus stays put, which is what keeps a deep
	// split from growing the modulus without bound.
	assert.EqualValues(t, 8, modulus)
	assert.Equal(t, []uint64{0, 2}, targets[0].GetBuckets())
	assert.Equal(t, []uint64{4, 6}, targets[1].GetBuckets())
}

func TestShouldSplitBySizeIgnoresNamespaceCount(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	require.NoError(t, params.Save(params.DataCoordCfg.ShardSplitMaxShardRows.Key, "100"))
	defer params.Reset(params.DataCoordCfg.ShardSplitMaxShardRows.Key)

	// Over the row threshold: split.
	assert.True(t, mgr.shouldSplitBySize(&shardStats{rows: 200}))
	// Under it: no split, however many namespaces it holds (that threshold is
	// a namespace-collection concept and must not fire here).
	assert.False(t, mgr.shouldSplitBySize(&shardStats{rows: 10, namespaceCount: 1_000_000}))
}

func TestTotalActiveTaskCountSpansBothKinds(t *testing.T) {
	// The concurrency cap must bound the cluster's total split work, so a
	// hash task and a namespace task both count against it.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)

	mgr.tasks.Insert(1, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         1, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing,
	})
	mgr.tasks.Insert(2, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         2, State: datapb.SplitShardTaskState_SplitShardTaskDone,
	})
	mgr.tasks.Insert(3, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         3, State: datapb.SplitShardTaskState_SplitShardTaskRedistributing,
	})
	mgr.tasks.Insert(4, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         4, State: datapb.SplitShardTaskState_SplitShardTaskDone,
	})

	// One active of each kind.
	assert.Equal(t, 2, mgr.activeTaskCount())
}

func TestHasActiveHashTaskOnVChannelCoversSourceAndTargets(t *testing.T) {
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.tasks.Insert(7, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         7,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: hashSrcVChannel}},
		State:          datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Targets: []*datapb.SplitShardTaskTarget{
			{Vchannel: hashTgtA}, {Vchannel: hashTgtB},
		},
	})

	assert.True(t, mgr.hasActiveHashTaskOnVChannel(hashSrcVChannel), "the source is busy")
	assert.True(t, mgr.hasActiveHashTaskOnVChannel(hashTgtA), "a target is busy")
	assert.True(t, mgr.hasActiveHashTaskOnVChannel(hashTgtB))
	assert.False(t, mgr.hasActiveHashTaskOnVChannel("unrelated"))

	// A finished task frees its vchannels again.
	mgr.tasks.Insert(7, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         7,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: hashSrcVChannel}},
		State:          datapb.SplitShardTaskState_SplitShardTaskDone,
	})
	assert.False(t, mgr.hasActiveHashTaskOnVChannel(hashSrcVChannel))
}

func TestIsVChannelSplittingCoversHashTasks(t *testing.T) {
	// The freeze predicate gates compaction/GC on a splitting shard. A hash
	// split rewrites its source segments, so a concurrent compaction would
	// replace the very segments being rewritten — the freeze must cover it.
	m := newHashRewriteMeta(nil)
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.tasks.Insert(7, &datapb.SplitShardTask{
		Redistribution: datapb.SplitShardRedistribution_SplitShardRewrite,
		TaskId:         7,
		Sources:        []*datapb.SplitShardTaskSource{{Vchannel: hashSrcVChannel}},
		State:          datapb.SplitShardTaskState_SplitShardTaskRedistributing,
		Targets:        []*datapb.SplitShardTaskTarget{{Vchannel: hashTgtA}},
	})

	assert.True(t, mgr.IsVChannelSplitting(hashSrcVChannel))
	assert.True(t, mgr.IsVChannelSplitting(hashTgtA))
	assert.False(t, mgr.IsVChannelSplitting("unrelated"))
}

func TestDetectHashSplitRespectsFeatureSwitch(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataCoordCfg.ShardSplitEnable.Key, "false"))
	defer params.Reset(params.DataCoordCfg.ShardSplitEnable.Key)
	defer params.Reset(params.DataCoordCfg.ShardSplitAutoTriggerEnable.Key)

	m := newHashRewriteMeta([]int64{101})
	mgr, _ := newHashSplitTestManager(t, m)
	mgr.detectHashSplitOnce()
	assert.Equal(t, 0, mgr.activeTaskCount(), "the switch is off, nothing may be created")
}

// The per-collection half of the mode: with the cluster trigger ON, a manual
// collection is skipped while its automatic neighbor is not. Before the
// property existed, sizing one collection by hand meant turning the trigger off
// for every collection in the cluster.
func TestIsHashSplittableRespectsThePerCollectionMode(t *testing.T) {
	hashColl := func(props map[string]string) *collectionInfo {
		return &collectionInfo{
			ID:             1,
			Schema:         &schemapb.CollectionSchema{Name: "mode_test"},
			RoutingModulus: 0,
			Properties:     props,
		}
	}

	assert.True(t, isHashSplittable(hashColl(nil)),
		"a collection that never chose a mode is managed for the user")
	assert.True(t, isHashSplittable(hashColl(map[string]string{
		common.CollectionShardSplitMode: common.ShardSplitModeAuto,
	})))
	assert.False(t, isHashSplittable(hashColl(map[string]string{
		common.CollectionShardSplitMode: common.ShardSplitModeManual,
	})), "a collection the user sizes by hand is not the trigger's to touch")
	// Case and padding are tolerated, matching what the setter accepts.
	assert.False(t, isHashSplittable(hashColl(map[string]string{
		common.CollectionShardSplitMode: " Manual ",
	})))
	// An unparseable stored value reads as the default rather than failing the
	// scan: it was refused when it was set.
	assert.True(t, isHashSplittable(hashColl(map[string]string{
		common.CollectionShardSplitMode: "manul",
	})))
}

// The two predicates must stay apart. Folding the mode into isHashRouted once
// made StartRehash refuse a manual collection with "is not hash-routed" -- the
// one kind of collection a user is allowed to rehash by hand, told it is
// something it is not.
func TestHashRoutedIgnoresTheMode(t *testing.T) {
	manual := &collectionInfo{
		ID:             1,
		Schema:         &schemapb.CollectionSchema{Name: "mode_test"},
		RoutingModulus: 0,
		Properties: map[string]string{
			common.CollectionShardSplitMode: common.ShardSplitModeManual,
		},
	}
	assert.True(t, isHashRouted(manual),
		"a manual collection is still hash-routed: a rehash means something for it")
	assert.False(t, isHashSplittable(manual),
		"but the automatic trigger must not touch it")

	// A collection PLACED by namespace routes by it, and its splits are driven by
	// namespace count rather than size, so the size trigger leaves it alone.
	namespaced := &collectionInfo{
		ID:     2,
		Schema: &schemapb.CollectionSchema{Name: "ns_test", EnableNamespace: true},
		Properties: map[string]string{
			common.NamespaceShardingEnabledKey: "true",
			common.NamespaceModeKey:            common.NamespaceModePartitionKey,
		},
	}
	assert.False(t, isHashRouted(namespaced))
	assert.False(t, isHashSplittable(namespaced))

	// A namespace collection whose rows are placed by primary key -- the default,
	// sharding.enabled=false -- is hash-routed like any other. Routing it by
	// namespace would split its new rows from its existing ones.
	pkPlaced := &collectionInfo{
		ID:         3,
		Schema:     &schemapb.CollectionSchema{Name: "ns_default", EnableNamespace: true},
		Properties: map[string]string{common.NamespaceShardingEnabledKey: "false"},
	}
	assert.True(t, isHashRouted(pkPlaced))
	assert.True(t, isHashSplittable(pkPlaced))
}
