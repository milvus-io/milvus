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

package taskresource

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// withNode pins the two inputs every CPU charge is derived from, so none of
// these tests depends on the core count of whatever machine built them.
func withNode(t *testing.T, cores int, vecPoolWidth string) {
	t.Helper()
	paramtable.Init()

	mk := mockey.Mock(hardware.GetCPUNum).Return(cores).Build()
	t.Cleanup(func() { mk.UnPatch() })

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceCPURatio.Key, "1.0")
	t.Cleanup(func() { pt.Reset(pt.DataNodeCfg.ResourceCPURatio.Key) })
	pt.Save(pt.DataNodeCfg.MaxVecIndexBuildConcurrency.Key, vecPoolWidth)
	t.Cleanup(func() { pt.Reset(pt.DataNodeCfg.MaxVecIndexBuildConcurrency.Key) })
}

// The defining property of the pool-share charge: exactly poolWidth builds fill
// the node's CPU dimension, and the next one overflows it.
//
// That is what makes the local cap and the coordinator's placement agree. If
// this drifted low the coordinator would keep feeding a worker whose build pool
// is already saturated -- the fifth build would queue behind the pool instead of
// running on an idle worker, which is the concentration this charge exists to
// prevent. If it drifted high the worker would report itself full with build
// slots still free.
func TestVectorIndexBuildCPUFillsTheNodeAtPoolWidth(t *testing.T) {
	withNode(t, 16, "4")

	capacity := NodeCapacity().CPU
	require.Equal(t, float64(16), capacity)

	per := VectorIndexBuildCPU()
	assert.Equal(t, float64(4), per)
	assert.Equal(t, capacity, per*4, "poolWidth builds must fill the node exactly")
	assert.Greater(t, per*5, capacity, "and the next one must overflow it")
}

// The charge follows the pool, not a constant: maxVecIndexBuildConcurrency is
// hot-reloadable and resizes the pool at runtime (resizeVecIndexBuildPool in
// internal/datanode/index/pool.go), so a charge that ignored it would disagree
// with the pool the moment an operator tuned it.
func TestVectorIndexBuildCPUTracksThePoolWidth(t *testing.T) {
	withNode(t, 16, "8")
	assert.Equal(t, float64(2), VectorIndexBuildCPU())

	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.MaxVecIndexBuildConcurrency.Key, "2")
	assert.Equal(t, float64(8), VectorIndexBuildCPU())
}

// A vector build must cost strictly more than a scalar one. This is the whole
// asymmetry: before it, both charged a flat core, so a coordinator had no way
// to tell a node running four HNSW builds from one running four INVERTED
// builds.
func TestVectorIndexCostsMoreCPUThanScalarIndex(t *testing.T) {
	withNode(t, 16, "4")

	vec := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: gib})
	scalar := EstimateIndexBuild(IndexInput{IndexType: "INVERTED", FieldMemorySize: gib})

	assert.Equal(t, float64(4), vec.CPU)
	assert.Equal(t, NominalCPU(), scalar.CPU)
	assert.Greater(t, vec.CPU, scalar.CPU)
}

// Stats sub-jobs and analyze run inline on the index scheduler's goroutine --
// their IsVectorIndex is a literal false -- so they must not be charged a pool
// slot they never take.
func TestStatsAndAnalyzeTakeTheNominalCharge(t *testing.T) {
	withNode(t, 16, "4")

	stats := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_TextIndexJob, FieldMemorySize: gib})
	analyze := EstimateAnalyze(gib, 0.1)

	assert.Equal(t, NominalCPU(), stats.CPU)
	assert.Equal(t, NominalCPU(), analyze.CPU)
	assert.Less(t, stats.CPU, VectorIndexBuildCPU())
}

func TestIsVectorIndexTypeClassification(t *testing.T) {
	for _, scalar := range []string{"INVERTED", "BITMAP", "STL_SORT", "TRIE", "Trie", "HYBRID", "NGRAM", "FMINDEX", "RTREE", "inverted"} {
		assert.False(t, IsVectorIndexType(scalar), "%s is a scalar index", scalar)
	}
	for _, vec := range []string{"HNSW", "IVF_FLAT", "DISKANN", "SCANN", "SPARSE_WAND", "hnsw"} {
		assert.True(t, IsVectorIndexType(vec), "%s is a vector index", vec)
	}

	// An unrecognized type is charged as a vector index: that is the larger
	// request, so the node reports itself full sooner and the work is spread.
	assert.True(t, IsVectorIndexType("SOME_FUTURE_INDEX"))

	// An absent type is not, because knowhere's own predicate answers false for
	// it and the runtime would not route it to the pool either. Claiming a slot
	// the task will not take would hold capacity back for nothing.
	assert.False(t, IsVectorIndexType(""))
}

// PoolShareCPU is a divisor away from a crash and a zero away from switching the
// CPU dimension off entirely; both degenerate inputs are reachable from config.
func TestPoolShareCPUHandlesDegenerateInputs(t *testing.T) {
	withNode(t, 16, "4")

	assert.Equal(t, float64(16), PoolShareCPU(0), "a zero-width pool is one worker, not a division by zero")
	assert.Equal(t, float64(16), PoolShareCPU(-3))

	// cpuRatio 0 leaves the node with no cores to divide. Falling through to
	// the nominal charge keeps the dimension meaningful instead of pricing
	// every task at zero.
	pt := paramtable.Get()
	pt.Save(pt.DataNodeCfg.ResourceCPURatio.Key, "0")
	defer pt.Reset(pt.DataNodeCfg.ResourceCPURatio.Key)
	assert.Equal(t, NominalCPU(), PoolShareCPU(4))
	assert.Equal(t, NominalCPU(), VectorIndexBuildCPU())
}
