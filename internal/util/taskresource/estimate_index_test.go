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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Reproduces issue #52180 incident 2: HNSW, 1163739 rows, dim 768.
// calculateIndexTaskSlot produced taskSlot=384 on a node reporting
// totalSlots=128, i.e. a task apparently larger than the whole node.
// Expressed as bytes it is ~6.7GiB, which fits a 64GiB node comfortably.
func TestEstimateIndexBuildIncident2FitsNode(t *testing.T) {
	paramtable.Init()

	fieldSize := int64(1_163_739) * 768 * 4 // ~3.33GiB

	got := EstimateIndexBuild(IndexInput{
		IndexType:       "HNSW",
		FieldMemorySize: fieldSize,
		StorageVersion:  2,
	})

	nodeMem := int64(64) * gib
	assert.Less(t, got.Memory, nodeMem)
	assert.GreaterOrEqual(t, got.Memory, fieldSize)
	// CPU must not scale with data volume: knowhere's build parallelism is
	// fixed, and scaling CPU is what manufactured the "bigger than the node"
	// illusion in the scalar scheme.
	assert.LessOrEqual(t, got.CPU, float64(4))
}

func TestEstimateIndexBuildCPUIsSizeIndependent(t *testing.T) {
	paramtable.Init()

	small := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: 10 * mib})
	large := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: 100 * gib})

	assert.Equal(t, small.CPU, large.CPU)
	assert.Greater(t, large.Memory, small.Memory)
}

func TestIndexBuildMemoryFactorByType(t *testing.T) {
	paramtable.Init()

	assert.Greater(t, IndexBuildMemoryFactor("HNSW"), IndexBuildMemoryFactor("IVF_FLAT"))
	assert.Less(t, IndexBuildMemoryFactor("DISKANN"), float64(1))
	// Unknown types fall back to the configured default rather than to zero.
	assert.Greater(t, IndexBuildMemoryFactor("SOME_FUTURE_INDEX"), float64(0))
}

func TestEstimateIndexBuildAddsDecodeWindowOnV3(t *testing.T) {
	paramtable.Init()

	v2 := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: gib, StorageVersion: 2})
	v3 := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: gib, StorageVersion: 3})

	// The storage-v3 in-flight decode window is pure extra peak for the
	// accumulating caller and is absent from today's Go-side estimate.
	assert.Greater(t, v3.Memory, v2.Memory)
}

func TestEstimateStatsUsesFieldNotWholeSegment(t *testing.T) {
	paramtable.Init()

	got := EstimateStats(StatsInput{
		SubJobType:      indexpb.StatsSubJob_JsonKeyIndexJob,
		FieldMemorySize: 100 * mib,
	})

	assert.Greater(t, got.Memory, int64(0))
	assert.Greater(t, got.CPU, float64(0))
}

func TestEstimateAnalyzeScalesWithInputAndIsCapped(t *testing.T) {
	paramtable.Init()

	small := EstimateAnalyze(gib)
	huge := EstimateAnalyze(1000 * gib)

	assert.Greater(t, small.Memory, int64(0))
	assert.Greater(t, huge.Memory, small.Memory)
	assert.LessOrEqual(t, huge.Memory,
		paramtable.Get().DataCoordCfg.ResourceAnalyzeMaxMemory.GetAsInt64())
}

// --- Additional coverage: each test below fails if the formula it names were
// wrong, not just if the estimator returned zero. ---

// TestEstimateIndexBuildExactFormula pins the multiplication itself: Memory
// must equal FieldMemorySize * IndexBuildMemoryFactor(type), not merely be
// "some positive number bigger than the field".
func TestEstimateIndexBuildExactFormula(t *testing.T) {
	paramtable.Init()

	const fieldSize = 2 * gib
	got := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: fieldSize, StorageVersion: 2})

	want := int64(float64(fieldSize) * IndexBuildMemoryFactor("HNSW"))
	assert.Equal(t, want, got.Memory)
	assert.Equal(t, paramtable.Get().DataCoordCfg.ResourceIndexBuildCPU.GetAsFloat(), got.CPU)
}

// TestEstimateIndexBuildTinyFieldHitsFloor proves the 64MiB floor is actually
// wired in: a field small enough that factor*size undercuts it must come back
// at exactly the floor, not at the (near-zero) raw product.
func TestEstimateIndexBuildTinyFieldHitsFloor(t *testing.T) {
	paramtable.Init()

	got := EstimateIndexBuild(IndexInput{IndexType: "DISKANN", FieldMemorySize: 1024, StorageVersion: 2})

	assert.Equal(t, int64(64)*mib, got.Memory)
}

// TestEstimateIndexBuildDecodeWindowIsExact proves the v3 branch adds exactly
// the configured decode window, not merely "some" extra memory.
func TestEstimateIndexBuildDecodeWindowIsExact(t *testing.T) {
	paramtable.Init()

	v2 := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: gib, StorageVersion: 2})
	v3 := EstimateIndexBuild(IndexInput{IndexType: "HNSW", FieldMemorySize: gib, StorageVersion: 3})

	window := paramtable.Get().DataCoordCfg.ResourceIndexDecodeWindow.GetAsInt64()
	assert.Equal(t, window, v3.Memory-v2.Memory)
}

// TestIndexBuildMemoryFactorTableValues pins the concrete numbers in the
// factor table, not just their relative order, and checks the lookup is
// case-insensitive.
func TestIndexBuildMemoryFactorTableValues(t *testing.T) {
	paramtable.Init()

	assert.Equal(t, 2.0, IndexBuildMemoryFactor("HNSW"))
	assert.Equal(t, 1.2, IndexBuildMemoryFactor("IVF_FLAT"))
	assert.Equal(t, 0.5, IndexBuildMemoryFactor("DISKANN"))
	assert.Equal(t, IndexBuildMemoryFactor("HNSW"), IndexBuildMemoryFactor("hnsw"))
}

// TestIndexBuildMemoryFactorUnknownTypeReadsConfiguredDefault proves the
// fallback actually reads the config item (not a hardcoded literal that
// happens to match its default): changing the config changes the result.
func TestIndexBuildMemoryFactorUnknownTypeReadsConfiguredDefault(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	pt.Save(pt.DataCoordCfg.ResourceIndexBuildFactorDefault.Key, "3.25")
	defer pt.Reset(pt.DataCoordCfg.ResourceIndexBuildFactorDefault.Key)

	assert.Equal(t, 3.25, IndexBuildMemoryFactor("SOME_FUTURE_INDEX"))
}

// TestEstimateStatsFactorDiffersByJobType proves EstimateStats actually
// branches on SubJobType instead of always applying one factor: with the two
// factors configured apart, JsonKeyIndexJob and TextIndexJob must charge
// different amounts for the identical field size.
func TestEstimateStatsFactorDiffersByJobType(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	pt.Save(pt.DataCoordCfg.ResourceTextIndexFactor.Key, "2.0")
	pt.Save(pt.DataCoordCfg.ResourceJSONKeyIndexFactor.Key, "5.0")
	defer pt.Reset(pt.DataCoordCfg.ResourceTextIndexFactor.Key)
	defer pt.Reset(pt.DataCoordCfg.ResourceJSONKeyIndexFactor.Key)

	const fieldSize = 100 * mib

	text := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_TextIndexJob, FieldMemorySize: fieldSize})
	bm25 := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_BM25Job, FieldMemorySize: fieldSize})
	jsonKey := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob, FieldMemorySize: fieldSize})

	assert.Equal(t, int64(float64(fieldSize)*2.0), text.Memory)
	// BM25 shares the text-index config item; there is no separate BM25 factor.
	assert.Equal(t, text.Memory, bm25.Memory)
	assert.Equal(t, int64(float64(fieldSize)*5.0), jsonKey.Memory)
	assert.Greater(t, jsonKey.Memory, text.Memory)
}

// TestEstimateStatsUnrecognizedSubJobUsesConservativeDefault pins the
// default: branch of EstimateStats's switch: StatsSubJob_Sort and the
// zero-value StatsSubJob_None (neither of which DataCoord submits today —
// Sort is estimated as CompactionType_SortCompaction via EstimateCompaction
// instead) must land on the larger of the two known factors, not silently
// inherit whichever factor happens to be declared first or the smaller one.
//
// The factors are flipped between the two halves of this test specifically
// so a default that quietly always picked one named factor (instead of
// max-ing them), or that picked the smaller one, would fail here.
func TestEstimateStatsUnrecognizedSubJobUsesConservativeDefault(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	const fieldSize = 100 * mib

	// JSON-key factor is the larger one: the default must match it.
	pt.Save(pt.DataCoordCfg.ResourceTextIndexFactor.Key, "2.0")
	pt.Save(pt.DataCoordCfg.ResourceJSONKeyIndexFactor.Key, "5.0")
	sortGot := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_Sort, FieldMemorySize: fieldSize})
	noneGot := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_None, FieldMemorySize: fieldSize})
	pt.Reset(pt.DataCoordCfg.ResourceTextIndexFactor.Key)
	pt.Reset(pt.DataCoordCfg.ResourceJSONKeyIndexFactor.Key)

	assert.Equal(t, int64(float64(fieldSize)*5.0), sortGot.Memory)
	assert.Equal(t, int64(float64(fieldSize)*5.0), noneGot.Memory)

	// Flip which factor is larger: the default must switch to tracking text,
	// not stay pinned to json-key.
	pt.Save(pt.DataCoordCfg.ResourceTextIndexFactor.Key, "7.0")
	pt.Save(pt.DataCoordCfg.ResourceJSONKeyIndexFactor.Key, "3.0")
	defer pt.Reset(pt.DataCoordCfg.ResourceTextIndexFactor.Key)
	defer pt.Reset(pt.DataCoordCfg.ResourceJSONKeyIndexFactor.Key)
	sortGot2 := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_Sort, FieldMemorySize: fieldSize})

	assert.Equal(t, int64(float64(fieldSize)*7.0), sortGot2.Memory)
}

// TestEstimateStatsFloorAppliesToTinyField proves the 64MiB floor is wired
// into EstimateStats too.
func TestEstimateStatsFloorAppliesToTinyField(t *testing.T) {
	paramtable.Init()

	got := EstimateStats(StatsInput{SubJobType: indexpb.StatsSubJob_TextIndexJob, FieldMemorySize: 1024})

	assert.Equal(t, int64(64)*mib, got.Memory)
}

// TestEstimateAnalyzeExactFormulaBelowCap pins the multiplication for an
// input comfortably under the cap.
func TestEstimateAnalyzeExactFormulaBelowCap(t *testing.T) {
	paramtable.Init()

	const size = 2 * gib
	got := EstimateAnalyze(size)

	factor := paramtable.Get().DataCoordCfg.ResourceAnalyzeFactor.GetAsFloat()
	assert.Equal(t, int64(float64(size)*factor), got.Memory)
	assert.Equal(t, 1.0, got.CPU)
}

// TestEstimateAnalyzeCapIsExact proves the cap clamps to exactly
// analyzeMaxMemory, not merely to "something under" it.
func TestEstimateAnalyzeCapIsExact(t *testing.T) {
	paramtable.Init()

	got := EstimateAnalyze(1000 * gib)

	assert.Equal(t, paramtable.Get().DataCoordCfg.ResourceAnalyzeMaxMemory.GetAsInt64(), got.Memory)
}

// TestEstimateAnalyzeFloorAppliesToTinyInput proves the 64MiB floor also
// binds EstimateAnalyze for a near-zero input.
func TestEstimateAnalyzeFloorAppliesToTinyInput(t *testing.T) {
	paramtable.Init()

	got := EstimateAnalyze(1024)

	assert.Equal(t, int64(64)*mib, got.Memory)
}
