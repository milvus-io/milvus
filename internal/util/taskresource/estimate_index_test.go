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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// testNodeMemory is the node size every analyze test below is written
// against: the 64GiB DataNode from issue #52180. EstimateAnalyze now reads the
// node, so leaving it at whatever the build host happens to have would make
// every assertion below machine-dependent.
// It is a var rather than a const so the fractional products below (0.8 x it,
// 0.3 x it) are ordinary runtime arithmetic instead of untyped constants that
// have to divide exactly.
var testNodeMemory = int64(64) * gib

func mockNodeMemory(t *testing.T, total int64) {
	t.Helper()
	mk := mockey.Mock(hardware.GetMemoryCount).Return(uint64(total)).Build()
	t.Cleanup(func() { mk.UnPatch() })
}

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
	// fixed by its pool, and scaling CPU is what manufactured the "bigger than
	// the node" illusion in the scalar scheme. HNSW is a vector index, so the
	// charge is one slot of that pool -- whatever the build host's core count
	// makes that -- and never a function of the 3.33GiB field.
	assert.Equal(t, VectorIndexBuildCPU(), got.CPU)
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

// TestEstimateStatsUsesFieldNotWholeSegment exercises the claim its name
// makes, which means going through RequirementForStats: EstimateStats alone is
// handed the field size and cannot possibly disagree with it.
//
// The fixture is a segment whose json field is a small part of the whole, so
// the two answers are far apart and both comfortably clear the 64MiB floor.
func TestEstimateStatsUsesFieldNotWholeSegment(t *testing.T) {
	paramtable.Init()

	const jsonBytes = 400 * mib
	const otherBytes = 4 * gib

	req := &workerpb.CreateStatsRequest{
		SubJobType: indexpb.StatsSubJob_JsonKeyIndexJob,
		Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, DataType: schemapb.DataType_JSON},
				{FieldID: 101, DataType: schemapb.DataType_FloatVector},
			},
		},
		InsertLogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: jsonBytes}}},
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: otherBytes}}},
		},
	}

	got := RequirementForStats(req)

	factor := paramtable.Get().DataCoordCfg.ResourceJSONKeyIndexFactor.GetAsFloat()
	want := int64(float64(jsonBytes) * factor)
	require.Greater(t, want, int64(64)*mib, "setup: the expected value must not be the floor")
	assert.Equal(t, want, got.Memory)
	assert.Greater(t, got.CPU, float64(0))

	// The whole-segment charge is what this is meant to be cheaper than; if the
	// field selection silently fell through, that is the number it would land on.
	wholeSegment := int64(float64(jsonBytes+otherBytes) * factor)
	assert.Less(t, got.Memory, wholeSegment)
}

func TestEstimateAnalyzeScalesWithInputAndIsCapped(t *testing.T) {
	paramtable.Init()

	mockNodeMemory(t, testNodeMemory)

	small := EstimateAnalyze(gib, 0.8)
	huge := EstimateAnalyze(1000*gib, 0.8)

	assert.Greater(t, small.Memory, int64(0))
	assert.Greater(t, huge.Memory, small.Memory)
	// The cap now bounds the phase-2 grant only; what actually binds a large
	// dataset is the training buffer the task allocates today.
	assert.LessOrEqual(t, huge.Memory, int64(float64(testNodeMemory)*0.8))
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
	assert.Equal(t, VectorIndexBuildCPU(), got.CPU)
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

// TestEstimateAnalyzeExactFormulaBelowCap pins the multiplication for a
// dataset smaller than the training buffer: the dataset bounds the charge,
// because the training set cannot be larger than the data that exists.
func TestEstimateAnalyzeExactFormulaBelowCap(t *testing.T) {
	paramtable.Init()
	mockNodeMemory(t, testNodeMemory)

	const size = 2 * gib
	got := EstimateAnalyze(size, 0.8)

	// 0.8 x 64GiB = 51.2GiB of buffer, but only 2GiB of data exists.
	assert.Equal(t, size, got.Memory)
	assert.Equal(t, 1.0, got.CPU)
	require.Greater(t, got.Memory, int64(64)*mib, "must clear the floor to pin the formula")
}

// TestEstimateAnalyzeChargesTheBufferTheTaskActuallyAllocates is the C3
// regression. Until phase 2 converts the task, task_analyze.go sets
// TrainSize = GetMemoryCount() x MaxTrainSizeRatio -- 51.2GiB on this node --
// regardless of the grant. The old estimator answered min(dataset, 4GiB)
// here, so a node that was about to load 0.8 of its RAM reported ~94% free;
// the 65535-slot constant that used to serialize analyze no longer does
// anything, because phase 0 reroutes availability onto the ledger.
func TestEstimateAnalyzeChargesTheBufferTheTaskActuallyAllocates(t *testing.T) {
	paramtable.Init()
	mockNodeMemory(t, testNodeMemory)

	// A dataset far larger than the buffer: the buffer is what binds.
	got := EstimateAnalyze(1000*gib, 0.8)

	assert.Equal(t, int64(float64(testNodeMemory)*0.8), got.Memory)
	// The point of the fix: this must be well past the old cap, which is what
	// made the task look free.
	assert.Greater(t, got.Memory,
		10*paramtable.Get().DataCoordCfg.ResourceAnalyzeMaxMemory.GetAsInt64())
	// And past the node's own task budget, so the guard treats it as oversized
	// and runs it alone -- the behavior analyzeTaskSlotUsage=65535 used to buy.
	assert.Greater(t, got.Memory, int64(float64(testNodeMemory)*
		paramtable.Get().DataNodeCfg.ResourceMemoryRatio.GetAsFloat()))
}

// A legacy request that never filled MaxTrainSizeRatio must not be read as
// "this task allocates nothing"; it falls back to the config DataCoord fills
// the field from.
func TestEstimateAnalyzeMissingRatioFallsBackToConfig(t *testing.T) {
	paramtable.Init()
	mockNodeMemory(t, testNodeMemory)

	ratio := paramtable.Get().DataCoordCfg.ClusteringCompactionMaxTrainSizeRatio.GetAsFloat()
	require.Greater(t, ratio, 0.0, "setup: the fallback must be a real ratio")

	got := EstimateAnalyze(1000*gib, 0)

	assert.Equal(t, int64(float64(testNodeMemory)*ratio), got.Memory)
}

// An unknown dataset size (a field type with no closed-form width) must not
// bound the charge downwards: the buffer is allocated either way.
func TestEstimateAnalyzeUnknownDatasetStillChargesTheBuffer(t *testing.T) {
	paramtable.Init()
	mockNodeMemory(t, testNodeMemory)

	got := EstimateAnalyze(0, 0.8)

	assert.Equal(t, int64(float64(testNodeMemory)*0.8), got.Memory)
}

// TestEstimateAnalyzeFloorAppliesToTinyInput proves the 64MiB floor also
// binds EstimateAnalyze for a near-zero input.
func TestEstimateAnalyzeFloorAppliesToTinyInput(t *testing.T) {
	paramtable.Init()
	mockNodeMemory(t, testNodeMemory)

	got := EstimateAnalyze(1024, 0.8)

	assert.Equal(t, int64(64)*mib, got.Memory)
}
