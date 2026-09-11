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

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestEstimateMixCompactionV1IsInputIndependent(t *testing.T) {
	paramtable.Init()

	// V1 reads chunk by chunk, so a 100x larger input must not change the
	// estimate beyond its delta term.
	small := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  1,
		TotalMemorySize: 1 * gib,
	})
	large := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  1,
		TotalMemorySize: 100 * gib,
	})

	assert.Equal(t, small.Memory, large.Memory)
	assert.Greater(t, small.Memory, int64(0))
}

// TestEstimateMixCompactionV3ScalesWithInput is the estimator at the center of
// incidents 1 and 3, so it is asserted against the formula rather than against
// a bound. A single input size with a GreaterOrEqual against the true value
// tests neither the scaling nor the factor: a constant return passes it, and so
// does a factor ten times too large.
func TestEstimateMixCompactionV3ScalesWithInput(t *testing.T) {
	paramtable.Init()

	factor := paramtable.Get().DataCoordCfg.ResourceMixCompactionV3Factor.GetAsFloat()
	require.Greater(t, factor, 0.0, "setup: the factor must be live for the exact assertions below")

	v3 := func(input int64) Requirement {
		return EstimateCompaction(CompactionInput{
			Type:            datapb.CompactionType_MixCompaction,
			StorageVersion:  3,
			TotalMemorySize: input,
		})
	}

	// Exact, at two sizes far apart. V3 retains the whole input (issue #52180
	// incident 1); this is the defect that charged 8 tasks x 4.5GiB as 32 slots.
	small := v3(4 * gib)
	large := v3(40 * gib)
	assert.Equal(t, int64(float64(4*gib)*factor), small.Memory)
	assert.Equal(t, int64(float64(40*gib)*factor), large.Memory)

	// Linear, not merely monotonic: ten times the input, ten times the charge.
	assert.Equal(t, 10*small.Memory, large.Memory)

	// Both are well clear of the atLeast(binlogChunkBytes) floor, so neither
	// assertion above is being satisfied by the clamp.
	require.Greater(t, small.Memory, binlogChunkBytes())

	// And the delete payload is a separate additive term, not folded into the
	// factor.
	withDeletes := EstimateCompaction(CompactionInput{
		Type:                  datapb.CompactionType_MixCompaction,
		StorageVersion:        3,
		TotalMemorySize:       4 * gib,
		MaxSegmentDeleteBytes: 512 * mib,
	})
	assert.Equal(t, small.Memory+512*mib, withDeletes.Memory)
}

// Same shape for sort: storage.Sort Retains every record from every reader
// before sorting, then holds one rowIndex per surviving row, so both terms
// must be visible in the answer.
func TestEstimateSortCompactionScalesWithInput(t *testing.T) {
	paramtable.Init()

	const rowIndexBytes = 8
	expansion := arrowExpansion()
	chunk := binlogChunkBytes()

	sort := func(input, rows int64) Requirement {
		return EstimateCompaction(CompactionInput{
			Type:            datapb.CompactionType_SortCompaction,
			StorageVersion:  1,
			TotalMemorySize: input,
			TotalRows:       rows,
		})
	}

	got := sort(2*gib, 1_000_000)
	want := int64(float64(2*gib)*expansion) + 1_000_000*rowIndexBytes + chunk
	assert.Equal(t, want, got.Memory)
	require.Greater(t, got.Memory, chunk, "must clear the floor to pin the formula")

	// The data term scales linearly...
	bigger := sort(20*gib, 1_000_000)
	assert.Equal(t, int64(float64(20*gib)*expansion)+1_000_000*rowIndexBytes+chunk, bigger.Memory)

	// ...and the row term is genuinely separate from it: same bytes, ten times
	// the rows, and the difference is exactly the rowIndex array.
	moreRows := sort(2*gib, 10_000_000)
	assert.Equal(t, got.Memory+9_000_000*rowIndexBytes, moreRows.Memory)
}

// TestEstimateClusteringChargesTheBufferTheTaskActuallyAllocates is the C3
// regression for clustering. Until phase 2 converts the task,
// clusteringCompactor sizes its write buffer as GetMemoryCount() x
// memoryBufferRatio -- 19.2GiB on a 64GiB node -- regardless of input size,
// and the grant below caps out at 8GiB. Phase 0 rerouted availability onto the
// ledger, so clusteringCompactionSlotUsage=65535 no longer serializes anything.
func TestEstimateClusteringChargesTheBufferTheTaskActuallyAllocates(t *testing.T) {
	paramtable.Init()
	mockNodeMemory(t, testNodeMemory)

	ratio := paramtable.Get().DataNodeCfg.ClusteringCompactionMemoryBufferRatio.GetAsFloat()
	require.Greater(t, ratio, 0.0, "setup: the buffer ratio must be live")

	// A tiny input: the grant would be the configured minimum, but the task
	// still allocates the whole buffer.
	got := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_ClusteringCompaction,
		TotalMemorySize: 100 * mib,
	})

	buffer := int64(float64(testNodeMemory) * ratio)
	assert.Equal(t, buffer, got.Memory)
	assert.Greater(t, got.Memory,
		paramtable.Get().DataCoordCfg.ResourceClusteringMaxMemory.GetAsInt64(),
		"the real allocation must dominate the phase-2 grant's cap")
}

func TestEstimateL0CompactionUsesDeleteBytesNotRowCount(t *testing.T) {
	paramtable.Init()

	// The old formula divided TotalRows by BloomFilterApplyBatchSize, a
	// quantity unrelated to memory. Deletion volume is what actually lands
	// in allDelta.
	few := EstimateCompaction(CompactionInput{
		Type:                  datapb.CompactionType_Level0DeleteCompaction,
		TotalRows:             1_000_000,
		MaxSegmentDeleteBytes: 1 * mib,
	})
	many := EstimateCompaction(CompactionInput{
		Type:                  datapb.CompactionType_Level0DeleteCompaction,
		TotalRows:             1_000_000,
		MaxSegmentDeleteBytes: 1 * gib,
	})

	assert.Greater(t, many.Memory, few.Memory)
}

func TestEstimateL0CompactionSentinelBatchSizeFallsBackPositive(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()

	// deleteBytes is chosen large enough that neither case below is masked by
	// the atLeast(mem, binlogChunkBytes()) floor (default 64MiB): both must
	// exceed it purely from deleteBytes, so the allowance term's sign and
	// magnitude are visible in the final Memory value, not hidden by the floor.
	const deleteBytes = 100 * mib

	// L0CompactionMaxBatchSize's default is -1 ("no limit" sentinel, a segment
	// count). Without the <=0 guard, the term would be -1*mib and this
	// assertion would see deleteBytes-mib instead of deleteBytes+32*mib.
	pt.Save(pt.DataNodeCfg.L0CompactionMaxBatchSize.Key, "-1")
	sentinel := EstimateCompaction(CompactionInput{
		Type:                  datapb.CompactionType_Level0DeleteCompaction,
		MaxSegmentDeleteBytes: deleteBytes,
	})
	pt.Reset(pt.DataNodeCfg.L0CompactionMaxBatchSize.Key)

	assert.Equal(t, deleteBytes+defaultL0BatchSize*mib, sentinel.Memory)
	assert.Greater(t, sentinel.Memory, deleteBytes, "fallback allowance must be a positive contribution")

	// A real, positive configured batch count must dominate the sentinel
	// fallback, not be silently equal to or smaller than it.
	pt.Save(pt.DataNodeCfg.L0CompactionMaxBatchSize.Key, "100")
	defer pt.Reset(pt.DataNodeCfg.L0CompactionMaxBatchSize.Key)
	configured := EstimateCompaction(CompactionInput{
		Type:                  datapb.CompactionType_Level0DeleteCompaction,
		MaxSegmentDeleteBytes: deleteBytes,
	})

	assert.Equal(t, deleteBytes+100*mib, configured.Memory)
	assert.Greater(t, configured.Memory, sentinel.Memory)
}

func TestEstimateCompactionAlwaysPositive(t *testing.T) {
	paramtable.Init()

	for _, typ := range []datapb.CompactionType{
		datapb.CompactionType_MixCompaction,
		datapb.CompactionType_SortCompaction,
		datapb.CompactionType_Level0DeleteCompaction,
		datapb.CompactionType_ClusteringCompaction,
		datapb.CompactionType_BumpSchemaVersionCompaction,
	} {
		got := EstimateCompaction(CompactionInput{Type: typ})
		assert.Greater(t, got.Memory, int64(0), "type %s", typ.String())
		assert.Greater(t, got.CPU, float64(0), "type %s", typ.String())
	}
}

func TestEstimateClusteringCompactionRespectsMinMaxAndScales(t *testing.T) {
	paramtable.Init()
	// This test is about the phase-2 grant's clamps, so the node is mocked
	// small enough that the buffer the task allocates today (0.3 x node) stays
	// below every figure asserted below and cannot mask them.
	// TestEstimateClusteringChargesTheBufferTheTaskActuallyAllocates covers the
	// other side.
	mockNodeMemory(t, gib)

	// Below the floor: the grant clamps up to clusteringMinMemory (default
	// 512MiB), not down to ~0.3 * TotalMemorySize.
	tiny := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_ClusteringCompaction,
		TotalMemorySize: 1 * mib,
	})
	assert.Equal(t, int64(536870912), tiny.Memory)

	// Above the ceiling: the grant clamps down to clusteringMaxMemory (default
	// 8GiB), not up to ~0.3 * TotalMemorySize.
	huge := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_ClusteringCompaction,
		TotalMemorySize: 1000 * gib,
	})
	assert.Equal(t, int64(8589934592), huge.Memory)

	// In between, the grant scales with input at the configured factor (0.3)
	// rather than sitting at either clamp.
	mid := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_ClusteringCompaction,
		TotalMemorySize: 10 * gib,
	})
	assert.Equal(t, int64(float64(10*gib)*0.3), mid.Memory)
	assert.Greater(t, mid.Memory, tiny.Memory)
	assert.Less(t, mid.Memory, huge.Memory)
}

// The comment on estimateClusteringCompaction makes a claim about defaults --
// that clustering is bounded by the budget rather than serialized like analyze,
// and that two run concurrently on the incident node. Prose that specific
// should be executable, or it rots.
func TestClusteringIsBoundedByTheBudgetNotSerialized(t *testing.T) {
	paramtable.Init()
	mockNodeMemory(t, testNodeMemory)
	mkCPU := mockey.Mock(hardware.GetCPUNum).Return(16).Build()
	defer mkCPU.UnPatch()

	capacity := NodeCapacity()
	require.Equal(t, int64(48)*gib, capacity.Memory, "setup: 64GiB x memoryRatio 0.75")

	req := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_ClusteringCompaction,
		TotalMemorySize: 100 * mib,
	})

	// Not oversized: unlike analyze, it fits the node.
	assert.True(t, req.FitsIn(capacity), "clustering must not be classified oversized under defaults")

	// Two fit, three do not -- so the ledger bounds it at two rather than
	// serializing it, which is a real capacity gain over the 65535 sentinel.
	two := req.Add(req)
	assert.True(t, two.FitsIn(capacity), "two clustering compactions must fit a 64GiB node")
	assert.False(t, two.Add(req).FitsIn(capacity), "three must not")

	// The contrast that makes the distinction meaningful: analyze IS oversized
	// on the same node, because 0.8 exceeds the 0.75 memory ratio.
	analyze := EstimateAnalyze(1000*gib, 0.8)
	assert.False(t, analyze.FitsIn(capacity), "analyze must still be oversized under defaults")
}
