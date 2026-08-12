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

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
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

func TestEstimateMixCompactionV3ScalesWithInput(t *testing.T) {
	paramtable.Init()

	// V3 retains the whole input (issue #52180 incident 1), so the estimate
	// must scale. This is the defect that charged 8 tasks x 4.5GiB as 32 slots.
	got := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_MixCompaction,
		StorageVersion:  3,
		TotalMemorySize: 4 * gib,
	})

	assert.GreaterOrEqual(t, got.Memory, 4*gib)
}

func TestEstimateSortCompactionScalesWithInput(t *testing.T) {
	paramtable.Init()

	// storage.Sort retains every record before sorting.
	got := EstimateCompaction(CompactionInput{
		Type:            datapb.CompactionType_SortCompaction,
		StorageVersion:  1,
		TotalMemorySize: 2 * gib,
		TotalRows:       1_000_000,
	})

	assert.GreaterOrEqual(t, got.Memory, 2*gib)
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
