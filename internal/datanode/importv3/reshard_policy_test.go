// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package importv3

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestGetReshardPolicyDerivesRunConfiguration pins every value deriveReshardPolicies
// computes once: the fragment target and its structural overhead, the whole-task
// resident ceiling (min(buckets, cap) x F), the sort-input split and the spill
// stream count. The expected values are recomputed from the live parameters and
// reshardmem so the test tracks the shared memory model instead of re-encoding
// magic numbers.
func TestGetReshardPolicyDerivesRunConfiguration(t *testing.T) {
	initReshardPipelineParams(t)
	bt := paramtable.Get()

	const fragmentTarget = int64(32 << 20)
	fix := newReshardPipelineFixture()
	plan := fix.plan([]*internalpb.ImportFile{fix.source(1)}, fragmentTarget)
	req := fix.request(plan, 2)

	policy, err := getReshardPolicy(req, plan)
	require.NoError(t, err)

	require.Equal(t, int64(2), policy.slot)

	bufferSize := bt.DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	require.Equal(t, bufferSize, policy.write.bufferSize)
	require.Equal(t, bufferSize, policy.source.bufferSize)

	require.Equal(t, fragmentTarget, policy.fragment.target)
	nFields := int64(len(typeutil.GetAllFieldSchemas(importutilv2.FragmentSchema(fix.schema, false))))
	require.Equal(t, reshardmem.FragmentOverhead(nFields), policy.fragment.overhead)

	// The whole-task resident ceiling is min(buckets, cap) x fragment target,
	// not the raw bucket count x F: the cap keeps a high-bucket job schedulable.
	bucketCount := int64(len(plan.GetVchannels()) * len(plan.GetPartitionIds()))
	bucketCap := bt.DataCoordCfg.ReshardResidentBucketCap.GetAsInt64()
	require.Equal(t, min(bucketCount, bucketCap)*fragmentTarget, policy.memory.residentBudget)

	memoryBudget := int64(2) * reshardmem.MemoryPerSlot(bt.DataNodeCfg.WorkerSlotUnit.GetAsInt64(), bt.DataNodeCfg.BuildParallel.GetAsInt64())
	require.Equal(t, memoryBudget, policy.memory.budget)
	model := reshardmem.Model{
		ReadBuffer:       bufferSize,
		FragmentTarget:   fragmentTarget,
		FlushConcurrency: bt.DataCoordCfg.ReshardFlushConcurrency.GetAsInt64(),
		ExpansionFactor:  bt.DataCoordCfg.ReshardMemoryExpansionFactor.GetAsFloat(),
	}
	require.Equal(t, model, policy.memory.model)
	require.Equal(t, model.SortInput(memoryBudget), policy.fragment.sortInput)

	// One spill stream per bucket, capped by the configured stream limit.
	require.Equal(t, int(min(bucketCount, int64(bt.DataNodeCfg.ReshardSpillMaxStreams.GetAsInt()))),
		policy.write.spillStreams)
}

// TestGetReshardPolicyClampsSlotAndRejectsBadTarget covers the two boundary
// guards: a non-positive slot is clamped to one (so the memory budget stays
// positive), while a non-positive fragment target is a hard failure because it
// would flush every non-empty bucket after every batch.
func TestGetReshardPolicyClampsSlotAndRejectsBadTarget(t *testing.T) {
	initReshardPipelineParams(t)
	bt := paramtable.Get()
	fix := newReshardPipelineFixture()

	plan := fix.plan([]*internalpb.ImportFile{fix.source(1)}, 8<<20)

	t.Run("slot clamped to one", func(t *testing.T) {
		policy, err := getReshardPolicy(fix.request(plan, 0), plan)
		require.NoError(t, err)
		require.Equal(t, int64(1), policy.slot)
		require.Equal(t, reshardmem.MemoryPerSlot(bt.DataNodeCfg.WorkerSlotUnit.GetAsInt64(), bt.DataNodeCfg.BuildParallel.GetAsInt64()),
			policy.memory.budget)
	})

	t.Run("non-positive fragment target fails", func(t *testing.T) {
		bad := fix.plan([]*internalpb.ImportFile{fix.source(1)}, 0)
		_, err := getReshardPolicy(fix.request(bad, 3), bad)
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrImportSysFailed)
	})
}

// TestGetReshardPolicyResidentBudgetAndSpillStreamsOverCap pins the cap itself:
// with more buckets than the resident cap the ceiling stays cap x F (the excess
// spills) while the spill stream count follows the bucket count up to its own
// limit.
func TestGetReshardPolicyResidentBudgetAndSpillStreamsOverCap(t *testing.T) {
	initReshardPipelineParams(t)
	bt := paramtable.Get()
	fix := newReshardPipelineFixture()

	const fragmentTarget = int64(4 << 20)
	vchannels := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		vchannels = append(vchannels, "ch")
	}
	plan := fix.plan([]*internalpb.ImportFile{fix.source(1)}, fragmentTarget)
	plan.Vchannels = vchannels

	policy, err := getReshardPolicy(fix.request(plan, 1), plan)
	require.NoError(t, err)

	bucketCap := bt.DataCoordCfg.ReshardResidentBucketCap.GetAsInt64()
	require.Less(t, bucketCap, int64(len(vchannels)), "the test needs more buckets than the cap")
	require.Equal(t, bucketCap*fragmentTarget, policy.memory.residentBudget)
	require.Equal(t, int(min(int64(len(vchannels)), int64(bt.DataNodeCfg.ReshardSpillMaxStreams.GetAsInt()))),
		policy.write.spillStreams)
}
