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

package compactor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSplitRewriteRejectsPendingSourceL0(t *testing.T) {
	plan := &datapb.CompactionPlan{SplitRewrite: &datapb.SplitShardRewritePlan{TtlCutoffTimetick: 1, RoutingModulus: 2, Targets: doublingTargets(0, 1)}, PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 10, End: 20}, SegmentBinlogs: []*datapb.CompactionSegmentBinlogs{{SegmentID: 1}, {SegmentID: 2, Level: datapb.SegmentLevel_L0}}}
	task := NewSplitShardRewriteCompactionTask(context.Background(), nil, plan, compaction.GenParams())
	require.Error(t, task.preCompact())
	plan.SegmentBinlogs = plan.SegmentBinlogs[:1]
	require.NoError(t, task.preCompact())
}

func TestSplitRewritePartitionerRejectsMalformedRouting(t *testing.T) {
	for _, tc := range []struct {
		mod     uint64
		targets []*datapb.SplitShardTaskTarget
	}{{0, doublingTargets(0, 1)}, {2, doublingTargets(0, 0)}, {2, doublingTargets(0, 2)}, {2, nil}, {2, []*datapb.SplitShardTaskTarget{{Vchannel: "a", Residues: []uint64{0}}, {Vchannel: "a", Residues: []uint64{1}}}}} {
		_, err := newHashSplitPartitioner(tc.mod, tc.targets)
		require.Error(t, err)
	}
}

func TestSplitRewriteStringPartitioningMatchesWriteRoute(t *testing.T) {
	p, err := newHashSplitPartitioner(2, doublingTargets(0, 1))
	require.NoError(t, err)
	for _, key := range []string{"one", "two", "three", "四", ""} {
		got, err := p.Route(key)
		require.NoError(t, err)
		want, err := routing.PKResidueVarChar(key, 2)
		require.NoError(t, err)
		require.EqualValues(t, want, got)
	}
}

func TestSplitRewriteWritersStayOnSourceBeforeAdoption(t *testing.T) {
	plan := &datapb.CompactionPlan{Channel: "s", Schema: recordSchema(), PreAllocatedSegmentIDs: &datapb.IDRange{Begin: 10, End: 20}, PreAllocatedLogIDs: &datapb.IDRange{Begin: 100, End: 200}}
	task := NewSplitShardRewriteCompactionTask(t.Context(), nil, plan, compaction.GenParams())
	p, err := newHashSplitPartitioner(2, doublingTargets(0, 1))
	require.NoError(t, err)
	writers, err := task.newTargetWriters(t.Context(), p)
	require.NoError(t, err)
	for _, writer := range writers {
		require.Equal(t, "s", writer.channel)
		require.NoError(t, writer.Close())
	}
}
