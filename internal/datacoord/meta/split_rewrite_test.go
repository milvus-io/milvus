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

package meta

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSplitRewriteCommitsLabelsInSourceAndRetainsInputsOnFailure(t *testing.T) {
	ctx := t.Context()
	m, err := NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, m.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: 1, CollectionID: 100, PartitionID: 1, InsertChannel: "s", Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, NumOfRows: 5, TransformStartAfterTimetick: 50})))
	task := &datapb.CompactionTask{PlanID: 7, CollectionID: 100, InputSegments: []int64{1}, Channel: "s", Type: datapb.CompactionType_SplitShardRewriteCompaction, Schema: &schemapb.CollectionSchema{}, SplitRewrite: &datapb.SplitShardRewritePlan{RoutingModulus: 2, Targets: []*datapb.SplitShardTaskTarget{{Vchannel: "t1", Residues: []uint64{0}}, {Vchannel: "t2", Residues: []uint64{1}}}}}
	result := &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{SegmentID: 2, NumOfRows: 3, SplitTarget: "t1"}, {SegmentID: 3, NumOfRows: 2, SplitTarget: "t2"}}}
	failure := merr.WrapErrServiceUnavailableMsg("injected rewrite commit failure")
	patch := mockey.Mock(mockey.GetMethod(m.Catalog, "Update")).Return(failure).Build()
	defer patch.UnPatch()
	_, err = m.CompleteCompactionMutation(ctx, task, result)
	require.ErrorIs(t, err, failure)
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 1).GetState())
	require.Nil(t, m.GetSegment(ctx, 2))
	patch.UnPatch()
	outputs, err := m.CompleteCompactionMutation(ctx, task, result)
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	for _, output := range outputs {
		require.Equal(t, "s", output.GetInsertChannel())
		require.NotEmpty(t, output.GetSplitTarget())
		require.EqualValues(t, 50, output.GetTransformStartAfterTimetick())
	}
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 1).GetState())
	_, err = m.CompleteCompactionMutation(ctx, task, result)
	require.NoError(t, err, "lost task-state write can replay already committed results")
}
