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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	ctask "github.com/milvus-io/milvus/internal/datacoord/compaction/task"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSplitCompactionPublicPlanPreservesOutcome(t *testing.T) {
	server := &Server{domainMetas: domainMetas{compactionTaskMeta: ctask.NewTestCompactionTaskMeta(t)}}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	server.compactionTaskMeta.SaveCompactionTaskMemory(&datapb.CompactionTask{
		PlanID: 10, TriggerID: 100, CollectionID: 1, PartitionID: 2, Channel: "source",
		Type: datapb.CompactionType_SplitShardRewriteCompaction, State: datapb.CompactionTaskState_failed,
		InputSegments: []int64{3}, ResultSegments: []int64{5, 6}, FailReason: "rewrite failed",
	})
	server.compactionTaskMeta.SaveCompactionTaskMemory(&datapb.CompactionTask{
		PlanID: 11, TriggerID: 101, CollectionID: 1,
		Type: datapb.CompactionType_MixCompaction, State: datapb.CompactionTaskState_completed,
	})
	server.compactionTaskMeta.SaveCompactionTaskMemory(&datapb.CompactionTask{
		PlanID: 12, TriggerID: 102, CollectionID: 2,
		Type: datapb.CompactionType_SplitShardRewriteCompaction, State: datapb.CompactionTaskState_executing,
	})

	response, err := server.GetCompactionStateWithPlans(t.Context(), &milvuspb.GetCompactionPlansRequest{CollectionId: 1})
	require.NoError(t, merr.CheckRPCCall(response, err))
	wire, err := proto.Marshal(response)
	require.NoError(t, err)
	decoded := &milvuspb.GetCompactionPlansResponse{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.Equal(t, commonpb.CompactionState_Completed, decoded.GetState())
	require.Len(t, decoded.GetMergeInfos(), 2)
	plan := decoded.GetMergeInfos()[0]
	// Split rewrite has no named public type. Its outcome and all targets remain visible.
	require.Equal(t, commonpb.CompactionType_CompactionTypeUndefined, plan.GetType())
	require.Equal(t, commonpb.CompactionTaskState_CompactionTaskStateFailed, plan.GetState())
	require.Equal(t, "rewrite failed", plan.GetFailureReason())
	require.EqualValues(t, 10, plan.GetPlanId())
	require.EqualValues(t, 100, plan.GetTriggerId())
	require.EqualValues(t, 1, plan.GetCollectionId())
	require.EqualValues(t, 2, plan.GetPartitionId())
	require.Equal(t, "source", plan.GetChannel())
	require.Equal(t, []int64{3}, plan.GetSources())
	require.Equal(t, []int64{5, 6}, plan.GetTargets())
	require.EqualValues(t, 5, plan.GetTarget())
	require.EqualValues(t, 11, decoded.GetMergeInfos()[1].GetPlanId())
	require.Equal(t, commonpb.CompactionType_CompactionTypeMix, decoded.GetMergeInfos()[1].GetType())
	require.Equal(t, datapb.CompactionType_SplitShardRewriteCompaction,
		server.compactionTaskMeta.GetCompactionTasksByCollection(1)[100][0].GetType())
}
