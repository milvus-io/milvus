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

package tasks

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func hintedTask(t testing.TB, first byte, states ...int) *SearchTask {
	t.Helper()
	paramtable.Init()
	values := make([][]byte, len(states))
	queries := make([]*querypb.QueryClusterSearchHints, len(states))
	for i, state := range states {
		values[i] = make([]byte, 8)
		binary.LittleEndian.PutUint32(values[i], uint32(first)+uint32(i))
		queries[i] = &querypb.QueryClusterSearchHints{}
		if state >= 0 {
			queries[i].Hints = &querypb.ClusterSearchHintList{}
		}
		if state > 0 {
			queries[i].Hints.Ranges = []*querypb.ClusterSearchHint{{RowOffset: int64(state), RowCount: 1, CentroidDistance: 0.5}}
		}
	}
	ph, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Tag: "$0", Type: commonpb.PlaceholderType_FloatVector, Values: values}}})
	require.NoError(t, err)
	req := &querypb.SearchRequest{Req: &internalpb.SearchRequest{CollectionID: 1, PartitionIDs: []int64{2}, FieldId: 100, MetricType: "L2", Nq: int64(len(states)), Topk: 4, PlaceholderGroup: ph}, DmlChannels: []string{"ch"}, SegmentIDs: []int64{10}, Scope: querypb.DataScope_Historical, ClusteringSearchHints: map[int64]*querypb.SegmentClusterSearchHints{10: {SnapshotVersion: 100, NumRows: 100, Queries: queries}}}
	return NewSearchTask(context.Background(), nil, nil, req, 1)
}

func TestClusteringHintMergeRejectsFunctionChains(t *testing.T) {
	for _, stage := range []schemapb.FunctionChainStage{schemapb.FunctionChainStage_FunctionChainStageL0Rerank, schemapb.FunctionChainStage_FunctionChainStageL1Rerank} {
		t.Run(stage.String(), func(t *testing.T) {
			a, b := hintedTask(t, 10, 1, 0), hintedTask(t, 20, -1)
			plan, err := proto.Marshal(&planpb.PlanNode{QuerynodeFunctionChains: []*schemapb.FunctionChain{{Stage: stage}}})
			require.NoError(t, err)
			a.req.Req.SerializedExprPlan, b.req.Req.SerializedExprPlan = plan, plan
			before := proto.Clone(a.req)
			require.False(t, a.Merge(b))
			require.True(t, proto.Equal(before, a.req))
			require.Nil(t, a.mergedHints)
			require.Empty(t, a.others)
			require.False(t, b.merged)
		})
	}
}

func TestClusteringHintMergeOwnershipAndOrder(t *testing.T) {
	a, b := hintedTask(t, 10, 1, 0), hintedTask(t, 20, -1)
	b.req.ClusteringSearchHints = nil
	original := proto.Clone(a.req)
	require.True(t, a.Merge(b))
	require.True(t, proto.Equal(original, a.req))
	hints := taskClusteringHints(a)[10]
	require.Len(t, hints.Queries, 3)
	require.NotNil(t, hints.Queries[0].Hints)
	require.NotNil(t, hints.Queries[1].Hints)
	require.Empty(t, hints.Queries[1].Hints.Ranges)
	require.Nil(t, hints.Queries[2].GetHints())
	require.NoError(t, a.combinePlaceHolderGroups())
	var group commonpb.PlaceholderGroup
	require.NoError(t, proto.Unmarshal(a.placeholderGroup, &group))
	require.Equal(t, []uint32{10, 11, 20}, []uint32{binary.LittleEndian.Uint32(group.Placeholders[0].Values[0]), binary.LittleEndian.Uint32(group.Placeholders[0].Values[1]), binary.LittleEndian.Uint32(group.Placeholders[0].Values[2])})
	before := append([]byte(nil), a.placeholderGroup...)
	require.NoError(t, a.combinePlaceHolderGroups())
	require.Equal(t, before, a.placeholderGroup)
	a.req.ClusteringSearchHints[10].Queries[0].Hints.Ranges[0].RowOffset = 99
	require.Equal(t, int64(1), hints.Queries[0].Hints.Ranges[0].RowOffset)
	require.False(t, a.Merge(hintedTask(t, 30, 2)))
}

func TestClusteringHintMergeNestedGroups(t *testing.T) {
	a, b, c, d := hintedTask(t, 10, 1, 0), hintedTask(t, 20, -1), hintedTask(t, 30, 0), hintedTask(t, 40, 2, -1)
	b.req.ClusteringSearchHints = nil
	require.True(t, c.Merge(d))
	require.True(t, a.Merge(b))
	require.True(t, a.Merge(c))
	require.Equal(t, []int64{2, 1, 1, 2}, a.originNqs)
	require.Len(t, a.others, 3)
	require.Empty(t, c.others)
	require.Equal(t, int64(6), a.NQ())
	require.Len(t, taskClusteringHints(a)[10].Queries, 6)
	for i, task := range []*SearchTask{a, b, c, d} {
		require.Same(t, task, a.subTaskAt(i))
	}
	require.NoError(t, a.combinePlaceHolderGroups())
	var group commonpb.PlaceholderGroup
	require.NoError(t, proto.Unmarshal(a.placeholderGroup, &group))
	var order []uint32
	for _, v := range group.Placeholders[0].Values {
		order = append(order, binary.LittleEndian.Uint32(v))
	}
	require.Equal(t, []uint32{10, 11, 20, 30, 40, 41}, order)
	a.Done(nil)
	for _, task := range []*SearchTask{a, b, c, d} {
		require.Len(t, task.notifier, 1)
		require.NoError(t, task.Wait())
		require.Empty(t, task.notifier)
	}
}

func TestClusteringHintMergeRejectsIncompatible(t *testing.T) {
	for name, change := range map[string]func(*SearchTask){
		"snapshot":      func(x *SearchTask) { x.req.ClusteringSearchHints[10].SnapshotVersion++ },
		"row_domain":    func(x *SearchTask) { x.req.ClusteringSearchHints[10].NumRows++ },
		"shape":         func(x *SearchTask) { x.req.ClusteringSearchHints[10].Queries = nil },
		"range":         func(x *SearchTask) { x.req.ClusteringSearchHints[10].Queries[0].Hints.Ranges[0].RowCount = 101 },
		"outside_scope": func(x *SearchTask) { x.req.ClusteringSearchHints[99] = x.req.ClusteringSearchHints[10] },
		"field":         func(x *SearchTask) { x.req.Req.FieldId++ },
		"ttl":           func(x *SearchTask) { x.req.Req.CollectionTtlTimestamps++ },
		"scope":         func(x *SearchTask) { x.req.Scope = querypb.DataScope_Streaming },
		"placeholder":   func(x *SearchTask) { x.placeholderGroup = []byte("bad") },
	} {
		t.Run(name, func(t *testing.T) {
			a, b := hintedTask(t, 10, 1), hintedTask(t, 20, 2)
			change(b)
			before := proto.Clone(a.req)
			require.False(t, a.Merge(b))
			require.True(t, proto.Equal(before, a.req))
			require.Equal(t, int64(1), a.nq)
		})
	}
}

func TestClusteringHintMergeDifferentTopks(t *testing.T) {
	a, b := hintedTask(t, 10, 1, 0), hintedTask(t, 20, 2)
	b.topk = 8
	b.originTopks = []int64{8}
	b.req.Req.Topk = 8
	require.True(t, a.Merge(b))
	require.Equal(t, []int64{4, 8}, a.originTopks)
	require.Equal(t, int64(8), a.topk)
}
