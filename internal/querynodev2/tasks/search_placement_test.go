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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// distanceDF builds a reduce input with a $distance column: one chunk per query.
func distanceDF(t *testing.T, chunks ...[]float32) *chain.DataFrame {
	t.Helper()
	pool := memory.NewGoAllocator()
	arrays := make([]arrow.Array, len(chunks))
	sizes := make([]int64, len(chunks))
	for i, values := range chunks {
		b := array.NewFloat32Builder(pool)
		b.AppendValues(values, nil)
		arrays[i] = b.NewArray()
		b.Release()
		sizes[i] = int64(len(values))
	}
	builder := chain.NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes(sizes)
	require.NoError(t, builder.AddColumnFromChunks(distanceColumnName, arrays))
	return builder.Build()
}

func TestPlacementProjectionItems(t *testing.T) {
	plan := &planpb.PlanNode{ProjectionItems: []*planpb.ProjectionItem{
		{OutputId: -1000, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentID},
		{OutputId: -1001, VirtualColumn: planpb.VirtualColumn_VirtualColumnScore},
		{OutputId: -1002, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentOffset},
		{OutputId: -1003, VirtualColumn: planpb.VirtualColumn_VirtualColumnDistance},
	}}
	items := placementProjectionItems(plan)
	require.Len(t, items, 3)
	require.Equal(t, []int64{-1000, -1002, -1003}, []int64{items[0].GetOutputId(), items[1].GetOutputId(), items[2].GetOutputId()})
	require.Empty(t, placementProjectionItems(nil))
	require.Empty(t, placementProjectionItems(&planpb.PlanNode{}))
}

func TestSearchPlacementAppendColumns(t *testing.T) {
	// Two segments, two queries. Query 0 merges rows from both inputs, query
	// 1 takes rows from the second input only.
	sources := [][]segmentSource{
		{{InputIdx: 1, SegOffset: 7, OriginalIdx: 0}, {InputIdx: 0, SegOffset: 3, OriginalIdx: 1}, {InputIdx: 1, SegOffset: 9, OriginalIdx: 1}},
		{{InputIdx: 1, SegOffset: 2, OriginalIdx: 1}},
	}
	df0 := distanceDF(t, []float32{0.5, 0.25}, []float32{})
	defer df0.Release()
	df1 := distanceDF(t, []float32{0.125, 0.75}, []float32{1.5, 2.5})
	defer df1.Release()

	p := &searchPlacement{
		items: []*planpb.ProjectionItem{
			{OutputId: -1000, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentID},
			{OutputId: -1001, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentOffset},
			{OutputId: -1002, VirtualColumn: planpb.VirtualColumn_VirtualColumnDistance},
			{OutputId: -1003, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentID},
		},
		segmentIDs: []int64{501, 502},
		segDFs:     []*chain.DataFrame{df0, df1},
		nqOffsets:  []int{0},
	}
	result := &schemapb.SearchResultData{FieldsData: []*schemapb.FieldData{{FieldId: 101}}}
	require.NoError(t, p.appendColumns(0, sources, result))
	require.Len(t, result.FieldsData, 5)
	require.Equal(t, int64(-1000), result.FieldsData[1].GetFieldId())
	require.Equal(t, schemapb.DataType_Int64, result.FieldsData[1].GetType())
	require.Equal(t, []int64{502, 501, 502, 502}, result.FieldsData[1].GetScalars().GetLongData().GetData())
	require.Equal(t, []int64{7, 3, 9, 2}, result.FieldsData[2].GetScalars().GetLongData().GetData())
	require.Equal(t, schemapb.DataType_Float, result.FieldsData[3].GetType())
	require.Equal(t, []float32{0.125, 0.25, 0.75, 2.5}, result.FieldsData[3].GetScalars().GetFloatData().GetData())
	require.Equal(t, []int64{502, 501, 502, 502}, result.FieldsData[4].GetScalars().GetLongData().GetData())

	// A reduce range that starts at the second query reads the second chunk.
	p.nqOffsets = []int{0, 1}
	ranged := &schemapb.SearchResultData{}
	require.NoError(t, p.appendColumns(1, sources[1:], ranged))
	require.Equal(t, []float32{2.5}, ranged.FieldsData[2].GetScalars().GetFloatData().GetData())

	// No rows: the columns are present and empty.
	empty := &schemapb.SearchResultData{}
	require.NoError(t, p.appendColumns(0, nil, empty))
	require.Len(t, empty.FieldsData, 4)
	require.Empty(t, empty.FieldsData[0].GetScalars().GetLongData().GetData())

	// A request without such items is untouched.
	var none *searchPlacement
	require.NoError(t, none.appendColumns(0, sources, empty))
	require.Len(t, empty.FieldsData, 4)
}

func TestSearchPlacementErrors(t *testing.T) {
	df := distanceDF(t, []float32{0.5})
	defer df.Release()
	segID := []*planpb.ProjectionItem{{OutputId: -1000, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentID}}
	offset := []*planpb.ProjectionItem{{OutputId: -1000, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentOffset}}
	distance := []*planpb.ProjectionItem{{OutputId: -1000, VirtualColumn: planpb.VirtualColumn_VirtualColumnDistance}}
	run := func(items []*planpb.ProjectionItem, dfs []*chain.DataFrame, src segmentSource) error {
		p := &searchPlacement{items: items, segmentIDs: []int64{501}, segDFs: dfs, nqOffsets: []int{0}}
		return p.appendColumns(0, [][]segmentSource{{src}}, &schemapb.SearchResultData{})
	}
	require.ErrorContains(t, run(segID, nil, segmentSource{InputIdx: 1}), "reduce input 1 out of range")
	require.ErrorContains(t, run(offset, nil, segmentSource{InputIdx: 0, SegOffset: -1}), "segment offset is not available")
	require.ErrorContains(t, run(distance, nil, segmentSource{InputIdx: 0}), "reduce input 0 is not available")
	require.ErrorContains(t, run(distance, []*chain.DataFrame{df}, segmentSource{InputIdx: 0, OriginalIdx: 4}), "row 4 out of range")

	p := &searchPlacement{items: distance, segmentIDs: []int64{501}, segDFs: []*chain.DataFrame{df}, nqOffsets: []int{0}}
	require.ErrorContains(t, p.appendColumns(2, [][]segmentSource{{{InputIdx: 0}}}, &schemapb.SearchResultData{}), "reduce range 2 out of range")
	require.ErrorContains(t, p.appendColumns(0, [][]segmentSource{{}, {{InputIdx: 0}}}, &schemapb.SearchResultData{}), "query 1 out of range")

	noColumn := chain.NewDataFrameBuilder()
	noColumn.SetChunkSizes([]int64{0})
	bare := noColumn.Build()
	defer bare.Release()
	require.ErrorContains(t, run(distance, []*chain.DataFrame{bare}, segmentSource{InputIdx: 0}), "has no $distance column")
}
