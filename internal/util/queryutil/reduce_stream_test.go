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

package queryutil

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSplitRetrieveResult(t *testing.T) {
	result := newQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	result.ReqID = 100
	result.AllRetrieveCount = 3
	result.HasMoreResult = true

	chunks, err := SplitRetrieveResult(result, queryChunkBytes(t, result, 2))

	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Equal(t, []int64{1, 2}, chunks[0].GetIds().GetIntId().GetData())
	require.Equal(t, []int64{10, 20}, chunks[0].GetFieldsData()[0].GetScalars().GetLongData().GetData())
	require.Equal(t, int64(100), chunks[0].GetReqID())
	require.Equal(t, int64(3), chunks[0].GetAllRetrieveCount())
	require.True(t, chunks[0].GetHasMoreResult())
	require.Equal(t, []int64{3}, chunks[1].GetIds().GetIntId().GetData())
	require.Equal(t, []int64{30}, chunks[1].GetFieldsData()[0].GetScalars().GetLongData().GetData())
	require.Zero(t, chunks[1].GetReqID())
	require.Zero(t, chunks[1].GetAllRetrieveCount())
	require.False(t, chunks[1].GetHasMoreResult())
}

func TestQueryStreamPreservesNullableFields(t *testing.T) {
	result := newQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	result.FieldsData = append(result.FieldsData,
		&schemapb.FieldData{Type: schemapb.DataType_VarChar, FieldId: 102,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				ValidData: []bool{true, false, true},
				Data:      &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "", "b"}}},
			}}},
		&schemapb.FieldData{Type: schemapb.DataType_FloatVector, FieldId: 103,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim: 2, ValidData: []bool{true, false, true},
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}},
			}}},
	)
	chunks, err := SplitRetrieveResult(result, 1)
	require.NoError(t, err)
	require.Len(t, chunks, 3)
	for i, chunk := range chunks {
		require.Equal(t, []bool{i != 1}, chunk.GetFieldsData()[1].GetScalars().GetValidData())
		require.Equal(t, []bool{i != 1}, chunk.GetFieldsData()[2].GetVectors().GetValidData())
	}
	child := &queryTestStream{}
	for _, chunk := range chunks {
		child.recv = append(child.recv, queryTestRecv{chunk: chunk})
	}
	stream, err := NewReduceStream(&internalpb.RetrieveRequest{Limit: 3}, nil, []ReduceStream{child}, 1<<20)
	require.NoError(t, err)
	merged, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []bool{true, false, true}, merged.GetFieldsData()[1].GetScalars().GetValidData())
	require.Equal(t, []string{"a", "", "b"}, merged.GetFieldsData()[1].GetScalars().GetStringData().GetData())
	require.Equal(t, []bool{true, false, true}, merged.GetFieldsData()[2].GetVectors().GetValidData())
	require.Equal(t, []float32{1, 2, 3, 4}, merged.GetFieldsData()[2].GetVectors().GetFloatVector().GetData())
	require.NoError(t, stream.Close())
}

func TestSplitRetrieveResultAcceptsUnitThatCrossesThreshold(t *testing.T) {
	result := newQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	chunkBytes := queryChunkBytes(t, result, 2) - 1

	chunks, err := SplitRetrieveResult(result, chunkBytes)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Equal(t, []int64{1, 2}, queryTestIDs(chunks[0]))
	require.Equal(t, []int64{3}, queryTestIDs(chunks[1]))
}

func TestSplitRetrieveResultEmitsOversizedUnitAlone(t *testing.T) {
	largeID := int64(1 << 56)
	result := newQueryTestChunk([]int64{1, largeID}, []int64{10, largeID})
	result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
		Type: schemapb.DataType_VarChar, FieldId: 102,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "a much larger row"}}},
		}},
	})
	firstUnitBytes := queryChunkBytes(t, result, 1)
	largeUnitBytes, err := (retrieveUnit{result: result, rowIndex: 1}).reducibleByteSize()
	require.NoError(t, err)
	chunkBytes := firstUnitBytes + 1
	require.Greater(t, largeUnitBytes, chunkBytes)

	chunks, err := SplitRetrieveResult(result, chunkBytes)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Equal(t, []int64{1}, queryTestIDs(chunks[0]))
	require.Equal(t, []int64{largeID}, queryTestIDs(chunks[1]))
}

func TestSplitRetrieveResultWithGroupedUnits(t *testing.T) {
	result := newGroupQueryTestChunk([]int64{1, 2, 3}, []int64{10, 20, 30})
	result.ReqID = 100
	result.AllRetrieveCount = 6

	chunks, err := SplitRetrieveResult(result, queryChunkBytes(t, result, 2))

	require.NoError(t, err)
	require.Len(t, chunks, 2)
	require.Nil(t, chunks[0].GetIds())
	require.Equal(t, []int64{1, 2}, chunks[0].GetFieldsData()[0].GetScalars().GetLongData().GetData())
	require.Equal(t, []int64{10, 20}, chunks[0].GetFieldsData()[1].GetScalars().GetLongData().GetData())
	require.Equal(t, int64(100), chunks[0].GetReqID())
	require.Equal(t, int64(6), chunks[0].GetAllRetrieveCount())
	require.Equal(t, []int64{3}, chunks[1].GetFieldsData()[0].GetScalars().GetLongData().GetData())
	require.Zero(t, chunks[1].GetReqID())
}

func TestSplitRetrieveResultIncludesElementIndexBytes(t *testing.T) {
	result := makeElementLevelResult([]int64{1, 2}, []int64{10, 20}, [][]int32{{0}, {1, 2, 3, 4, 5, 6, 7, 8}})
	expectedBytes := queryUnitReferenceSize(t, retrieveUnit{result: result})
	require.Equal(t, expectedBytes, queryChunkBytes(t, result, 1))
	chunks, err := SplitRetrieveResult(result, expectedBytes+1)
	require.NoError(t, err)
	require.Len(t, chunks, 2)
	for i, chunk := range chunks {
		require.True(t, chunk.GetElementLevel())
		require.Equal(t, []int64{int64(i + 1)}, queryTestIDs(chunk))
		require.Equal(t, result.GetElementIndices()[i], chunk.GetElementIndices()[0])
	}
}

func TestOrderedReduceStreamElementFilter(t *testing.T) {
	for _, chunkBytes := range []int{1, 1 << 20} {
		left := makeElementLevelResult([]int64{1, 3}, []int64{10, 10}, [][]int32{{0, 2, 4}, {1}})
		right := makeElementLevelResult([]int64{2, 4}, []int64{10, 10}, [][]int32{{1, 3}, {0}})
		children := make([]ReduceStream, 0, 2)
		for _, input := range []*internalpb.RetrieveResults{left, right} {
			input.Status = merr.Success()
			chunks, err := SplitRetrieveResult(input, 1)
			require.NoError(t, err)
			child := &queryTestStream{}
			for _, chunk := range chunks {
				child.recv = append(child.recv, queryTestRecv{chunk: chunk})
			}
			children = append(children, child)
		}
		stream, err := NewReduceStream(&internalpb.RetrieveRequest{Limit: 4}, nil, children, chunkBytes)
		require.NoError(t, err)
		var results []*internalpb.RetrieveResults
		for {
			chunk, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			require.NoError(t, err)
			require.True(t, chunk.GetElementLevel())
			results = append(results, chunk)
		}
		require.NoError(t, stream.Close())
		var ids []int64
		for _, chunk := range results {
			ids = append(ids, queryTestIDs(chunk)...)
		}
		require.Equal(t, []int64{1, 2}, ids)
		got, err := NewSortAndCheckPKPageOperator(0, nil, 3, 1).Run(context.Background(), nil, results)
		require.NoError(t, err)
		batch, err := NewSortAndCheckPKPageOperator(0, nil, 3, 1).Run(context.Background(), nil, []*internalpb.RetrieveResults{left, right})
		require.NoError(t, err)
		require.True(t, proto.Equal(batch[0].(*internalpb.RetrieveResults), got[0].(*internalpb.RetrieveResults)))
	}
}

func TestOrderedReduceStreamValidatesElementChunks(t *testing.T) {
	for _, inconsistent := range []bool{false, true} {
		chunk := makeElementLevelResult([]int64{1}, []int64{10}, [][]int32{{0}})
		chunk.Status = merr.Success()
		children := []ReduceStream{&queryTestStream{recv: []queryTestRecv{{chunk: chunk}}}}
		if inconsistent {
			children = append(children, &queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{2}, []int64{20})}}})
		} else {
			chunk.ElementIndices = nil
		}
		stream, err := NewReduceStream(&internalpb.RetrieveRequest{Limit: 10}, nil, children, 1)
		require.NoError(t, err)
		_, err = stream.Recv()
		require.ErrorContains(t, err, "element")
		require.NoError(t, stream.Close())
	}
}

func TestOrderedReduceStreamEmptyElementResult(t *testing.T) {
	input := makeElementLevelResult(nil, nil, nil)
	input.Status, input.HasMoreResult = merr.Success(), true
	stream, err := NewReduceStream(&internalpb.RetrieveRequest{Limit: 1}, nil, []ReduceStream{
		&queryTestStream{recv: []queryTestRecv{{chunk: input}}},
	}, 1)
	require.NoError(t, err)
	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.True(t, chunk.GetElementLevel())
	require.True(t, chunk.GetHasMoreResult())
	require.Empty(t, queryTestIDs(chunk))
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, stream.Close())
}

func TestOrderedReduceStreamMergesChildChunks(t *testing.T) {
	childA := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{1, 3}, []int64{10, 30})},
		{chunk: newQueryTestChunk([]int64{5}, []int64{50})},
	}}
	childB := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{2, 4, 6}, []int64{20, 40, 60})},
	}}
	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{IsIterator: true, Limit: 5},
		nil,
		[]ReduceStream{childA, childB},
		queryChunkBytes(t, newQueryTestChunk([]int64{1, 2}, []int64{10, 20}), 2),
	)
	require.NoError(t, err)

	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, queryTestIDs(first))

	second, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, queryTestIDs(second))

	third, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{5}, queryTestIDs(third))

	chunk, err := stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, childA.closeCount())
	require.Equal(t, 1, childB.closeCount())
}

func TestOrderedReduceStreamAcceptsBoundedOrdinaryQuery(t *testing.T) {
	child := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{1, 2}, []int64{10, 20})},
	}}
	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{Limit: 2},
		nil,
		[]ReduceStream{child},
		queryChunkBytes(t, child.recv[0].chunk, 2),
	)
	require.NoError(t, err)

	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, queryTestIDs(chunk))
}

func TestOrderedReduceStreamMergesByOrderByField(t *testing.T) {
	childA := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{4, 2}, []int64{10, 30})},
	}}
	childB := &queryTestStream{recv: []queryTestRecv{
		{chunk: newQueryTestChunk([]int64{3, 1}, []int64{20, 40})},
	}}
	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{
			Limit: 4,
			OrderByFields: []*planpb.OrderByField{
				{FieldId: 101, Ascending: true},
			},
		},
		nil,
		[]ReduceStream{childA, childB},
		queryChunkBytes(t, newQueryTestChunk([]int64{4, 3, 2, 1}, []int64{10, 20, 30, 40}), 4),
	)
	require.NoError(t, err)

	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{4, 3, 2, 1}, queryTestIDs(chunk))
	require.Equal(t, []int64{10, 20, 30, 40}, chunk.GetFieldsData()[0].GetScalars().GetLongData().GetData())
}

func TestOrderedReduceStreamDefersOversizedUnitToNextChunk(t *testing.T) {
	largeID := int64(1 << 56)
	result := newQueryTestChunk([]int64{1, largeID}, []int64{10, largeID})
	result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
		Type: schemapb.DataType_VarChar, FieldId: 102,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", "a much larger row"}}},
		}},
	})
	child := &queryTestStream{recv: []queryTestRecv{{chunk: result}}}
	chunkBytes := queryChunkBytes(t, result, 1) + 1
	largeUnitBytes, err := (retrieveUnit{result: result, rowIndex: 1}).reducibleByteSize()
	require.NoError(t, err)
	require.Greater(t, largeUnitBytes, chunkBytes)

	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{IsIterator: true, Limit: 2},
		nil,
		[]ReduceStream{child},
		chunkBytes,
	)
	require.NoError(t, err)
	first, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{1}, queryTestIDs(first))
	second, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []int64{largeID}, queryTestIDs(second))
}

func TestOrderedReduceStreamRejectsDuplicatePK(t *testing.T) {
	childA := &queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{1}, []int64{10})}}}
	childB := &queryTestStream{recv: []queryTestRecv{{chunk: newQueryTestChunk([]int64{1}, []int64{20})}}}
	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{IsIterator: true, Limit: 2},
		nil,
		[]ReduceStream{childA, childB},
		queryChunkBytes(t, newQueryTestChunk([]int64{1, 1}, []int64{10, 20}), 2),
	)
	require.NoError(t, err)

	chunk, err := stream.Recv()

	require.Nil(t, chunk)
	require.Error(t, err)
	require.Equal(t, 1, childA.closeCount())
	require.Equal(t, 1, childB.closeCount())
}

func TestUnOrderedReduceStreamMergesGroupedChunks(t *testing.T) {
	childA := &queryTestStream{recv: []queryTestRecv{
		{chunk: newGroupQueryTestChunk([]int64{1, 2}, []int64{2, 3})},
		{chunk: newGroupQueryTestChunk([]int64{3}, []int64{4})},
	}}
	childA.recv[0].chunk.AllRetrieveCount = 5
	childB := &queryTestStream{recv: []queryTestRecv{
		{chunk: newGroupQueryTestChunk([]int64{1, 3}, []int64{5, 6})},
	}}
	childB.recv[0].chunk.AllRetrieveCount = 6
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "group", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
	}}
	stream, err := NewReduceStream(
		&internalpb.RetrieveRequest{
			GroupByFieldIds: []int64{100},
			Aggregates: []*planpb.Aggregate{
				{Op: planpb.AggregateOp_count, FieldId: 101},
			},
		},
		schema,
		[]ReduceStream{childA, childB},
		1<<20,
	)
	require.NoError(t, err)
	require.IsType(t, &UnOrderedReduceStream{}, stream)

	chunk, err := stream.Recv()
	require.NoError(t, err)
	require.Nil(t, chunk.GetIds())
	require.Equal(t, int64(11), chunk.GetAllRetrieveCount())
	groups := chunk.GetFieldsData()[0].GetScalars().GetLongData().GetData()
	counts := chunk.GetFieldsData()[1].GetScalars().GetLongData().GetData()
	actual := make(map[int64]int64, len(groups))
	for i, group := range groups {
		actual[group] = counts[i]
	}
	require.Equal(t, map[int64]int64{1: 7, 2: 3, 3: 10}, actual)

	chunk, err = stream.Recv()
	require.Nil(t, chunk)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 1, childA.closeCount())
	require.Equal(t, 1, childB.closeCount())
}

func TestUnOrderedReduceStreamEmptyChildren(t *testing.T) {
	for _, test := range []struct {
		name        string
		emptyShard  bool
		emptyWorker bool
		withData    bool
	}{
		{name: "empty_shard", emptyShard: true, withData: true},
		{name: "empty_worker", emptyWorker: true, withData: true},
		{name: "all_empty_shards", emptyShard: true},
		{name: "all_empty_worker_and_shard", emptyShard: true, emptyWorker: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "group", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
			}}
			request := &internalpb.RetrieveRequest{
				GroupByFieldIds: []int64{100},
				Aggregates:      []*planpb.Aggregate{{Op: planpb.AggregateOp_count, FieldId: 101}},
			}
			children := make([]ReduceStream, 0, 3)
			if test.emptyShard {
				for range 2 {
					shard, err := NewReduceStream(request, schema, nil, 1)
					require.NoError(t, err)
					children = append(children, shard)
				}
			}
			var expectedCount, expectedBytes, expectedCost, expectedMVCC int64
			if test.emptyWorker {
				payload, err := agg.NewGroupAggReducer(request.GroupByFieldIds, request.Aggregates, -1, schema).EncodePartial(&agg.PartialAggregationResult{})
				require.NoError(t, err)
				payload.Status = merr.Success()
				payload.AllRetrieveCount = 3
				payload.ScannedTotalBytes = 11
				payload.CostAggregation = &internalpb.CostAggregation{TotalRelatedDataSize: 101}
				payload.HasMoreResult = true
				payload.MvccTimestamp = 20
				chunks, err := SplitRetrieveResult(payload, 1)
				require.NoError(t, err)
				require.Len(t, chunks, 1)
				worker := &queryTestStream{recv: []queryTestRecv{{chunk: chunks[0]}}}
				shard, err := NewReduceStream(request, schema, []ReduceStream{worker}, 1)
				require.NoError(t, err)
				children = append(children, shard)
				expectedCount, expectedBytes, expectedCost, expectedMVCC = 3, 11, 101, 20
			}
			if test.withData {
				payload := newGroupQueryTestChunk([]int64{7}, []int64{2})
				payload.AllRetrieveCount = 2
				payload.ScannedTotalBytes = 13
				payload.CostAggregation = &internalpb.CostAggregation{TotalRelatedDataSize: 202}
				payload.MvccTimestamp = 30
				worker := &queryTestStream{recv: []queryTestRecv{{chunk: payload}}}
				shard, err := NewReduceStream(request, schema, []ReduceStream{worker}, 1)
				require.NoError(t, err)
				children = append(children, shard)
				expectedCount += 2
				expectedBytes += 13
				expectedCost += 202
				expectedMVCC = 30
			}
			root, err := NewReduceStream(request, schema, children, 1)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, root.Close()) })
			result, err := root.Recv()
			require.NoError(t, err)
			require.Len(t, result.GetFieldsData(), 2)
			groups := result.GetFieldsData()[0].GetScalars().GetLongData().GetData()
			counts := result.GetFieldsData()[1].GetScalars().GetLongData().GetData()
			if test.withData {
				require.Equal(t, []int64{7}, groups)
				require.Equal(t, []int64{2}, counts)
			} else {
				require.Empty(t, groups)
				require.Empty(t, counts)
			}
			require.Equal(t, expectedCount, result.GetAllRetrieveCount())
			require.Equal(t, expectedBytes, result.GetScannedTotalBytes())
			require.Equal(t, expectedCost, result.GetCostAggregation().GetTotalRelatedDataSize())
			require.Equal(t, uint64(expectedMVCC), result.GetMvccTimestamp())
			require.Equal(t, test.emptyWorker, result.GetHasMoreResult())
			result, err = root.Recv()
			require.Nil(t, result)
			require.ErrorIs(t, err, io.EOF)
		})
	}
}

func TestUnOrderedReduceStreamPartialContracts(t *testing.T) {
	for _, test := range []struct {
		name    string
		op      planpb.AggregateOp
		value   int64
		average bool
	}{
		{name: "sum", op: planpb.AggregateOp_sum, value: 210},
		{name: "min", op: planpb.AggregateOp_min, value: 60},
		{name: "max", op: planpb.AggregateOp_max, value: 150},
		{name: "avg", op: planpb.AggregateOp_sum, value: 90, average: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "group", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64, Nullable: true},
			}}
			request := &internalpb.RetrieveRequest{GroupByFieldIds: []int64{100}, Aggregates: []*planpb.Aggregate{{Op: test.op, FieldId: 101}}}
			if test.average {
				request.Aggregates = append(request.Aggregates, &planpb.Aggregate{Op: planpb.AggregateOp_count, FieldId: 101})
			}
			payload := func(groups, values, counts []int64, validity []bool) *internalpb.RetrieveResults {
				result := newGroupQueryTestChunk(groups, values)
				typeutil.SetFieldDataValidData(result.FieldsData[1], validity)
				if test.average {
					count := newGroupQueryTestChunk(groups, counts).FieldsData[1]
					result.FieldsData = append(result.FieldsData, count)
				}
				return result
			}
			first := int64(150)
			if test.average {
				first = 30
			}
			inputs := [][]*internalpb.RetrieveResults{
				{{Status: merr.Success(), AllRetrieveCount: 3}, payload([]int64{7, 9}, []int64{0, 0}, []int64{0, 0}, []bool{false, false}), payload([]int64{7, 8}, []int64{first, 0}, []int64{2, 1}, []bool{true, true})},
				{payload([]int64{7}, []int64{60}, []int64{3}, []bool{true})},
			}
			inputs[1][0].AllRetrieveCount = 4
			shards := make([]ReduceStream, 0, len(inputs))
			for _, chunks := range inputs {
				worker := &queryTestStream{}
				for _, chunk := range chunks {
					split, err := SplitRetrieveResult(chunk, 1)
					require.NoError(t, err)
					for _, part := range split {
						worker.recv = append(worker.recv, queryTestRecv{chunk: part})
					}
				}
				shard, err := NewReduceStream(request, schema, []ReduceStream{worker}, 1)
				require.NoError(t, err)
				shards = append(shards, shard)
			}
			root, err := NewReduceStream(request, schema, shards, 1)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, root.Close()) })
			seen := map[int64]bool{}
			var count int64
			for {
				chunk, err := root.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				require.NoError(t, err)
				count += chunk.GetAllRetrieveCount()
				group := chunk.FieldsData[0].GetScalars().GetLongData().GetData()[0]
				require.False(t, seen[group])
				seen[group] = true
				field := chunk.FieldsData[1]
				validity := typeutil.GetFieldDataValidData(field)
				require.Equal(t, validity, field.GetValidData())
				require.Equal(t, validity, field.GetScalars().GetValidData())
				if group == 9 {
					require.Equal(t, []bool{false}, validity)
				} else {
					require.Equal(t, []bool{true}, validity)
					want := int64(0)
					if group == 7 {
						want = test.value
					}
					require.Equal(t, []int64{want}, field.GetScalars().GetLongData().GetData())
				}
				if test.average {
					average, err := agg.ComputeAvgFromSumAndCount(field, chunk.FieldsData[2])
					require.NoError(t, err)
					if group == 7 {
						require.Equal(t, []float64{18}, average.GetScalars().GetDoubleData().GetData())
					} else if group == 9 {
						require.Equal(t, []bool{false}, typeutil.GetFieldDataValidData(average))
					}
				}
			}
			require.Len(t, seen, 3)
			require.Equal(t, int64(7), count)
		})
	}
}

func TestUnOrderedReduceStreamRejectsMalformedPartials(t *testing.T) {
	for _, test := range []string{"column_count", "row_count", "validity_count"} {
		t.Run(test, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "group", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
			}}
			request := &internalpb.RetrieveRequest{GroupByFieldIds: []int64{100}, Aggregates: []*planpb.Aggregate{{Op: planpb.AggregateOp_sum, FieldId: 101}}}
			payload := newGroupQueryTestChunk([]int64{1}, []int64{2})
			switch test {
			case "column_count":
				payload.FieldsData = payload.FieldsData[:1]
			case "row_count":
				payload.FieldsData[1].GetScalars().GetLongData().Data = nil
			case "validity_count":
				typeutil.SetFieldDataValidData(payload.FieldsData[1], []bool{true, false})
			}
			child := &queryTestStream{recv: []queryTestRecv{{chunk: payload}}}
			stream, err := NewReduceStream(request, schema, []ReduceStream{child}, 64)
			require.NoError(t, err)
			chunk, err := stream.Recv()
			require.Nil(t, chunk)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, 1, child.closeCount())
			require.Nil(t, stream.(*UnOrderedReduceStream).outputBuffer)
		})
	}
}

func newQueryTestChunk(ids, values []int64) *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{
		Status: merr.Success(),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
					},
				},
			},
		},
	}
}

func newGroupQueryTestChunk(groups, values []int64) *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{
		Status: merr.Success(),
		FieldsData: []*schemapb.FieldData{
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 100,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: groups}},
				}},
			},
			{
				Type:    schemapb.DataType_Int64,
				FieldId: 101,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
				}},
			},
		},
	}
}

func queryChunkBytes(t *testing.T, result *internalpb.RetrieveResults, unitCount int) int {
	t.Helper()
	bytes := 0
	for row := 0; row < unitCount; row++ {
		unitBytes, err := (retrieveUnit{result: result, rowIndex: int64(row)}).reducibleByteSize()
		require.NoError(t, err)
		bytes += unitBytes
	}
	return bytes
}

func queryTestIDs(result *internalpb.RetrieveResults) []int64 {
	return result.GetIds().GetIntId().GetData()
}

type queryTestRecv struct {
	chunk *internalpb.RetrieveResults
	err   error
}

type queryTestStream struct {
	mu         sync.Mutex
	recv       []queryTestRecv
	closeCalls int
}

func (s *queryTestStream) Recv() (*internalpb.RetrieveResults, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.recv) == 0 {
		return nil, io.EOF
	}
	next := s.recv[0]
	s.recv = s.recv[1:]
	return next.chunk, next.err
}

func (s *queryTestStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalls++
	return nil
}

func (*queryTestStream) Interrupt() (*internalpb.RetrieveResults, error) {
	return nil, errors.New("not implemented")
}

func (s *queryTestStream) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalls
}
