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

package segcore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestComputeScorerScoresOnChunkedOffsetsValidation(t *testing.T) {
	offsets := newInt64Chunked(t, []int64{0})
	defer offsets.Release()

	segment := newBoostScoreTestSegment(t)
	searchReq := newBoostScoreTestSearchRequest(t)

	t.Run("nil segment", func(t *testing.T) {
		scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(nil, searchReq, 0, offsets)
		require.Error(t, err)
		require.Nil(t, scores)
		require.Contains(t, err.Error(), "segment is nil")
	})

	t.Run("nil search request", func(t *testing.T) {
		scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(segment, nil, 0, offsets)
		require.Error(t, err)
		require.Nil(t, scores)
		require.Contains(t, err.Error(), "search request or plan is nil")
	})

	t.Run("negative scorer index", func(t *testing.T) {
		scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(segment, searchReq, -1, offsets)
		require.Error(t, err)
		require.Nil(t, scores)
		require.Contains(t, err.Error(), "scorer index must be non-negative")
	})

	t.Run("scorer index out of range", func(t *testing.T) {
		scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(segment, searchReq, 2, offsets)
		require.Error(t, err)
		require.Nil(t, scores)
		require.Contains(t, err.Error(), "scorer index 2 is out of range")
	})

	t.Run("nil offsets", func(t *testing.T) {
		scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(segment, searchReq, 0, nil)
		require.Error(t, err)
		require.Nil(t, scores)
		require.Contains(t, err.Error(), "offsets is nil")
	})

	t.Run("non int64 offsets", func(t *testing.T) {
		badOffsets := newFloat32Chunked(t, []float32{0})
		defer badOffsets.Release()

		scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(segment, searchReq, 0, badOffsets)
		require.Error(t, err)
		require.Nil(t, scores)
		require.Contains(t, err.Error(), "offset column must be Int64")
	})

	t.Run("null offset", func(t *testing.T) {
		badOffsets := newNullableInt64Chunked(t)
		defer badOffsets.Release()

		scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(segment, searchReq, 0, badOffsets)
		require.Error(t, err)
		require.Nil(t, scores)
		require.Contains(t, err.Error(), "offset chunk 0 contains null")
	})

	t.Run("empty offsets", func(t *testing.T) {
		emptyOffsets := arrow.NewChunked(arrow.PrimitiveTypes.Int64, nil)
		defer emptyOffsets.Release()

		scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(segment, searchReq, 0, emptyOffsets)
		require.NoError(t, err)
		require.NotNil(t, scores)
		defer scores.Release()
		require.Equal(t, arrow.PrimitiveTypes.Float32, scores.DataType())
		require.Empty(t, scores.Chunks())
	})
}

func TestComputeScorerScoresOnChunkedOffsets(t *testing.T) {
	collection, segment, searchReq := newBoostScoreTestContext(t)
	defer collection.Release()
	defer segment.Release()
	defer searchReq.Delete()

	offsets := newInt64Chunked(t, []int64{0, 1}, []int64{2, 3})
	defer offsets.Release()

	scores, err := segcore.ComputeScorerScoresOnChunkedOffsets(
		segment,
		searchReq,
		0,
		offsets,
	)
	require.NoError(t, err)
	requireBoostScores(t, scores, [][]float32{{2.5, 2.5}, {2.5, 2.5}})
}

func TestAsyncComputeScorerScoresOnChunkedOffsets(t *testing.T) {
	collection, segment, searchReq := newBoostScoreTestContext(t)
	defer collection.Release()
	defer segment.Release()
	defer searchReq.Delete()

	offsets := newInt64Chunked(t, []int64{0, 1, 2})
	defer offsets.Release()

	scores, err := segcore.AsyncComputeScorerScoresOnChunkedOffsets(
		context.Background(),
		segment,
		searchReq,
		1,
		offsets,
	)
	require.NoError(t, err)
	requireBoostScores(t, scores, [][]float32{{3.5, 3.5, 3.5}})
}

func newBoostScoreTestContext(t *testing.T) (*segcore.CCollection, segcore.CSegment, *segcore.SearchRequest) {
	t.Helper()
	initBoostScoreTestEnv(t)

	const (
		collectionID = int64(100)
		partitionID  = int64(10)
		segmentID    = int64(1)
	)

	schema := mock_segcore.GenTestCollectionSchema("boost-score-suite", schemapb.DataType_Int64, false)
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: collectionID,
		Schema:       schema,
		IndexMeta:    mock_segcore.GenTestIndexMeta(collectionID, schema),
	})
	require.NoError(t, err)

	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  collection,
		SegmentID:   segmentID,
		SegmentType: segcore.SegmentTypeGrowing,
		IsSorted:    false,
	})
	require.NoError(t, err)

	insertMsg, err := mock_segcore.GenInsertMsg(collection, partitionID, segmentID, 10)
	require.NoError(t, err)
	_, err = segment.Insert(context.Background(), &segcore.InsertRequest{
		RowIDs:     insertMsg.RowIDs,
		Timestamps: insertMsg.Timestamps,
		Record: &segcorepb.InsertRecord{
			FieldsData: insertMsg.FieldsData,
			NumRows:    int64(len(insertMsg.RowIDs)),
		},
	})
	require.NoError(t, err)

	searchReq := newBoostScoreTestSearchRequestForCollection(t, collection, segmentID)

	return collection, segment, searchReq
}

func newBoostScoreTestSearchRequestForCollection(t *testing.T, collection *segcore.CCollection, segmentID int64) *segcore.SearchRequest {
	t.Helper()
	var vectorField *schemapb.FieldSchema
	for _, field := range collection.Schema().GetFields() {
		if field.GetDataType() == schemapb.DataType_FloatVector {
			vectorField = field
			break
		}
	}
	require.NotNil(t, vectorField)

	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{
			VectorType:     planpb.VectorType_FloatVector,
			FieldId:        vectorField.GetFieldID(),
			QueryInfo:      &planpb.QueryInfo{Topk: 10, MetricType: "L2", SearchParams: `{"nprobe":10}`, RoundDecimal: -1},
			PlaceholderTag: "$0",
		}},
		Scorers: []*planpb.ScoreFunction{
			{Weight: 2.5, Type: planpb.FunctionType_FunctionTypeWeight},
			{Weight: 3.5, Type: planpb.FunctionType_FunctionTypeWeight},
		},
	}
	serializedPlan, err := proto.Marshal(plan)
	require.NoError(t, err)
	placeholderGroup, err := proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_FloatVector,
			Values: [][]byte{make([]byte, mock_segcore.DefaultDim*4)},
		}},
	})
	require.NoError(t, err)

	req := &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			CollectionID:       collection.ID(),
			PlaceholderGroup:   placeholderGroup,
			SerializedExprPlan: serializedPlan,
			DslType:            commonpb.DslType_BoolExprV1,
			Nq:                 1,
			MvccTimestamp:      typeutil.MaxTimestamp,
		},
		SegmentIDs: []int64{segmentID},
		Scope:      querypb.DataScope_Historical,
	}
	searchReq, err := segcore.NewSearchRequest(collection, req, placeholderGroup)
	require.NoError(t, err)
	return searchReq
}

func initBoostScoreTestEnv(t *testing.T) {
	t.Helper()
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	require.NoError(t, initcore.InitLocalChunkManager(localDataRootPath))
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
}

func newBoostScoreTestSegment(t *testing.T) segcore.CSegment {
	t.Helper()
	collection, segment, searchReq := newBoostScoreTestContext(t)
	t.Cleanup(collection.Release)
	t.Cleanup(segment.Release)
	t.Cleanup(searchReq.Delete)
	return segment
}

func newBoostScoreTestSearchRequest(t *testing.T) *segcore.SearchRequest {
	t.Helper()
	collection, segment, searchReq := newBoostScoreTestContext(t)
	t.Cleanup(collection.Release)
	t.Cleanup(segment.Release)
	t.Cleanup(searchReq.Delete)
	return searchReq
}

func newInt64Chunked(t *testing.T, chunks ...[]int64) *arrow.Chunked {
	t.Helper()
	arrays := make([]arrow.Array, 0, len(chunks))
	for _, values := range chunks {
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.AppendValues(values, nil)
		chunk := builder.NewArray()
		builder.Release()
		arrays = append(arrays, chunk)
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int64, arrays)
	for _, chunk := range arrays {
		chunk.Release()
	}
	return chunked
}

func newNullableInt64Chunked(t *testing.T) *arrow.Chunked {
	t.Helper()
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(1)
	builder.AppendNull()
	chunk := builder.NewArray()
	builder.Release()
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{chunk})
	chunk.Release()
	return chunked
}

func newFloat32Chunked(t *testing.T, values []float32) *arrow.Chunked {
	t.Helper()
	builder := array.NewFloat32Builder(memory.DefaultAllocator)
	builder.AppendValues(values, nil)
	chunk := builder.NewArray()
	builder.Release()
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{chunk})
	chunk.Release()
	return chunked
}

func requireBoostScores(t *testing.T, scores *arrow.Chunked, expected [][]float32) {
	t.Helper()
	require.NotNil(t, scores)
	defer scores.Release()
	require.Equal(t, arrow.PrimitiveTypes.Float32, scores.DataType())
	require.Len(t, scores.Chunks(), len(expected))
	for chunkIdx, expectedChunk := range expected {
		chunk := scores.Chunk(chunkIdx).(*array.Float32)
		require.Equal(t, len(expectedChunk), chunk.Len())
		require.Zero(t, chunk.NullN())
		for rowIdx, expectedScore := range expectedChunk {
			require.InDelta(t, expectedScore, chunk.Value(rowIdx), 1e-6)
		}
	}
}
