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
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
)

func TestBoostScoreColumn(t *testing.T) {
	require.Equal(t, "boost_score_0", boostScoreColumn(0))
	require.Equal(t, "boost_score_3", boostScoreColumn(3))
}

func withBoostScoreCheckedAllocator(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	oldAllocator := defaultAllocator
	defaultAllocator = pool
	t.Cleanup(func() {
		defaultAllocator = oldAllocator
		pool.AssertSize(t, 0)
	})
}

func makeBoostScoreTestDF(t *testing.T, ids []int64, scores []float32, offsets []int64, chunkSizes []int64) *chain.DataFrame {
	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes(chunkSizes)

	idChunks := make([]arrow.Array, len(chunkSizes))
	scoreChunks := make([]arrow.Array, len(chunkSizes))
	offsetChunks := make([]arrow.Array, len(chunkSizes))

	pos := 0
	for chunkIdx, size := range chunkSizes {
		idBuilder := array.NewInt64Builder(defaultAllocator)
		scoreBuilder := array.NewFloat32Builder(defaultAllocator)
		offsetBuilder := array.NewInt64Builder(defaultAllocator)
		for rowIdx := int64(0); rowIdx < size; rowIdx++ {
			idBuilder.Append(ids[pos])
			scoreBuilder.Append(scores[pos])
			offsetBuilder.Append(offsets[pos])
			pos++
		}
		idChunks[chunkIdx] = idBuilder.NewArray()
		scoreChunks[chunkIdx] = scoreBuilder.NewArray()
		offsetChunks[chunkIdx] = offsetBuilder.NewArray()
		idBuilder.Release()
		scoreBuilder.Release()
		offsetBuilder.Release()
	}

	require.NoError(t, builder.AddColumnFromChunks(types.IDFieldName, idChunks))
	require.NoError(t, builder.AddColumnFromChunks(types.ScoreFieldName, scoreChunks))
	require.NoError(t, builder.AddColumnFromChunks(types.SegOffsetFieldName, offsetChunks))
	return builder.Build()
}

func addBoostScorePruningColumns(t *testing.T, df *chain.DataFrame) (*chain.DataFrame, string) {
	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes(df.ChunkSizes())
	require.NoError(t, builder.AddColumnFrom(df, types.IDFieldName))
	require.NoError(t, builder.AddColumnFrom(df, types.ScoreFieldName))
	require.NoError(t, builder.AddColumnFrom(df, types.SegOffsetFieldName))

	elementIndicesBuilder := array.NewInt32Builder(defaultAllocator)
	elementIndicesBuilder.AppendValues([]int32{0, 1, 2}, nil)
	elementIndicesArr := elementIndicesBuilder.NewArray()
	elementIndicesBuilder.Release()
	require.NoError(t, builder.AddColumnFromChunks(elementIndicesCol, []arrow.Array{elementIndicesArr}))

	groupByCol := groupByColumnName(100)
	groupByBuilder := array.NewInt64Builder(defaultAllocator)
	groupByBuilder.AppendValues([]int64{1000, 2000, 3000}, nil)
	groupByArr := groupByBuilder.NewArray()
	groupByBuilder.Release()
	require.NoError(t, builder.AddColumnFromChunks(groupByCol, []arrow.Array{groupByArr}))

	userFieldBuilder := array.NewInt64Builder(defaultAllocator)
	userFieldBuilder.AppendValues([]int64{7, 8, 9}, nil)
	userFieldArr := userFieldBuilder.NewArray()
	userFieldBuilder.Release()
	require.NoError(t, builder.AddColumnFromChunks("user_field", []arrow.Array{userFieldArr}))

	df.Release()
	return builder.Build(), groupByCol
}

func makeBoostScoreTestTask(t *testing.T, plan *planpb.PlanNode) *SearchTask {
	if plan.GetVectorAnns().GetQueryInfo().GetMetricType() == "" {
		plan.Node = &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				QueryInfo: &planpb.QueryInfo{MetricType: metric.COSINE},
			},
		}
	}

	blob, err := proto.Marshal(plan)
	require.NoError(t, err)
	return &SearchTask{
		ctx: context.Background(),
		req: &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: blob,
			},
		},
	}
}

type boostScoreOutput struct {
	scores   []float32
	hasScore []bool
}

func mockBoostScoreRunnerFactory(outputs ...boostScoreOutput) func(boostScoreFunc, segments.Segment, *segcore.SearchRequest, *planpb.ScoreFunction) expr.BoostScoreRunner {
	call := 0
	return func(boostScoreFunc, segments.Segment, *segcore.SearchRequest, *planpb.ScoreFunction) expr.BoostScoreRunner {
		idx := call
		call++
		return func(ctx context.Context, offsets *arrow.Chunked) (*arrow.Chunked, error) {
			chunks := make([]arrow.Array, 0, len(offsets.Chunks()))
			pos := 0
			for _, offsetChunk := range offsets.Chunks() {
				builder := array.NewFloat32Builder(defaultAllocator)
				for rowIdx := 0; rowIdx < offsetChunk.Len(); rowIdx++ {
					if outputs[idx].hasScore[pos] {
						builder.Append(outputs[idx].scores[pos])
					} else {
						builder.AppendNull()
					}
					pos++
				}
				chunk := builder.NewArray()
				builder.Release()
				chunks = append(chunks, chunk)
			}
			return newBoostScoreTestChunked(chunks), nil
		}
	}
}

func newBoostScoreTestChunked(chunks []arrow.Array) *arrow.Chunked {
	result := arrow.NewChunked(arrow.PrimitiveTypes.Float32, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return result
}

func newConstantBoostScoreTestChunked(offsets *arrow.Chunked, score float32) *arrow.Chunked {
	chunks := make([]arrow.Array, 0, len(offsets.Chunks()))
	for _, offsetChunk := range offsets.Chunks() {
		builder := array.NewFloat32Builder(defaultAllocator)
		for rowIdx := 0; rowIdx < offsetChunk.Len(); rowIdx++ {
			builder.Append(score)
		}
		chunk := builder.NewArray()
		builder.Release()
		chunks = append(chunks, chunk)
	}
	return newBoostScoreTestChunked(chunks)
}

func TestBuildBoostScoreChainSkipsFunctionCombineForSingleScorer(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	boostScoreRunnerFactory = mockBoostScoreRunnerFactory(boostScoreOutput{
		scores:   []float32{2.0},
		hasScore: []bool{true},
	})
	defer func() { boostScoreRunnerFactory = oldFactory }()

	df := makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1})
	defer df.Release()

	boostChain, err := buildBoostScoreChain(
		df,
		nil,
		nil,
		[]*planpb.ScoreFunction{{Weight: 1}},
		segments.ComputeScorerScoresOnChunkedOffsets,
		expr.ModeSum,
		expr.ModeMultiply,
	)
	require.NoError(t, err)

	result, err := boostChain.ExecuteWithOptions(context.Background(), chain.ExecuteOptions{EnableColumnPruning: true}, df)
	require.NoError(t, err)
	defer result.Release()

	require.Nil(t, result.Column(boostScoreColumn(0)))
	require.Nil(t, result.Column(functionScoreColumn))
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.InDelta(t, 1.0, scores.Value(0), 1e-6)
}

func TestBuildBoostScoreChainCombinesMultipleScorers(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	boostScoreRunnerFactory = mockBoostScoreRunnerFactory(
		boostScoreOutput{scores: []float32{2.0}, hasScore: []bool{true}},
		boostScoreOutput{scores: []float32{3.0}, hasScore: []bool{true}},
	)
	defer func() { boostScoreRunnerFactory = oldFactory }()

	df := makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1})
	defer df.Release()

	boostChain, err := buildBoostScoreChain(
		df,
		nil,
		nil,
		[]*planpb.ScoreFunction{{Weight: 1}, {Weight: 1}},
		segments.ComputeScorerScoresOnChunkedOffsets,
		expr.ModeSum,
		expr.ModeMultiply,
	)
	require.NoError(t, err)

	result, err := boostChain.ExecuteWithOptions(context.Background(), chain.ExecuteOptions{EnableColumnPruning: true}, df)
	require.NoError(t, err)
	defer result.Release()

	require.Nil(t, result.Column(boostScoreColumn(0)))
	require.Nil(t, result.Column(boostScoreColumn(1)))
	require.Nil(t, result.Column(functionScoreColumn))
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.InDelta(t, 2.5, scores.Value(0), 1e-6)
}

func TestApplyBoostScoresPrunesTempsAndPreservesReduceSystemColumns(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	boostScoreRunnerFactory = mockBoostScoreRunnerFactory(boostScoreOutput{
		scores:   []float32{1.0, 10.0, 2.0},
		hasScore: []bool{true, true, false},
	})
	defer func() { boostScoreRunnerFactory = oldFactory }()

	df := makeBoostScoreTestDF(t,
		[]int64{1, 2, 3},
		[]float32{0.5, 0.2, 0.9},
		[]int64{10, 20, 30},
		[]int64{3},
	)
	df, groupByCol := addBoostScorePruningColumns(t, df)
	segDFs := []*chain.DataFrame{df}

	task := makeBoostScoreTestTask(t, &planpb.PlanNode{
		Scorers: []*planpb.ScoreFunction{{Weight: 1}},
		ScoreOption: &planpb.ScoreOption{
			BoostMode: planpb.BoostMode_BoostModeMultiply,
		},
	})

	require.NoError(t, task.applyBoostScores(segDFs, []segments.Segment{nil}, nil))
	defer segDFs[0].Release()

	result := segDFs[0]
	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.False(t, result.HasColumn(boostScoreColumn(0)))
	require.False(t, result.HasColumn(functionScoreColumn))
	require.False(t, result.HasColumn("user_field"))
	require.True(t, result.HasColumn(types.IDFieldName))
	require.True(t, result.HasColumn(types.ScoreFieldName))
	require.True(t, result.HasColumn(types.SegOffsetFieldName))
	require.True(t, result.HasColumn(elementIndicesCol))
	require.True(t, result.HasColumn(groupByCol))
	require.Equal(t, int64(2), ids.Value(0))
	require.InDelta(t, 2.0, scores.Value(0), 1e-6)
	require.Equal(t, int64(3), ids.Value(1))
	require.InDelta(t, 0.9, scores.Value(1), 1e-6)
	require.Equal(t, int64(1), ids.Value(2))
	require.InDelta(t, 0.5, scores.Value(2), 1e-6)
}

func TestApplyBoostScoresMultipleScorersCombinesScoresAndSorts(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	boostScoreRunnerFactory = mockBoostScoreRunnerFactory(
		boostScoreOutput{scores: []float32{2.0, 0.0, 3.0}, hasScore: []bool{true, false, true}},
		boostScoreOutput{scores: []float32{4.0, 5.0, 0.0}, hasScore: []bool{true, true, false}},
	)
	defer func() { boostScoreRunnerFactory = oldFactory }()

	df := makeBoostScoreTestDF(t,
		[]int64{1, 2, 3},
		[]float32{0.5, 0.2, 0.9},
		[]int64{10, 20, 30},
		[]int64{3},
	)
	segDFs := []*chain.DataFrame{df}

	task := makeBoostScoreTestTask(t, &planpb.PlanNode{
		Scorers: []*planpb.ScoreFunction{{Weight: 1}, {Weight: 2}},
		ScoreOption: &planpb.ScoreOption{
			FunctionMode: planpb.FunctionMode_FunctionModeSum,
			BoostMode:    planpb.BoostMode_BoostModeSum,
		},
	})

	require.NoError(t, task.applyBoostScores(segDFs, []segments.Segment{nil}, nil))
	defer segDFs[0].Release()

	result := segDFs[0]
	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.Nil(t, result.Column(functionScoreColumn))
	require.Equal(t, int64(1), ids.Value(0))
	require.InDelta(t, 6.5, scores.Value(0), 1e-6)
	require.Equal(t, int64(2), ids.Value(1))
	require.InDelta(t, 5.2, scores.Value(1), 1e-6)
	require.Equal(t, int64(3), ids.Value(2))
	require.InDelta(t, 3.9, scores.Value(2), 1e-6)
}

func TestApplyBoostScoresNoScorersNoop(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	df := makeBoostScoreTestDF(t,
		[]int64{1},
		[]float32{0.5},
		[]int64{10},
		[]int64{1},
	)
	defer df.Release()
	segDFs := []*chain.DataFrame{df}
	task := makeBoostScoreTestTask(t, &planpb.PlanNode{})

	require.NoError(t, task.applyBoostScores(segDFs, []segments.Segment{nil}, nil))
	require.Same(t, df, segDFs[0])
}

func TestApplyBoostScoresDistanceMetricKeepsInternalScoreDescending(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	boostScoreRunnerFactory = mockBoostScoreRunnerFactory(boostScoreOutput{
		scores:   []float32{1.0, 1.0, 0.25},
		hasScore: []bool{true, true, true},
	})
	defer func() { boostScoreRunnerFactory = oldFactory }()

	df := makeBoostScoreTestDF(t,
		[]int64{1, 2, 3},
		[]float32{0.0, -0.4, -0.8},
		[]int64{10, 20, 30},
		[]int64{3},
	)
	segDFs := []*chain.DataFrame{df}

	task := makeBoostScoreTestTask(t, &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				QueryInfo: &planpb.QueryInfo{MetricType: metric.L2},
			},
		},
		Scorers: []*planpb.ScoreFunction{{Weight: 1}},
		ScoreOption: &planpb.ScoreOption{
			BoostMode: planpb.BoostMode_BoostModeMultiply,
		},
	})

	require.NoError(t, task.applyBoostScores(segDFs, []segments.Segment{nil}, nil))
	defer segDFs[0].Release()

	result := segDFs[0]
	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.Equal(t, int64(1), ids.Value(0))
	require.InDelta(t, 0.0, scores.Value(0), 1e-6)
	require.Equal(t, int64(3), ids.Value(1))
	require.InDelta(t, -0.2, scores.Value(1), 1e-6)
	require.Equal(t, int64(2), ids.Value(2))
	require.InDelta(t, -0.4, scores.Value(2), 1e-6)
}

func TestApplyBoostScoresDistanceMetricSumKeepsInternalScoreDescending(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	boostScoreRunnerFactory = mockBoostScoreRunnerFactory(boostScoreOutput{
		scores:   []float32{0.0, 0.0, 0.7},
		hasScore: []bool{false, false, true},
	})
	defer func() { boostScoreRunnerFactory = oldFactory }()

	df := makeBoostScoreTestDF(t,
		[]int64{1, 2, 3},
		[]float32{0.0, -0.4, -0.8},
		[]int64{10, 20, 30},
		[]int64{3},
	)
	segDFs := []*chain.DataFrame{df}

	task := makeBoostScoreTestTask(t, &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				QueryInfo: &planpb.QueryInfo{MetricType: metric.L2},
			},
		},
		Scorers: []*planpb.ScoreFunction{{Weight: 1}},
		ScoreOption: &planpb.ScoreOption{
			BoostMode: planpb.BoostMode_BoostModeSum,
		},
	})

	require.NoError(t, task.applyBoostScores(segDFs, []segments.Segment{nil}, nil))
	defer segDFs[0].Release()

	result := segDFs[0]
	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.Equal(t, int64(1), ids.Value(0))
	require.InDelta(t, 0.0, scores.Value(0), 1e-6)
	require.Equal(t, int64(3), ids.Value(1))
	require.InDelta(t, -0.1, scores.Value(1), 1e-6)
	require.Equal(t, int64(2), ids.Value(2))
	require.InDelta(t, -0.4, scores.Value(2), 1e-6)
}

func TestApplyBoostScoresMultipleScorersAllMissKeepOriginalScores(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	boostScoreRunnerFactory = mockBoostScoreRunnerFactory(
		boostScoreOutput{scores: []float32{0.0, 0.0}, hasScore: []bool{false, false}},
		boostScoreOutput{scores: []float32{0.0, 0.0}, hasScore: []bool{false, false}},
	)
	defer func() { boostScoreRunnerFactory = oldFactory }()

	df := makeBoostScoreTestDF(t,
		[]int64{1, 2},
		[]float32{0.3, 0.7},
		[]int64{10, 20},
		[]int64{2},
	)
	segDFs := []*chain.DataFrame{df}

	task := makeBoostScoreTestTask(t, &planpb.PlanNode{
		Scorers: []*planpb.ScoreFunction{{Weight: 1}, {Weight: 2}},
		ScoreOption: &planpb.ScoreOption{
			FunctionMode: planpb.FunctionMode_FunctionModeSum,
			BoostMode:    planpb.BoostMode_BoostModeSum,
		},
	})

	require.NoError(t, task.applyBoostScores(segDFs, []segments.Segment{nil}, nil))
	defer segDFs[0].Release()

	result := segDFs[0]
	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.Nil(t, result.Column(functionScoreColumn))
	require.Equal(t, int64(2), ids.Value(0))
	require.InDelta(t, 0.7, scores.Value(0), 1e-6)
	require.Equal(t, int64(1), ids.Value(1))
	require.InDelta(t, 0.3, scores.Value(1), 1e-6)
}

func TestExtractPlanScorers(t *testing.T) {
	t.Run("empty plan", func(t *testing.T) {
		scorers, err := extractPlanScorers(nil)
		require.NoError(t, err)
		require.Empty(t, scorers)
	})

	t.Run("no scorers", func(t *testing.T) {
		blob, err := proto.Marshal(&planpb.PlanNode{})
		require.NoError(t, err)

		scorers, err := extractPlanScorers(blob)
		require.NoError(t, err)
		require.Empty(t, scorers)
	})

	t.Run("one scorer", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Scorers: []*planpb.ScoreFunction{{Weight: 2.5}},
		}
		blob, err := proto.Marshal(plan)
		require.NoError(t, err)

		scorers, err := extractPlanScorers(blob)
		require.NoError(t, err)
		require.Len(t, scorers, 1)
		require.Equal(t, float32(2.5), scorers[0].GetWeight())
	})

	t.Run("multiple scorers", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Scorers: []*planpb.ScoreFunction{{Weight: 1}, {Weight: 3}},
		}
		blob, err := proto.Marshal(plan)
		require.NoError(t, err)

		scorers, err := extractPlanScorers(blob)
		require.NoError(t, err)
		require.Len(t, scorers, 2)
		require.Equal(t, float32(1), scorers[0].GetWeight())
		require.Equal(t, float32(3), scorers[1].GetWeight())
	})

	t.Run("invalid plan", func(t *testing.T) {
		_, err := extractPlanScorers([]byte{0xff, 0x01})
		require.Error(t, err)
	})
}

func TestApplyBoostScoresUsesAsyncBoostScoreFunc(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	defer func() { boostScoreRunnerFactory = oldFactory }()

	boostScoreRunnerFactory = func(scoreFunc boostScoreFunc, segment segments.Segment, searchReq *segcore.SearchRequest, scorer *planpb.ScoreFunction) expr.BoostScoreRunner {
		require.Equal(t,
			reflect.ValueOf(segments.AsyncComputeScorerScoresOnChunkedOffsets).Pointer(),
			reflect.ValueOf(scoreFunc).Pointer(),
		)
		return func(ctx context.Context, offsets *arrow.Chunked) (*arrow.Chunked, error) {
			return newConstantBoostScoreTestChunked(offsets, 1.0), nil
		}
	}

	df := makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1})
	segDFs := []*chain.DataFrame{df}

	task := makeBoostScoreTestTask(t, &planpb.PlanNode{
		Scorers: []*planpb.ScoreFunction{{Weight: 1}},
		ScoreOption: &planpb.ScoreOption{
			BoostMode: planpb.BoostMode_BoostModeMultiply,
		},
	})

	require.NoError(t, task.applyBoostScores(segDFs, []segments.Segment{nil}, nil))
	defer segDFs[0].Release()
}

func TestApplyBoostScoresRunsSegmentsConcurrently(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	defer func() { boostScoreRunnerFactory = oldFactory }()

	var started atomic.Int32
	var releaseBoth sync.Once
	bothStarted := make(chan struct{})
	boostScoreRunnerFactory = func(boostScoreFunc, segments.Segment, *segcore.SearchRequest, *planpb.ScoreFunction) expr.BoostScoreRunner {
		return func(ctx context.Context, offsets *arrow.Chunked) (*arrow.Chunked, error) {
			if started.Add(1) == 2 {
				releaseBoth.Do(func() { close(bothStarted) })
			}

			select {
			case <-bothStarted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}

			return newConstantBoostScoreTestChunked(offsets, 1.0), nil
		}
	}

	df1 := makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1})
	df2 := makeBoostScoreTestDF(t, []int64{2}, []float32{0.7}, []int64{20}, []int64{1})
	segDFs := []*chain.DataFrame{df1, df2}

	task := makeBoostScoreTestTask(t, &planpb.PlanNode{
		Scorers: []*planpb.ScoreFunction{{Weight: 1}},
		ScoreOption: &planpb.ScoreOption{
			BoostMode: planpb.BoostMode_BoostModeMultiply,
		},
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task.ctx = ctx

	require.NoError(t, task.applyBoostScores(segDFs, []segments.Segment{nil, nil}, nil))
	defer segDFs[0].Release()
	defer segDFs[1].Release()
	require.Equal(t, int32(2), started.Load())
}

func TestApplyBoostScoresReleasesBoostedFramesOnError(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	oldFactory := boostScoreRunnerFactory
	defer func() { boostScoreRunnerFactory = oldFactory }()

	var calls atomic.Int32
	boostScoreRunnerFactory = func(boostScoreFunc, segments.Segment, *segcore.SearchRequest, *planpb.ScoreFunction) expr.BoostScoreRunner {
		return func(ctx context.Context, offsets *arrow.Chunked) (*arrow.Chunked, error) {
			if calls.Add(1) == 1 {
				return newConstantBoostScoreTestChunked(offsets, 1.0), nil
			}
			return nil, errors.New("mock boost failure")
		}
	}

	df1 := makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1})
	df2 := makeBoostScoreTestDF(t, []int64{2}, []float32{0.7}, []int64{20}, []int64{1})
	segDFs := []*chain.DataFrame{df1, df2}
	defer df1.Release()
	defer df2.Release()

	task := makeBoostScoreTestTask(t, &planpb.PlanNode{
		Scorers: []*planpb.ScoreFunction{{Weight: 1}},
		ScoreOption: &planpb.ScoreOption{
			BoostMode: planpb.BoostMode_BoostModeMultiply,
		},
	})

	require.Error(t, task.applyBoostScores(segDFs, []segments.Segment{nil, nil}, nil))
}

func TestBoostScoreModeConversions(t *testing.T) {
	functionMode, err := functionModeToScoreCombineMode(planpb.FunctionMode_FunctionModeMultiply)
	require.NoError(t, err)
	require.Equal(t, expr.ModeMultiply, functionMode)

	functionMode, err = functionModeToScoreCombineMode(planpb.FunctionMode_FunctionModeSum)
	require.NoError(t, err)
	require.Equal(t, expr.ModeSum, functionMode)

	_, err = functionModeToScoreCombineMode(planpb.FunctionMode(999))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown function mode")

	boostMode, err := boostModeToScoreCombineMode(planpb.BoostMode_BoostModeMultiply)
	require.NoError(t, err)
	require.Equal(t, expr.ModeMultiply, boostMode)

	boostMode, err = boostModeToScoreCombineMode(planpb.BoostMode_BoostModeSum)
	require.NoError(t, err)
	require.Equal(t, expr.ModeSum, boostMode)

	_, err = boostModeToScoreCombineMode(planpb.BoostMode(999))
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown boost mode")
}

func TestApplyBoostScoresValidationErrors(t *testing.T) {
	t.Run("segment count mismatch", func(t *testing.T) {
		task := &SearchTask{}
		err := task.applyBoostScores(nil, []segments.Segment{nil}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "does not match segment count")
	})

	t.Run("invalid serialized plan", func(t *testing.T) {
		task := &SearchTask{
			req: &querypb.SearchRequest{
				Req: &internalpb.SearchRequest{SerializedExprPlan: []byte{0xff, 0x01}},
			},
		}
		err := task.applyBoostScores(nil, nil, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse search plan scorers")
	})

	t.Run("invalid function mode", func(t *testing.T) {
		withBoostScoreCheckedAllocator(t)
		df := makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1})
		defer df.Release()

		task := makeBoostScoreTestTask(t, &planpb.PlanNode{
			Scorers: []*planpb.ScoreFunction{{Weight: 1}},
			ScoreOption: &planpb.ScoreOption{
				FunctionMode: planpb.FunctionMode(999),
				BoostMode:    planpb.BoostMode_BoostModeMultiply,
			},
		})
		err := task.applyBoostScores([]*chain.DataFrame{df}, []segments.Segment{nil}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown function mode")
	})

	t.Run("invalid boost mode", func(t *testing.T) {
		withBoostScoreCheckedAllocator(t)
		df := makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1})
		defer df.Release()

		task := makeBoostScoreTestTask(t, &planpb.PlanNode{
			Scorers: []*planpb.ScoreFunction{{Weight: 1}},
			ScoreOption: &planpb.ScoreOption{
				FunctionMode: planpb.FunctionMode_FunctionModeSum,
				BoostMode:    planpb.BoostMode(999),
			},
		})
		err := task.applyBoostScores([]*chain.DataFrame{df}, []segments.Segment{nil}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown boost mode")
	})

	t.Run("nil dataframe", func(t *testing.T) {
		task := makeBoostScoreTestTask(t, &planpb.PlanNode{
			Scorers: []*planpb.ScoreFunction{{Weight: 1}},
			ScoreOption: &planpb.ScoreOption{
				BoostMode: planpb.BoostMode_BoostModeMultiply,
			},
		})
		err := task.applyBoostScores([]*chain.DataFrame{nil}, []segments.Segment{nil}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "DataFrame 0 is nil")
	})
}

func TestBuildBoostScoreChainPropagatesBuilderErrors(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	df := makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1})
	defer df.Release()

	oldFactory := boostScoreRunnerFactory
	boostScoreRunnerFactory = func(boostScoreFunc, segments.Segment, *segcore.SearchRequest, *planpb.ScoreFunction) expr.BoostScoreRunner {
		return nil
	}
	defer func() { boostScoreRunnerFactory = oldFactory }()

	_, err := buildBoostScoreChain(
		df,
		nil,
		nil,
		[]*planpb.ScoreFunction{{Weight: 1}},
		segments.ComputeScorerScoresOnChunkedOffsets,
		expr.ModeSum,
		expr.ModeMultiply,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "runner is nil")

	chainWithBadFunctionMode := chain.NewFuncChainWithAllocator(defaultAllocator)
	_, err = appendFunctionScoreColumn(chainWithBadFunctionMode, []string{boostScoreColumn(0), boostScoreColumn(1)}, "bad-mode")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid mode")

	chainWithBadBoostMode := chain.NewFuncChainWithAllocator(defaultAllocator)
	err = appendFinalBoostScore(chainWithBadBoostMode, boostScoreColumn(0), "bad-mode")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid mode")
}
