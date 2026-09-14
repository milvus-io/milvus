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
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chainexpr "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func histogramSampleCount(t *testing.T, observer prometheus.Observer) uint64 {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, observer.(prometheus.Metric).Write(metric))
	return metric.GetHistogram().GetSampleCount()
}

func TestApplyPublicL0RerankRejectsNilPreparedChain(t *testing.T) {
	task := &SearchTask{ctx: t.Context()}

	err := task.applyPublicL0Rerank(nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "prepared L0 function chain is nil")
}

func TestApplyPublicL0RerankPrunesInputsAndPreservesReduceSystemColumns(t *testing.T) {
	withBoostScoreCheckedAllocator(t)
	pool := withL0RerankMallocator(t)

	df := makeBoostScoreTestDF(t,
		[]int64{1, 2, 3},
		[]float32{0.5, 0.2, 0.9},
		[]int64{10, 20, 30},
		[]int64{3},
	)
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
	tsBuilder := array.NewFloat32Builder(defaultAllocator)
	tsBuilder.AppendValues([]float32{0.1, 3.0, 0.1}, nil)
	tsArr := tsBuilder.NewArray()
	tsBuilder.Release()
	require.NoError(t, builder.AddColumnFromChunks("ts", []arrow.Array{tsArr}))
	df.Release()
	df = builder.Build()
	segDFs := []*chain.DataFrame{df}

	repr, err := chain.ProtoChainToRepr(l0FunctionChainForTest(mapOpWithParamsForTest(
		types.ScoreFieldName,
		chainexpr.NumCombineFuncName,
		map[string]*schemapb.FunctionParamValue{
			types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum),
		},
		columnArgForTest(types.ScoreFieldName),
		columnArgForTest("ts"),
	)))
	require.NoError(t, err)
	task := &SearchTask{ctx: t.Context()}

	require.NoError(t, task.applyPublicL0Rerank(segDFs, &preparedL0Rerank{chain: repr}))

	result := segDFs[0]
	ids := result.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := result.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.False(t, result.HasColumn("ts"))
	require.True(t, result.HasColumn(types.IDFieldName))
	require.True(t, result.HasColumn(types.ScoreFieldName))
	require.True(t, result.HasColumn(types.SegOffsetFieldName))
	require.True(t, result.HasColumn(elementIndicesCol))
	require.True(t, result.HasColumn(groupByCol))
	require.Equal(t, int64(2), ids.Value(0))
	require.InDelta(t, 3.2, scores.Value(0), 1e-6)
	require.Equal(t, int64(3), ids.Value(1))
	require.InDelta(t, 1.0, scores.Value(1), 1e-6)
	require.Equal(t, int64(1), ids.Value(2))
	require.InDelta(t, 0.6, scores.Value(2), 1e-6)

	result.Release()
	require.Zero(t, pool.AllocatedBytes())
}

func TestL0ThenL1RerankPrunesStageLocalColumns(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	df := makeBoostScoreTestDF(t,
		[]int64{1, 2, 3},
		[]float32{0.5, 0.2, 0.9},
		[]int64{10, 20, 30},
		[]int64{3},
	)
	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes(df.ChunkSizes())
	require.NoError(t, builder.AddColumnFrom(df, types.IDFieldName))
	require.NoError(t, builder.AddColumnFrom(df, types.ScoreFieldName))
	require.NoError(t, builder.AddColumnFrom(df, types.SegOffsetFieldName))
	l0BonusBuilder := array.NewFloat32Builder(defaultAllocator)
	l0BonusBuilder.AppendValues([]float32{0.1, 3.0, 0.1}, nil)
	require.NoError(t, builder.AddColumnFromChunks("l0_bonus", []arrow.Array{l0BonusBuilder.NewArray()}))
	l0BonusBuilder.Release()
	unusedBuilder := array.NewInt64Builder(defaultAllocator)
	unusedBuilder.AppendValues([]int64{100, 200, 300}, nil)
	require.NoError(t, builder.AddColumnFromChunks("unused", []arrow.Array{unusedBuilder.NewArray()}))
	unusedBuilder.Release()
	df.Release()
	segDFs := []*chain.DataFrame{builder.Build()}

	l0Repr, err := chain.ProtoChainToRepr(l0FunctionChainForTest(mapOpWithParamsForTest(
		types.ScoreFieldName,
		chainexpr.NumCombineFuncName,
		map[string]*schemapb.FunctionParamValue{
			types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum),
		},
		columnArgForTest(types.ScoreFieldName),
		columnArgForTest("l0_bonus"),
	)))
	require.NoError(t, err)
	task := &SearchTask{ctx: t.Context()}
	require.NoError(t, task.applyPublicL0Rerank(segDFs, &preparedL0Rerank{chain: l0Repr}))
	defer segDFs[0].Release()

	require.False(t, segDFs[0].HasColumn("l0_bonus"))
	require.False(t, segDFs[0].HasColumn("unused"))
	require.True(t, segDFs[0].HasColumn(types.IDFieldName))
	require.True(t, segDFs[0].HasColumn(types.ScoreFieldName))
	require.True(t, segDFs[0].HasColumn(types.SegOffsetFieldName))

	reduced, err := heapMergeReduce(defaultAllocator, segDFs, 3, nil)
	require.NoError(t, err)
	defer reduced.DF.Release()

	l1Repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(
		mapOpWithParamsForTest(
			"$l1_tmp",
			chainexpr.NumCombineFuncName,
			map[string]*schemapb.FunctionParamValue{
				types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum),
			},
			columnArgForTest(types.ScoreFieldName),
			columnArgForTest("ts"),
		),
		mapOpWithParamsForTest(
			types.ScoreFieldName,
			chainexpr.NumCombineFuncName,
			map[string]*schemapb.FunctionParamValue{
				types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum),
			},
			columnArgForTest("$l1_tmp"),
			columnArgForTest(types.ScoreFieldName),
		),
	))
	require.NoError(t, err)
	mockL1FieldReader(t, defaultAllocator, []int32{0, 0, 0}, []int64{20, 30, 10}, []int64{1, 2, 3})

	reranked, err := task.applyL1Rerank(reduced, []*segments.SearchResult{{}}, &segcore.SearchPlan{}, &preparedL1FunctionChain{
		chain:         l1Repr,
		inputFieldIDs: []int64{101},
	})
	require.NoError(t, err)
	defer reranked.DF.Release()

	for _, column := range []string{"l0_bonus", "unused", "ts", "$l1_tmp", l1SourceIndexColumn} {
		require.False(t, reranked.DF.HasColumn(column), column)
	}
	ids := reranked.DF.Column(types.IDFieldName).Chunk(0).(*array.Int64)
	scores := reranked.DF.Column(types.ScoreFieldName).Chunk(0).(*array.Float32)
	require.Equal(t, []int64{2, 1, 3}, []int64{ids.Value(0), ids.Value(1), ids.Value(2)})
	require.InDelta(t, 7.4, scores.Value(0), 1e-6)
	require.InDelta(t, 4.2, scores.Value(1), 1e-6)
	require.InDelta(t, 4.0, scores.Value(2), 1e-6)
}

func TestApplyL0RerankMetrics(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	repr, err := chain.ProtoChainToRepr(l0FunctionChainForTest(mapOpWithParamsForTest(
		types.ScoreFieldName,
		chainexpr.NumCombineFuncName,
		map[string]*schemapb.FunctionParamValue{
			types.NumCombineParamMode: stringParamForTest(types.NumCombineModeSum),
		},
		columnArgForTest(types.ScoreFieldName),
		columnArgForTest(types.IDFieldName),
	)))
	require.NoError(t, err)
	publicPrepared := &preparedL0Rerank{chain: repr}
	boostPrepared := &preparedL0Rerank{
		boostScore: &preparedBoostScore{
			scorers:      []*planpb.ScoreFunction{{Weight: 1}},
			functionMode: chainexpr.ModeSum,
			boostMode:    chainexpr.ModeMultiply,
		},
	}
	task := &SearchTask{ctx: t.Context()}
	nodeID := fmt.Sprint(task.GetNodeID())

	successObserver := metrics.QueryNodeFunctionChainLatency.WithLabelValues(
		nodeID,
		metrics.FunctionChainLevelL0,
		metrics.SuccessLabel,
	)
	failObserver := metrics.QueryNodeFunctionChainLatency.WithLabelValues(
		nodeID,
		metrics.FunctionChainLevelL0,
		metrics.FailLabel,
	)

	t.Run("public L0 success", func(t *testing.T) {
		before := histogramSampleCount(t, successObserver)
		segDFs := []*chain.DataFrame{
			makeBoostScoreTestDF(t, []int64{1, 2}, []float32{0.5, 0.2}, []int64{10, 20}, []int64{2}),
			makeBoostScoreTestDF(t, []int64{3, 4}, []float32{0.9, 0.1}, []int64{30, 40}, []int64{2}),
		}
		defer func() {
			for _, df := range segDFs {
				df.Release()
			}
		}()

		require.NoError(t, task.applyL0Rerank(segDFs, publicPrepared, nil, nil))
		require.Equal(t, before+1, histogramSampleCount(t, successObserver))
	})

	t.Run("public L0 failure", func(t *testing.T) {
		before := histogramSampleCount(t, failObserver)
		err := task.applyL0Rerank([]*chain.DataFrame{nil}, publicPrepared, nil, nil)
		require.Error(t, err)
		require.Equal(t, before+1, histogramSampleCount(t, failObserver))
	})

	t.Run("boost score success", func(t *testing.T) {
		oldFactory := boostScoreRunnerFactory
		boostScoreRunnerFactory = mockBoostScoreRunnerFactory(boostScoreOutput{
			scores:   []float32{2.0},
			hasScore: []bool{true},
		})
		defer func() { boostScoreRunnerFactory = oldFactory }()

		before := histogramSampleCount(t, successObserver)
		segDFs := []*chain.DataFrame{
			makeBoostScoreTestDF(t, []int64{1}, []float32{0.5}, []int64{10}, []int64{1}),
		}
		defer func() {
			for _, df := range segDFs {
				df.Release()
			}
		}()

		require.NoError(t, task.applyL0Rerank(segDFs, boostPrepared, []segments.Segment{nil}, nil))
		require.Equal(t, before+1, histogramSampleCount(t, successObserver))
	})

	t.Run("boost score failure", func(t *testing.T) {
		before := histogramSampleCount(t, failObserver)
		err := task.applyL0Rerank(nil, boostPrepared, []segments.Segment{nil}, nil)
		require.Error(t, err)
		require.Equal(t, before+1, histogramSampleCount(t, failObserver))
	})

	t.Run("no rerank does not record", func(t *testing.T) {
		successBefore := histogramSampleCount(t, successObserver)
		failBefore := histogramSampleCount(t, failObserver)
		require.NoError(t, task.applyL0Rerank(nil, nil, nil, nil))
		require.Equal(t, successBefore, histogramSampleCount(t, successObserver))
		require.Equal(t, failBefore, histogramSampleCount(t, failObserver))
	})
}
