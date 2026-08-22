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
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chainexpr "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestApplyL1RerankSortLimitRealignsSources(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	oldAllocator := defaultAllocator
	defaultAllocator = pool
	defer func() {
		defaultAllocator = oldAllocator
		pool.AssertSize(t, 0)
	}()

	seg0 := makeL1SegmentDF(t, pool, []int64{1, 3}, []float32{0.9, 0.7}, []int64{10, 30})
	seg1 := makeL1SegmentDF(t, pool, []int64{2, 4}, []float32{0.8, 0.6}, []int64{20, 40})
	defer seg0.Release()
	defer seg1.Release()

	reduced, err := heapMergeReduce(pool, []*chain.DataFrame{seg0, seg1}, 4, nil)
	require.NoError(t, err)
	defer reduced.DF.Release()

	repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(
		&schemapb.FunctionChainOp{
			Op:     chaintypes.OpTypeSort,
			Inputs: []string{"ts"},
			Params: map[string]*schemapb.FunctionParamValue{
				"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
			},
		},
		&schemapb.FunctionChainOp{
			Op: chaintypes.OpTypeLimit,
			Params: map[string]*schemapb.FunctionParamValue{
				"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 2}},
			},
		},
	))
	require.NoError(t, err)
	mockL1FieldReader(t, pool, []int32{0, 1, 0, 1}, []int64{10, 20, 30, 40}, []int64{1, 50, 100, 2})

	task := &SearchTask{ctx: t.Context()}
	reranked, err := task.applyL1Rerank(reduced, []*segments.SearchResult{{}, {}}, &segcore.SearchPlan{}, &preparedL1FunctionChain{chain: repr, inputFieldIDs: []int64{101}})
	require.NoError(t, err)
	defer reranked.DF.Release()

	ids := reranked.DF.Column(chaintypes.IDFieldName).Chunk(0).(*array.Int64)
	scores := reranked.DF.Column(chaintypes.ScoreFieldName).Chunk(0).(*array.Float32)
	assert.Equal(t, []int64{2, 3}, []int64{ids.Value(0), ids.Value(1)})
	assert.InDelta(t, 0.8, scores.Value(0), 1e-6)
	assert.InDelta(t, 0.7, scores.Value(1), 1e-6)
	assert.Equal(t, []segmentSource{
		{InputIdx: 1, SegOffset: 20, OriginalIdx: 0},
		{InputIdx: 0, SegOffset: 30, OriginalIdx: 1},
	}, reranked.Sources[0])
	assert.False(t, reranked.DF.HasColumn("ts"))
	assert.False(t, reranked.DF.HasColumn(l1SourceIndexColumn))
}

func TestApplyL1RerankSortLimitWithOffsetRealignsSources(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	oldAllocator := defaultAllocator
	defaultAllocator = pool
	defer func() {
		defaultAllocator = oldAllocator
		pool.AssertSize(t, 0)
	}()

	makeInt64Chunks := func(values ...[]int64) []arrow.Array {
		chunks := make([]arrow.Array, len(values))
		for i, values := range values {
			builder := array.NewInt64Builder(pool)
			builder.AppendValues(values, nil)
			chunks[i] = builder.NewArray()
			builder.Release()
		}
		return chunks
	}
	makeFloat32Chunks := func(values ...[]float32) []arrow.Array {
		chunks := make([]arrow.Array, len(values))
		for i, values := range values {
			builder := array.NewFloat32Builder(pool)
			builder.AppendValues(values, nil)
			chunks[i] = builder.NewArray()
			builder.Release()
		}
		return chunks
	}

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{4, 4})
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.IDFieldName, makeInt64Chunks(
		[]int64{11, 12, 13, 14},
		[]int64{21, 22, 23, 24},
	)))
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.ScoreFieldName, makeFloat32Chunks(
		[]float32{0, 2, 1, 9},
		[]float32{0, 2, 1, 9},
	)))
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.SegOffsetFieldName, makeInt64Chunks(
		[]int64{110, 120, 130, 140},
		[]int64{210, 220, 230, 240},
	)))
	reduced := &mergeResult{
		DF: builder.Build(),
		Sources: [][]segmentSource{
			{
				{InputIdx: 0, SegOffset: 110, OriginalIdx: 0},
				{InputIdx: 0, SegOffset: 120, OriginalIdx: 1},
				{InputIdx: 1, SegOffset: 130, OriginalIdx: 2},
				{InputIdx: 1, SegOffset: 140, OriginalIdx: 3},
			},
			{
				{InputIdx: 2, SegOffset: 210, OriginalIdx: 0},
				{InputIdx: 2, SegOffset: 220, OriginalIdx: 1},
				{InputIdx: 3, SegOffset: 230, OriginalIdx: 2},
				{InputIdx: 3, SegOffset: 240, OriginalIdx: 3},
			},
		},
	}
	defer reduced.DF.Release()

	repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(
		mapOpWithParamsForTest(
			"temporary_score",
			chainexpr.NumCombineFuncName,
			map[string]*schemapb.FunctionParamValue{
				chaintypes.NumCombineParamMode: stringParamForTest(chaintypes.NumCombineModeSum),
			},
			columnArgForTest(chaintypes.ScoreFieldName),
			columnArgForTest("ts"),
		),
		mapOpWithParamsForTest(
			chaintypes.ScoreFieldName,
			chainexpr.NumCombineFuncName,
			map[string]*schemapb.FunctionParamValue{
				chaintypes.NumCombineParamMode: stringParamForTest(chaintypes.NumCombineModeSum),
			},
			columnArgForTest("temporary_score"),
			columnArgForTest("ts"),
		),
		&schemapb.FunctionChainOp{
			Op:     chaintypes.OpTypeSort,
			Inputs: []string{"temporary_score", chaintypes.IDFieldName},
			Params: map[string]*schemapb.FunctionParamValue{
				"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
			},
		},
		&schemapb.FunctionChainOp{
			Op: chaintypes.OpTypeLimit,
			Params: map[string]*schemapb.FunctionParamValue{
				"limit":  {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 2}},
				"offset": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 1}},
			},
		},
	))
	require.NoError(t, err)
	mockL1FieldReader(t, pool,
		[]int32{0, 0, 1, 1, 2, 2, 3, 3},
		[]int64{110, 120, 130, 140, 210, 220, 230, 240},
		[]int64{10, 9, 9, 10, 9, 10, 10, 9},
	)

	task := &SearchTask{ctx: t.Context()}
	reranked, err := task.applyL1Rerank(reduced, []*segments.SearchResult{{}, {}, {}, {}}, &segcore.SearchPlan{}, &preparedL1FunctionChain{
		chain:         repr,
		inputFieldIDs: []int64{101},
	})
	require.NoError(t, err)
	defer reranked.DF.Release()

	assert.Equal(t, []int64{2, 2}, reranked.DF.ChunkSizes())
	assertL1ChunkInt64Values(t, reranked.DF, chaintypes.IDFieldName, [][]int64{{11, 12}, {22, 23}})
	assertL1ChunkFloat32Values(t, reranked.DF, chaintypes.ScoreFieldName, [][]float32{{20, 20}, {22, 21}})
	assert.Equal(t, [][]segmentSource{
		{
			{InputIdx: 0, SegOffset: 110, OriginalIdx: 0},
			{InputIdx: 0, SegOffset: 120, OriginalIdx: 1},
		},
		{
			{InputIdx: 2, SegOffset: 220, OriginalIdx: 1},
			{InputIdx: 3, SegOffset: 230, OriginalIdx: 2},
		},
	}, reranked.Sources)
	for _, name := range []string{"ts", "temporary_score", l1SourceIndexColumn} {
		assert.False(t, reranked.DF.HasColumn(name), "column %q must be pruned", name)
	}
}

func TestApplyL1RerankReadsFieldsFromReducedSources(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	oldAllocator := defaultAllocator
	defaultAllocator = pool
	defer func() {
		defaultAllocator = oldAllocator
		pool.AssertSize(t, 0)
	}()

	idBuilder := array.NewInt64Builder(pool)
	idBuilder.Append(2)
	scoreBuilder := array.NewFloat32Builder(pool)
	scoreBuilder.Append(0.8)
	offsetBuilder := array.NewInt64Builder(pool)
	offsetBuilder.Append(20)
	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.SegOffsetFieldName, []arrow.Array{offsetBuilder.NewArray()}))
	idBuilder.Release()
	scoreBuilder.Release()
	offsetBuilder.Release()
	reduced := &mergeResult{
		DF: builder.Build(),
		Sources: [][]segmentSource{{
			{InputIdx: 0, SegOffset: 20, OriginalIdx: 999},
		}},
	}
	defer reduced.DF.Release()

	repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(mapOpWithParamsForTest(
		chaintypes.ScoreFieldName,
		chainexpr.NumCombineFuncName,
		map[string]*schemapb.FunctionParamValue{chaintypes.NumCombineParamMode: stringParamForTest(chaintypes.NumCombineModeSum)},
		columnArgForTest(chaintypes.ScoreFieldName),
		columnArgForTest("ts"),
	)))
	require.NoError(t, err)
	mockL1FieldReader(t, pool, []int32{0}, []int64{20}, []int64{5})

	task := &SearchTask{ctx: t.Context()}
	reranked, err := task.applyL1Rerank(reduced, []*segments.SearchResult{{}}, &segcore.SearchPlan{}, &preparedL1FunctionChain{chain: repr, inputFieldIDs: []int64{101}})
	require.NoError(t, err)
	defer reranked.DF.Release()
	scores := reranked.DF.Column(chaintypes.ScoreFieldName).Chunk(0).(*array.Float32)
	assert.InDelta(t, 5.8, scores.Value(0), 1e-6)
}

func TestApplyL1RerankMapNormalizesScoreAndPreservesSource(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	oldAllocator := defaultAllocator
	defaultAllocator = pool
	defer func() {
		defaultAllocator = oldAllocator
		pool.AssertSize(t, 0)
	}()

	seg := makeL1SegmentDF(t, pool, []int64{1, 2}, []float32{0.9, 0.8}, []int64{10, 20})
	defer seg.Release()
	reduced, err := heapMergeReduce(pool, []*chain.DataFrame{seg}, 2, nil)
	require.NoError(t, err)
	defer reduced.DF.Release()

	repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(mapOpWithParamsForTest(
		chaintypes.ScoreFieldName,
		chainexpr.NumCombineFuncName,
		map[string]*schemapb.FunctionParamValue{chaintypes.NumCombineParamMode: stringParamForTest(chaintypes.NumCombineModeSum)},
		columnArgForTest(chaintypes.ScoreFieldName),
		columnArgForTest("ts"),
	)))
	require.NoError(t, err)

	mockL1FieldReader(t, pool, []int32{0, 0}, []int64{10, 20}, []int64{1, 5})
	task := &SearchTask{ctx: t.Context()}
	reranked, err := task.applyL1Rerank(reduced, []*segments.SearchResult{{}}, &segcore.SearchPlan{}, &preparedL1FunctionChain{chain: repr, inputFieldIDs: []int64{101}})
	require.NoError(t, err)
	defer reranked.DF.Release()

	ids := reranked.DF.Column(chaintypes.IDFieldName).Chunk(0).(*array.Int64)
	assert.Equal(t, int64(2), ids.Value(0))
	assert.Equal(t, int64(1), ids.Value(1))
	assert.Equal(t, []segmentSource{
		{InputIdx: 0, SegOffset: 20, OriginalIdx: 1},
		{InputIdx: 0, SegOffset: 10, OriginalIdx: 0},
	}, reranked.Sources[0])
}

func TestApplyL1RerankPreservesContextErrors(t *testing.T) {
	repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(&schemapb.FunctionChainOp{
		Op:     chaintypes.OpTypeSort,
		Inputs: []string{chaintypes.ScoreFieldName},
	}))
	require.NoError(t, err)

	tests := []struct {
		name     string
		newCtx   func() (context.Context, context.CancelFunc)
		wantErr  error
		wantCode int32
	}{
		{
			name: "canceled",
			newCtx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, func() {}
			},
			wantErr:  context.Canceled,
			wantCode: merr.CanceledCode,
		},
		{
			name: "deadline exceeded",
			newCtx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			wantErr:  context.DeadlineExceeded,
			wantCode: merr.TimeoutCode,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
			oldAllocator := defaultAllocator
			defaultAllocator = pool
			defer func() {
				defaultAllocator = oldAllocator
				pool.AssertSize(t, 0)
			}()

			seg := makeL1SegmentDF(t, pool, []int64{1}, []float32{0.9}, []int64{10})
			defer seg.Release()
			reduced, err := heapMergeReduce(pool, []*chain.DataFrame{seg}, 1, nil)
			require.NoError(t, err)
			defer reduced.DF.Release()

			ctx, cancel := test.newCtx()
			defer cancel()
			task := &SearchTask{ctx: ctx}
			_, err = task.applyL1Rerank(reduced, nil, &segcore.SearchPlan{}, &preparedL1FunctionChain{chain: repr})
			require.ErrorIs(t, err, test.wantErr)
			assert.Equal(t, test.wantCode, merr.Code(err))
		})
	}
}

func TestApplyL1RerankKeepsProvenanceChunkScoped(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	oldAllocator := defaultAllocator
	defaultAllocator = pool
	defer func() {
		defaultAllocator = oldAllocator
		pool.AssertSize(t, 0)
	}()

	makeInt64Chunks := func(values ...[]int64) []arrow.Array {
		chunks := make([]arrow.Array, len(values))
		for i, chunk := range values {
			builder := array.NewInt64Builder(pool)
			builder.AppendValues(chunk, nil)
			chunks[i] = builder.NewArray()
			builder.Release()
		}
		return chunks
	}
	makeFloat32Chunks := func(values ...[]float32) []arrow.Array {
		chunks := make([]arrow.Array, len(values))
		for i, chunk := range values {
			builder := array.NewFloat32Builder(pool)
			builder.AppendValues(chunk, nil)
			chunks[i] = builder.NewArray()
			builder.Release()
		}
		return chunks
	}

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{2, 2})
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.IDFieldName, makeInt64Chunks([]int64{1, 2}, []int64{3, 4})))
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.ScoreFieldName, makeFloat32Chunks([]float32{0.9, 0.1}, []float32{0.8, 0.2})))
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.SegOffsetFieldName, makeInt64Chunks([]int64{10, 20}, []int64{30, 40})))

	sourceA1 := segmentSource{InputIdx: 0, SegOffset: 20, OriginalIdx: 1}
	sourceB1 := segmentSource{InputIdx: 1, SegOffset: 40, OriginalIdx: 1}
	reduced := &mergeResult{
		DF: builder.Build(),
		Sources: [][]segmentSource{
			{{InputIdx: 0, SegOffset: 10, OriginalIdx: 0}, sourceA1},
			{{InputIdx: 1, SegOffset: 30, OriginalIdx: 0}, sourceB1},
		},
	}
	defer reduced.DF.Release()

	repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(
		&schemapb.FunctionChainOp{
			Op:     chaintypes.OpTypeSort,
			Inputs: []string{chaintypes.ScoreFieldName},
			Params: map[string]*schemapb.FunctionParamValue{
				"desc": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: false}},
			},
		},
		&schemapb.FunctionChainOp{
			Op: chaintypes.OpTypeLimit,
			Params: map[string]*schemapb.FunctionParamValue{
				"limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 1}},
			},
		},
	))
	require.NoError(t, err)

	task := &SearchTask{ctx: t.Context()}
	reranked, err := task.applyL1Rerank(reduced, nil, &segcore.SearchPlan{}, &preparedL1FunctionChain{chain: repr})
	require.NoError(t, err)
	defer reranked.DF.Release()

	assert.Equal(t, []int64{1, 1}, reranked.DF.ChunkSizes())
	assert.Equal(t, int64(2), reranked.DF.Column(chaintypes.IDFieldName).Chunk(0).(*array.Int64).Value(0))
	assert.Equal(t, int64(4), reranked.DF.Column(chaintypes.IDFieldName).Chunk(1).(*array.Int64).Value(0))
	assert.Equal(t, [][]segmentSource{{sourceA1}, {sourceB1}}, reranked.Sources)
	assert.False(t, reranked.DF.HasColumn(l1SourceIndexColumn))
}

func TestApplyL1RerankMetrics(t *testing.T) {
	withBoostScoreCheckedAllocator(t)

	repr, err := chain.ProtoChainToRepr(l1FunctionChainForTest(&schemapb.FunctionChainOp{
		Op:     chaintypes.OpTypeSort,
		Inputs: []string{chaintypes.ScoreFieldName},
	}))
	require.NoError(t, err)

	task := &SearchTask{ctx: t.Context()}
	nodeID := fmt.Sprint(task.GetNodeID())
	successObserver := metrics.QueryNodeFunctionChainLatency.WithLabelValues(
		nodeID,
		metrics.FunctionChainLevelL1,
		metrics.SuccessLabel,
	)
	failObserver := metrics.QueryNodeFunctionChainLatency.WithLabelValues(
		nodeID,
		metrics.FunctionChainLevelL1,
		metrics.FailLabel,
	)

	t.Run("success", func(t *testing.T) {
		before := histogramSampleCount(t, successObserver)
		seg := makeL1SegmentDF(t, defaultAllocator, []int64{1}, []float32{0.5}, []int64{10})
		defer seg.Release()
		reduced, err := heapMergeReduce(defaultAllocator, []*chain.DataFrame{seg}, 1, nil)
		require.NoError(t, err)
		defer reduced.DF.Release()

		reranked, err := task.applyL1Rerank(reduced, nil, &segcore.SearchPlan{}, &preparedL1FunctionChain{chain: repr})
		require.NoError(t, err)
		defer reranked.DF.Release()
		require.Equal(t, before+1, histogramSampleCount(t, successObserver))
	})

	t.Run("failure", func(t *testing.T) {
		before := histogramSampleCount(t, failObserver)
		_, err := task.applyL1Rerank(nil, nil, nil, &preparedL1FunctionChain{chain: repr})
		require.Error(t, err)
		require.Equal(t, before+1, histogramSampleCount(t, failObserver))
	})
}

func TestL1InternalContracts(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	newReduced := func(chunkSizes []int64, names []string) *mergeResult {
		builder := chain.NewDataFrameBuilder()
		builder.SetChunkSizes(chunkSizes)
		for _, name := range names {
			chunks := make([]arrow.Array, len(chunkSizes))
			for i, size := range chunkSizes {
				valueBuilder := array.NewInt64Builder(pool)
				for range size {
					valueBuilder.Append(0)
				}
				chunks[i] = valueBuilder.NewArray()
				valueBuilder.Release()
			}
			require.NoError(t, builder.AddColumnFromChunks(name, chunks))
		}
		return &mergeResult{DF: builder.Build()}
	}
	assertSystemInternal := func(t *testing.T, err error, message string) {
		t.Helper()
		require.Error(t, err)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
		assert.Contains(t, err.Error(), message)
	}

	t.Run("validate merge result", func(t *testing.T) {
		tests := []struct {
			name    string
			reduced *mergeResult
			message string
		}{
			{
				name:    "chunk source mismatch",
				reduced: newReduced([]int64{1, 1}, []string{chaintypes.IDFieldName}),
				message: "DataFrame chunks 2 does not match source chunks 1",
			},
			{
				name: "row source mismatch",
				reduced: &mergeResult{
					DF:      newReduced([]int64{2}, []string{chaintypes.IDFieldName}).DF,
					Sources: [][]segmentSource{{{InputIdx: 0}}},
				},
				message: "chunk 0 has 2 rows but 1 sources",
			},
			{
				name:    "preexisting reserved provenance",
				reduced: newReduced([]int64{1}, []string{l1SourceIndexColumn}),
				message: "reserved column",
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				defer test.reduced.DF.Release()
				test.reduced.Sources = [][]segmentSource{{{InputIdx: 0}}}
				assertSystemInternal(t, validateL1MergeResult(test.reduced), test.message)
			})
		}
	})

	t.Run("materialized input fields", func(t *testing.T) {
		makeRecord := func(field arrow.Field) arrow.Record {
			valueBuilder := array.NewInt64Builder(pool)
			valueBuilder.Append(1)
			values := valueBuilder.NewArray()
			valueBuilder.Release()
			record := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{values}, 1)
			values.Release()
			return record
		}
		oldReader := fillL1FieldsOrdered
		defer func() { fillL1FieldsOrdered = oldReader }()

		tests := []struct {
			name          string
			field         arrow.Field
			inputFieldIDs []int64
			message       string
		}{
			{
				name:          "missing field ID metadata",
				field:         arrow.Field{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
				inputFieldIDs: []int64{101},
				message:       "missing field id metadata",
			},
			{
				name: "conflicting materialized field",
				field: arrow.Field{
					Name:     chaintypes.IDFieldName,
					Type:     arrow.PrimitiveTypes.Int64,
					Metadata: arrow.NewMetadata([]string{arrowMetadataFieldIDKey}, []string{"101"}),
				},
				inputFieldIDs: []int64{101},
				message:       "conflicts with reduced dataframe column",
			},
			{
				name: "missing requested ID",
				field: arrow.Field{
					Name:     "ts",
					Type:     arrow.PrimitiveTypes.Int64,
					Metadata: arrow.NewMetadata([]string{arrowMetadataFieldIDKey}, []string{"102"}),
				},
				inputFieldIDs: []int64{101},
				message:       "missing field id 101",
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				fillL1FieldsOrdered = func(context.Context, []*segcore.SearchResult, *segcore.SearchPlan, []int64, []int32, []int64) (arrow.Record, error) {
					return makeRecord(test.field), nil
				}
				reduced := newReduced([]int64{1}, []string{chaintypes.IDFieldName, chaintypes.ScoreFieldName, chaintypes.SegOffsetFieldName})
				reduced.Sources = [][]segmentSource{{{InputIdx: 0, SegOffset: 10}}}
				defer reduced.DF.Release()

				input, err := buildL1InputDataFrame(t.Context(), pool, reduced, nil, nil, test.inputFieldIDs)
				require.Nil(t, input)
				assertSystemInternal(t, err, test.message)
			})
		}
	})

	t.Run("rebuild provenance", func(t *testing.T) {
		tests := []struct {
			name    string
			column  string
			message string
		}{
			{
				name:    "missing provenance column",
				column:  chaintypes.IDFieldName,
				message: "provenance column",
			},
			{
				name:    "source chunk mismatch",
				column:  l1SourceIndexColumn,
				message: "provenance chunks do not match source chunks",
			},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				chunkSizes := []int64{1}
				if test.name == "source chunk mismatch" {
					chunkSizes = []int64{1, 1}
				}
				df := newReduced(chunkSizes, []string{test.column}).DF
				defer df.Release()
				_, err := rebuildL1Sources([][]segmentSource{{{InputIdx: 0}}}, df)
				assertSystemInternal(t, err, test.message)
			})
		}
	})
}

func TestRebuildL1SourcesRejectsInvalidToken(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{1})
	idBuilder := array.NewInt64Builder(pool)
	idBuilder.Append(1)
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
	idBuilder.Release()
	scoreBuilder := array.NewFloat32Builder(pool)
	scoreBuilder.Append(1)
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
	scoreBuilder.Release()
	tokenBuilder := array.NewInt64Builder(pool)
	tokenBuilder.Append(2)
	require.NoError(t, builder.AddColumnFromChunks(l1SourceIndexColumn, []arrow.Array{tokenBuilder.NewArray()}))
	tokenBuilder.Release()
	df := builder.Build()
	defer df.Release()

	_, err := rebuildL1Sources([][]segmentSource{{{InputIdx: 0}}}, df)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "provenance token 2 out of range")
}

func assertL1ChunkInt64Values(t *testing.T, df *chain.DataFrame, column string, want [][]int64) {
	t.Helper()
	values := df.Column(column)
	require.NotNil(t, values)
	require.Len(t, values.Chunks(), len(want))
	for i, chunkWant := range want {
		chunk := values.Chunk(i).(*array.Int64)
		got := make([]int64, chunk.Len())
		for j := range got {
			got[j] = chunk.Value(j)
		}
		assert.Equal(t, chunkWant, got)
	}
}

func assertL1ChunkFloat32Values(t *testing.T, df *chain.DataFrame, column string, want [][]float32) {
	t.Helper()
	values := df.Column(column)
	require.NotNil(t, values)
	require.Len(t, values.Chunks(), len(want))
	for i, chunkWant := range want {
		chunk := values.Chunk(i).(*array.Float32)
		got := make([]float32, chunk.Len())
		for j := range got {
			got[j] = chunk.Value(j)
		}
		assert.Equal(t, chunkWant, got)
	}
}

func makeL1SegmentDF(t *testing.T, pool memory.Allocator, ids []int64, scores []float32, offsets []int64) *chain.DataFrame {
	t.Helper()
	require.Len(t, scores, len(ids))
	require.Len(t, offsets, len(ids))

	idBuilder := array.NewInt64Builder(pool)
	idBuilder.AppendValues(ids, nil)
	scoreBuilder := array.NewFloat32Builder(pool)
	scoreBuilder.AppendValues(scores, nil)
	offsetBuilder := array.NewInt64Builder(pool)
	offsetBuilder.AppendValues(offsets, nil)

	builder := chain.NewDataFrameBuilder()
	builder.SetChunkSizes([]int64{int64(len(ids))})
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.IDFieldName, []arrow.Array{idBuilder.NewArray()}))
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.ScoreFieldName, []arrow.Array{scoreBuilder.NewArray()}))
	require.NoError(t, builder.AddColumnFromChunks(chaintypes.SegOffsetFieldName, []arrow.Array{offsetBuilder.NewArray()}))
	idBuilder.Release()
	scoreBuilder.Release()
	offsetBuilder.Release()
	return builder.Build()
}

func mockL1FieldReader(t *testing.T, pool memory.Allocator, expectedSegIndices []int32, expectedSegOffsets []int64, values []int64) {
	t.Helper()
	oldReader := fillL1FieldsOrdered
	fillL1FieldsOrdered = func(
		ctx context.Context,
		results []*segcore.SearchResult,
		plan *segcore.SearchPlan,
		fieldIDs []int64,
		segIndices []int32,
		segOffsets []int64,
	) (arrow.Record, error) {
		require.NoError(t, ctx.Err())
		require.NotEmpty(t, results)
		require.NotNil(t, plan)
		require.Equal(t, []int64{101}, fieldIDs)
		require.Equal(t, expectedSegIndices, segIndices)
		require.Equal(t, expectedSegOffsets, segOffsets)
		require.Len(t, segIndices, len(values))

		field := arrow.Field{
			Name: "ts",
			Type: arrow.PrimitiveTypes.Int64,
			Metadata: arrow.NewMetadata(
				[]string{arrowMetadataFieldIDKey, arrowMetadataDataTypeKey},
				[]string{"101", "5"},
			),
		}
		valueBuilder := array.NewInt64Builder(pool)
		valueBuilder.AppendValues(values, nil)
		arr := valueBuilder.NewArray()
		valueBuilder.Release()
		record := array.NewRecord(arrow.NewSchema([]arrow.Field{field}, nil), []arrow.Array{arr}, int64(len(values)))
		arr.Release()
		return record, nil
	}
	t.Cleanup(func() {
		fillL1FieldsOrdered = oldReader
	})
}
