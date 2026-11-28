/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * # or implied. See the License for the specific language governing
 * # permissions and limitations under the License.
 */

package expr

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	boostnative "github.com/milvus-io/milvus/internal/util/function/boost/native"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type BoostScoreExprTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *BoostScoreExprTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *BoostScoreExprTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestBoostScoreExprTestSuite(t *testing.T) {
	suite.Run(t, new(BoostScoreExprTestSuite))
}

func (s *BoostScoreExprTestSuite) createOffsetColumn(chunks ...[]int64) *arrow.Chunked {
	arrays := make([]arrow.Array, 0, len(chunks))
	for _, values := range chunks {
		builder := array.NewInt64Builder(s.pool)
		builder.AppendValues(values, nil)
		arr := builder.NewArray()
		builder.Release()
		arrays = append(arrays, arr)
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Int64, arrays)
	for _, arr := range arrays {
		arr.Release()
	}
	return chunked
}

func (s *BoostScoreExprTestSuite) createScoreColumn(values [][]float32, valid [][]bool) *arrow.Chunked {
	arrays := make([]arrow.Array, 0, len(values))
	for chunkIdx, chunkValues := range values {
		builder := array.NewFloat32Builder(s.pool)
		for rowIdx, value := range chunkValues {
			if valid != nil && !valid[chunkIdx][rowIdx] {
				builder.AppendNull()
			} else {
				builder.Append(value)
			}
		}
		arr := builder.NewArray()
		builder.Release()
		arrays = append(arrays, arr)
	}
	chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float32, arrays)
	for _, arr := range arrays {
		arr.Release()
	}
	return chunked
}

func (s *BoostScoreExprTestSuite) TestFactorySegmentBinding() {
	segment := boostSegmentFunc(func(_ context.Context, _ *arrow.Chunked, _ *planpb.ScoreFunction) (*arrow.Chunked, error) {
		return nil, nil
	})
	expr, err := s.newExpr(segment)
	s.Require().NoError(err)
	s.Equal(BoostScoreFuncName, expr.Name())
	s.Equal([]arrow.DataType{arrow.PrimitiveTypes.Float32}, expr.OutputDataTypes())
	s.True(expr.IsRunnable(types.StageL0Rerank))
	s.False(expr.IsRunnable(types.StageL2Rerank))

	unbound, err := s.newExpr(nil)
	s.Require().NoError(err)
	_, err = unbound.Execute(nil, []*arrow.Chunked{nil})
	s.Error(err)
	s.Contains(err.Error(), "segment binding")
}

func (s *BoostScoreExprTestSuite) TestExecutePassesChunkedOffsets() {
	offsets := s.createOffsetColumn([]int64{10, 20}, []int64{30, 40, 50})
	defer offsets.Release()

	called := false
	expr, err := s.newExpr(boostSegmentFunc(func(ctx context.Context, got *arrow.Chunked, _ *planpb.ScoreFunction) (*arrow.Chunked, error) {
		called = true
		s.Equal(context.TODO(), ctx)
		s.Same(offsets, got)
		return s.createScoreColumn(
			[][]float32{{1.5, 0}, {2.5, 3.5, 0}},
			[][]bool{{true, false}, {true, true, false}},
		), nil
	}))
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.TODO(), s.pool, types.StageL0Rerank)
	outputs, err := expr.Execute(ctx, []*arrow.Chunked{offsets})
	s.Require().NoError(err)
	defer outputs[0].Release()

	s.True(called)
	s.Len(outputs, 1)
	s.Equal(2, len(outputs[0].Chunks()))
	s.Equal(2, outputs[0].Chunk(0).Len())
	s.Equal(3, outputs[0].Chunk(1).Len())

	chunk0 := outputs[0].Chunk(0).(*array.Float32)
	chunk1 := outputs[0].Chunk(1).(*array.Float32)
	s.InDelta(1.5, float64(chunk0.Value(0)), 1e-6)
	s.True(chunk0.IsNull(1))
	s.InDelta(2.5, float64(chunk1.Value(0)), 1e-6)
	s.InDelta(3.5, float64(chunk1.Value(1)), 1e-6)
	s.True(chunk1.IsNull(2))
}

func (s *BoostScoreExprTestSuite) TestExecuteRejectsInvalidInputs() {
	expr, err := s.newExpr(boostSegmentFunc(func(_ context.Context, _ *arrow.Chunked, _ *planpb.ScoreFunction) (*arrow.Chunked, error) {
		return nil, nil
	}))
	s.Require().NoError(err)
	ctx := types.NewFuncContextFull(context.Background(), s.pool, types.StageL0Rerank)

	_, err = expr.Execute(ctx, nil)
	s.Error(err)
	s.Contains(err.Error(), "expected 1 input")

	builder := array.NewInt32Builder(s.pool)
	builder.Append(1)
	arr := builder.NewArray()
	builder.Release()
	wrongType := arrow.NewChunked(arrow.PrimitiveTypes.Int32, []arrow.Array{arr})
	arr.Release()
	defer wrongType.Release()

	_, err = expr.Execute(ctx, []*arrow.Chunked{wrongType})
	s.Error(err)
	s.Contains(err.Error(), "must be Int64")

	_, err = expr.Execute(ctx, []*arrow.Chunked{nil})
	s.Error(err)
	s.Contains(err.Error(), "offset column is nil")

	expr.segment = nil
	_, err = expr.Execute(ctx, []*arrow.Chunked{wrongType})
	s.Error(err)
	s.Contains(err.Error(), "segment binding")
}

func (s *BoostScoreExprTestSuite) TestExecuteSegmentError() {
	offsets := s.createOffsetColumn([]int64{10})
	defer offsets.Release()

	expr, err := s.newExpr(boostSegmentFunc(func(_ context.Context, _ *arrow.Chunked, _ *planpb.ScoreFunction) (*arrow.Chunked, error) {
		return nil, fmt.Errorf("segment failed")
	}))
	s.Require().NoError(err)

	ctx := types.NewFuncContextFull(context.Background(), s.pool, types.StageL0Rerank)
	_, err = expr.Execute(ctx, []*arrow.Chunked{offsets})
	s.Error(err)
	s.Contains(err.Error(), "segment failed")
}

func (s *BoostScoreExprTestSuite) TestExecuteRejectsInvalidSegmentOutput() {
	offsets := s.createOffsetColumn([]int64{10, 20}, []int64{30})
	defer offsets.Release()
	ctx := types.NewFuncContextFull(context.Background(), s.pool, types.StageL0Rerank)

	cases := []struct {
		name    string
		scores  func() *arrow.Chunked
		message string
	}{
		{
			name: "nil scores",
			scores: func() *arrow.Chunked {
				return nil
			},
			message: "nil scores",
		},
		{
			name: "wrong type",
			scores: func() *arrow.Chunked {
				builder := array.NewFloat64Builder(s.pool)
				builder.Append(1)
				arr := builder.NewArray()
				builder.Release()
				chunked := arrow.NewChunked(arrow.PrimitiveTypes.Float64, []arrow.Array{arr})
				arr.Release()
				return chunked
			},
			message: "must be Float32",
		},
		{
			name: "chunk count mismatch",
			scores: func() *arrow.Chunked {
				return s.createScoreColumn([][]float32{{1, 2, 3}}, nil)
			},
			message: "score chunks",
		},
		{
			name: "chunk length mismatch",
			scores: func() *arrow.Chunked {
				return s.createScoreColumn([][]float32{{1}, {2}}, nil)
			},
			message: "length",
		},
	}

	for _, tc := range cases {
		s.Run(tc.name, func() {
			expr, err := s.newExpr(boostSegmentFunc(func(_ context.Context, _ *arrow.Chunked, _ *planpb.ScoreFunction) (*arrow.Chunked, error) {
				return tc.scores(), nil
			}))
			s.Require().NoError(err)
			_, err = expr.Execute(ctx, []*arrow.Chunked{offsets})
			s.Error(err)
			s.Contains(err.Error(), tc.message)
		})
	}
}

// Exercise the same factory and segment contract as production, including its
// validation; test callbacks only implement the native Segment interface.
func (s *BoostScoreExprTestSuite) newExpr(segment boostnative.Segment) (*BoostScoreExpr, error) {
	cfg := types.FunctionConfig{Name: BoostScoreFuncName, Params: map[string]*schemapb.FunctionParamValue{"weight": boostNumber(1)}}
	prepared, err := prepareBoostScoreFromParams(types.FunctionBuildContext{Schema: boostFactorySchema(s.T()), Stage: types.StageL0Rerank}, cfg)
	if err != nil {
		return nil, err
	}
	cfg.Prepared = prepared
	fn, err := NewBoostScoreExprFromParams(types.FunctionBuildContext{
		Stage: types.StageL0Rerank,
		L0:    &types.L0BuildContext{Segment: segment},
	}, cfg)
	if err != nil {
		return nil, err
	}
	return fn.(*BoostScoreExpr), nil
}

func boostFactorySchema(t *testing.T) *typeutil.SchemaHelper {
	s, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "category", DataType: schemapb.DataType_VarChar},
	}})
	require.NoError(t, err)
	return s
}

func boostNumber(v float64) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: v}}
}

func boostString(v string) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_StringValue{StringValue: v}}
}

type boostSegmentFunc func(context.Context, *arrow.Chunked, *planpb.ScoreFunction) (*arrow.Chunked, error)

func (f boostSegmentFunc) ComputeScores(c context.Context, a *arrow.Chunked, r *planpb.ScoreFunction) (*arrow.Chunked, error) {
	return f(c, a, r)
}

func TestBoostScoreFactoryPreparationAndBinding(t *testing.T) {
	cfg := types.FunctionConfig{Name: BoostScoreFuncName, Params: map[string]*schemapb.FunctionParamValue{
		"weight": boostNumber(1.5), "filter": boostString(`category == "vip"`),
	}}
	preparation, err := prepareBoostScoreFromParams(types.FunctionBuildContext{Schema: boostFactorySchema(t), Stage: types.StageL0Rerank}, cfg)
	require.NoError(t, err)
	compiled := preparation.(*planpb.ScoreFunction)
	require.NotNil(t, compiled.Filter)
	require.Equal(t, float32(1.5), compiled.Weight)
	cfg.Prepared = preparation
	unbound, err := types.CreateFunction(types.FunctionBuildContext{}, cfg)
	require.NoError(t, err)
	require.True(t, unbound.IsRunnable(types.StageL0Rerank))
	require.False(t, unbound.IsRunnable(types.StageL1Rerank))
	require.Equal(t, []arrow.DataType{arrow.PrimitiveTypes.Float32}, unbound.OutputDataTypes())
	_, err = unbound.Execute(nil, []*arrow.Chunked{nil})
	require.ErrorContains(t, err, "segment binding")

	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	b := array.NewInt64Builder(pool)
	b.AppendValues([]int64{7, 8}, nil)
	a := b.NewArray()
	b.Release()
	offsets := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{a})
	a.Release()
	defer offsets.Release()
	// Share prepared state between independent segment instances. Native nulls
	// are preserved and no request-level schema recompilation is required.
	for _, value := range []float32{3, 9} {
		fn, err := types.CreateFunction(types.FunctionBuildContext{L0: &types.L0BuildContext{Segment: boostSegmentFunc(func(ctx context.Context, got *arrow.Chunked, req *planpb.ScoreFunction) (*arrow.Chunked, error) {
			require.Same(t, offsets, got)
			require.Same(t, compiled, req)
			b := array.NewFloat32Builder(pool)
			b.Append(value)
			b.AppendNull()
			a := b.NewArray()
			b.Release()
			out := arrow.NewChunked(arrow.PrimitiveTypes.Float32, []arrow.Array{a})
			a.Release()
			return out, nil
		})}}, cfg)
		require.NoError(t, err)
		out, err := fn.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL0Rerank), []*arrow.Chunked{offsets})
		require.NoError(t, err)
		require.Equal(t, value, out[0].Chunk(0).(*array.Float32).Value(0))
		require.True(t, out[0].Chunk(0).IsNull(1))
		out[0].Release()
	}
	fn, err := types.CreateFunction(types.FunctionBuildContext{L0: &types.L0BuildContext{Segment: boostSegmentFunc(func(context.Context, *arrow.Chunked, *planpb.ScoreFunction) (*arrow.Chunked, error) {
		return nil, context.Canceled
	})}}, cfg)
	require.NoError(t, err)
	_, err = fn.Execute(types.NewFuncContext(pool), []*arrow.Chunked{offsets})
	require.ErrorIs(t, err, context.Canceled)
}

func TestBoostScorePublicValidation(t *testing.T) {
	ctx := types.FunctionBuildContext{Schema: boostFactorySchema(t), Stage: types.StageL0Rerank}
	cases := []struct {
		name   string
		params map[string]*schemapb.FunctionParamValue
	}{
		{"missing weight", nil},
		{"wrong weight", map[string]*schemapb.FunctionParamValue{"weight": boostString("2")}},
		{"non finite", map[string]*schemapb.FunctionParamValue{"weight": boostNumber(math.Inf(1))}},
		{"overflow", map[string]*schemapb.FunctionParamValue{"weight": boostNumber(math.MaxFloat64)}},
		{"unknown", map[string]*schemapb.FunctionParamValue{"weight": boostNumber(1), "runner": boostString("fake")}},
		{"bad filter", map[string]*schemapb.FunctionParamValue{"weight": boostNumber(1), "filter": boostString("id >")}},
		{"unknown field", map[string]*schemapb.FunctionParamValue{"weight": boostNumber(1), "filter": boostString("missing == 1")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := prepareBoostScoreFromParams(ctx, types.FunctionConfig{Name: BoostScoreFuncName, Params: tc.params})
			require.Error(t, err)
		})
	}
	cfg := types.FunctionConfig{Name: BoostScoreFuncName, Params: map[string]*schemapb.FunctionParamValue{"weight": boostNumber(0)}}
	prepared, err := prepareBoostScoreFromParams(ctx, cfg)
	require.NoError(t, err)
	data, err := proto.Marshal(prepared.(*planpb.ScoreFunction))
	require.NoError(t, err)
	require.NotEmpty(t, data)
	cfg.Prepared = prepared
	cfg.Args = []*schemapb.FunctionChainExprArg{{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: types.SegOffsetFieldName}}}}
	_, err = NewBoostScoreExprFromParams(ctx, cfg)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	cfg.Args = nil
	ctx.Stage = types.StageL1Rerank
	_, err = NewBoostScoreExprFromParams(ctx, cfg)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestBoostScoreRandomParameters(t *testing.T) {
	cfg := types.FunctionConfig{Name: BoostScoreFuncName, Params: map[string]*schemapb.FunctionParamValue{
		"weight": boostNumber(2),
		"random_score": {Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
			"field": boostString("id"), "seed": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 42}},
		}}}},
	}}
	prepared, err := prepareBoostScoreFromParams(types.FunctionBuildContext{Schema: boostFactorySchema(t)}, cfg)
	require.NoError(t, err)
	scorer := prepared.(*planpb.ScoreFunction)
	require.Equal(t, planpb.FunctionType_FunctionTypeRandom, scorer.Type)
	params := map[string]string{}
	for _, p := range scorer.Params {
		params[p.Key] = p.Value
	}
	require.Equal(t, "42", params["seed"])
}

func TestBoostScoreFactoryRequiresPreparedConfiguration(t *testing.T) {
	for _, prepared := range []any{nil, "invalid", (*planpb.ScoreFunction)(nil)} {
		// Valid public params and schema must not trigger compilation in Factory.
		fn, err := NewBoostScoreExprFromParams(types.FunctionBuildContext{Schema: boostFactorySchema(t), Stage: types.StageL0Rerank}, types.FunctionConfig{
			Name:     BoostScoreFuncName,
			Params:   map[string]*schemapb.FunctionParamValue{"weight": boostNumber(1)},
			Prepared: prepared,
		})
		require.Nil(t, fn)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.ErrorContains(t, err, "prepared configuration")
	}
}
