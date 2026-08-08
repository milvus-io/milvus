// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expr

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakePyUDFRuntime struct {
	acquire func(context.Context, string, string) (pyudf.Lease, error)
}

func (r *fakePyUDFRuntime) Acquire(ctx context.Context, resourceName, stage string) (pyudf.Lease, error) {
	return r.acquire(ctx, resourceName, stage)
}

type fakePyUDFLease struct {
	run          func(context.Context, *schemapb.FunctionParamObject, []*arrow.Chunked) ([]*arrow.Chunked, error)
	releaseCount int
}

func (l *fakePyUDFLease) Run(ctx context.Context, params *schemapb.FunctionParamObject, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	return l.run(ctx, params, inputs)
}

func (l *fakePyUDFLease) Release() {
	l.releaseCount++
}

func TestNewPyUDFExprFromParams(t *testing.T) {
	udfParams := &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"mode": stringParam("add"),
		"nested": objectParam(map[string]*schemapb.FunctionParamValue{
			"factor": doubleParam(0.3),
		}),
	}}
	fn, err := NewPyUDFExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: map[string]*schemapb.FunctionParamValue{
		pyUDFParamResourceName: stringParam(" rank_udf "),
		pyUDFParamUDFParams:    objectParam(udfParams.Fields),
	}})
	require.NoError(t, err)

	expr, ok := fn.(*PyUDFExpr)
	require.True(t, ok)
	assert.Equal(t, "rank_udf", expr.resourceName)
	assert.Equal(t, udfParams, expr.udfParams)
	assert.Same(t, globalPyUDFRuntime, expr.runtime)
	assert.True(t, expr.IsRunnable(types.StageL2Rerank))
	assert.False(t, expr.IsRunnable(types.StageL0Rerank))
	assert.Nil(t, expr.OutputDataTypes())
	assert.True(t, types.HasFunction(PyUDFFuncName))
}

func TestNewPyUDFExprFromParamsDefaults(t *testing.T) {
	fn, err := NewPyUDFExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: map[string]*schemapb.FunctionParamValue{
		pyUDFParamResourceName: stringParam("rank_udf"),
	}})
	require.NoError(t, err)
	expr := fn.(*PyUDFExpr)
	require.NotNil(t, expr.udfParams)
	assert.Empty(t, expr.udfParams.GetFields())
}

func TestNewPyUDFExprFromParamsInvalid(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]*schemapb.FunctionParamValue
		match  string
	}{
		{
			name:   "missing resource name",
			params: map[string]*schemapb.FunctionParamValue{},
			match:  "missing required parameter",
		},
		{
			name: "empty resource name",
			params: map[string]*schemapb.FunctionParamValue{
				pyUDFParamResourceName: stringParam(" "),
			},
			match: "resource_name is required",
		},
		{
			name: "resource name wrong type",
			params: map[string]*schemapb.FunctionParamValue{
				pyUDFParamResourceName: intParam(1),
			},
			match: "must be a string",
		},
		{
			name: "udf params wrong type",
			params: map[string]*schemapb.FunctionParamValue{
				pyUDFParamResourceName: stringParam("rank_udf"),
				pyUDFParamUDFParams:    stringParam("invalid"),
			},
			match: "must be an object",
		},
		{
			name: "udf params nil object",
			params: map[string]*schemapb.FunctionParamValue{
				pyUDFParamResourceName: stringParam("rank_udf"),
				pyUDFParamUDFParams:    {Value: &schemapb.FunctionParamValue_ObjectValue{}},
			},
			match: "must be an object",
		},
		{
			name: "name alias",
			params: map[string]*schemapb.FunctionParamValue{
				pyUDFParamResourceName: stringParam("rank_udf"),
				"name":                 stringParam("rank_udf"),
			},
			match: `unknown parameter "name"`,
		},
		{
			name: "unknown parameter",
			params: map[string]*schemapb.FunctionParamValue{
				pyUDFParamResourceName: stringParam("rank_udf"),
				"unknown":              stringParam("value"),
			},
			match: `unknown parameter "unknown"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewPyUDFExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: test.params})
			require.Error(t, err)
			assert.ErrorContains(t, err, test.match)
		})
	}
}

func TestPyUDFExprValidateArgs(t *testing.T) {
	expr, err := NewPyUDFExpr("rank_udf", nil, nil)
	require.NoError(t, err)

	assert.Same(t, globalPyUDFRuntime, expr.runtime)
	assert.Error(t, expr.ValidateArgs(nil))
	assert.Error(t, expr.ValidateArgs([]*schemapb.FunctionChainExprArg{nil}))
	assert.Error(t, expr.ValidateArgs([]*schemapb.FunctionChainExprArg{pyUDFLiteralArg(stringParam("value"))}))
	assert.NoError(t, expr.ValidateArgs([]*schemapb.FunctionChainExprArg{
		pyUDFColumnArg("a"), pyUDFColumnArg("a"), pyUDFColumnArg("b"),
	}))
}

func TestPyUDFExprExecute(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	inputA := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}, {3}})
	defer inputA.Release()
	inputB := newPyUDFInt64Chunked(pool, [][]int64{{4, 5}, {6}})
	defer inputB.Release()
	output1 := newPyUDFFloat32Chunked(pool, [][]float32{{5, 7}, {9}})
	output2 := newPyUDFStringChunked(pool, [][]string{{"a", "b"}, {"c"}})
	udfParams := &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{"mode": stringParam("add")}}
	lease := &fakePyUDFLease{}
	ctx := context.WithValue(context.Background(), pyUDFTestContextKey{}, "value")

	runtime := &fakePyUDFRuntime{acquire: func(actualCtx context.Context, resourceName, stage string) (pyudf.Lease, error) {
		assert.Same(t, ctx, actualCtx)
		assert.Equal(t, "rank_udf", resourceName)
		assert.Equal(t, types.StageL2Rerank, stage)
		return lease, nil
	}}
	lease.run = func(actualCtx context.Context, params *schemapb.FunctionParamObject, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
		assert.Same(t, ctx, actualCtx)
		assert.Same(t, udfParams, params)
		require.Len(t, inputs, 3)
		assert.Same(t, inputA, inputs[0])
		assert.Same(t, inputA, inputs[1])
		assert.Same(t, inputB, inputs[2])
		return []*arrow.Chunked{output1, output2}, nil
	}

	expr, err := NewPyUDFExpr("rank_udf", udfParams, runtime)
	require.NoError(t, err)
	outputs, err := expr.Execute(types.NewFuncContextFull(ctx, pool, types.StageL2Rerank), []*arrow.Chunked{inputA, inputA, inputB})
	require.NoError(t, err)
	require.Len(t, outputs, 2)
	assert.Same(t, output1, outputs[0])
	assert.Same(t, output2, outputs[1])
	assert.Equal(t, 1, lease.releaseCount)

	for _, output := range outputs {
		output.Release()
	}
}

type pyUDFTestContextKey struct{}

func TestPyUDFExprExecuteErrors(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	input := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}})
	defer input.Release()

	t.Run("nil function context", func(t *testing.T) {
		expr, err := NewPyUDFExpr("rank_udf", nil, &fakePyUDFRuntime{})
		require.NoError(t, err)
		_, err = expr.Execute(nil, []*arrow.Chunked{input})
		assert.ErrorContains(t, err, "function context is nil")
	})

	t.Run("unsupported stage", func(t *testing.T) {
		expr, err := NewPyUDFExpr("rank_udf", nil, &fakePyUDFRuntime{})
		require.NoError(t, err)
		_, err = expr.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL0Rerank), []*arrow.Chunked{input})
		assert.ErrorContains(t, err, "is not supported")
	})

	t.Run("acquire typed error", func(t *testing.T) {
		runtimeErr := merr.WrapErrServiceUnavailableMsg("not ready")
		runtime := &fakePyUDFRuntime{acquire: func(context.Context, string, string) (pyudf.Lease, error) {
			return nil, runtimeErr
		}}
		expr, err := NewPyUDFExpr("rank_udf", nil, runtime)
		require.NoError(t, err)
		_, err = expr.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("nil lease", func(t *testing.T) {
		runtime := &fakePyUDFRuntime{acquire: func(context.Context, string, string) (pyudf.Lease, error) {
			return nil, nil
		}}
		expr, err := NewPyUDFExpr("rank_udf", nil, runtime)
		require.NoError(t, err)
		_, err = expr.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
		assert.ErrorContains(t, err, "nil lease")
	})

	t.Run("run typed error releases lease", func(t *testing.T) {
		lease := &fakePyUDFLease{run: func(context.Context, *schemapb.FunctionParamObject, []*arrow.Chunked) ([]*arrow.Chunked, error) {
			return nil, merr.WrapErrFunctionFailedMsg("python failed")
		}}
		runtime := &fakePyUDFRuntime{acquire: func(context.Context, string, string) (pyudf.Lease, error) {
			return lease, nil
		}}
		expr, err := NewPyUDFExpr("rank_udf", nil, runtime)
		require.NoError(t, err)
		_, err = expr.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrFunctionFailed)
		assert.Equal(t, 1, lease.releaseCount)
	})

	t.Run("raw runtime error becomes function failed", func(t *testing.T) {
		rawErr := errors.New("python failed")
		lease := &fakePyUDFLease{run: func(context.Context, *schemapb.FunctionParamObject, []*arrow.Chunked) ([]*arrow.Chunked, error) {
			return nil, rawErr
		}}
		runtime := &fakePyUDFRuntime{acquire: func(context.Context, string, string) (pyudf.Lease, error) {
			return lease, nil
		}}
		expr, err := NewPyUDFExpr("rank_udf", nil, runtime)
		require.NoError(t, err)
		_, err = expr.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
		require.Error(t, err)
		assert.ErrorIs(t, err, merr.ErrFunctionFailed)
		assert.ErrorIs(t, err, rawErr)
		assert.Equal(t, 1, lease.releaseCount)
	})

	t.Run("cancellation preserved", func(t *testing.T) {
		lease := &fakePyUDFLease{run: func(context.Context, *schemapb.FunctionParamObject, []*arrow.Chunked) ([]*arrow.Chunked, error) {
			return nil, context.Canceled
		}}
		runtime := &fakePyUDFRuntime{acquire: func(context.Context, string, string) (pyudf.Lease, error) {
			return lease, nil
		}}
		expr, err := NewPyUDFExpr("rank_udf", nil, runtime)
		require.NoError(t, err)
		_, err = expr.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, lease.releaseCount)
	})
}

func TestValidatePyUDFInputs(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	valid := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}, {3}})
	defer valid.Release()
	sameShape := newPyUDFInt64Chunked(pool, [][]int64{{4, 5}, {6}})
	defer sameShape.Release()
	badChunks := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}})
	defer badChunks.Release()
	badRows := newPyUDFFloat32Chunked(pool, [][]float32{{1}, {2}})
	defer badRows.Release()

	chunkSizes, err := validatePyUDFInputs([]*arrow.Chunked{valid, sameShape})
	require.NoError(t, err)
	assert.Equal(t, []int{2, 1}, chunkSizes)

	_, err = validatePyUDFInputs(nil)
	assert.ErrorContains(t, err, "expected at least one")
	_, err = validatePyUDFInputs([]*arrow.Chunked{nil})
	assert.ErrorContains(t, err, "column 0 is nil")
	_, err = validatePyUDFInputs([]*arrow.Chunked{valid, nil})
	assert.ErrorContains(t, err, "column 1 is nil")
	_, err = validatePyUDFInputs([]*arrow.Chunked{valid, badChunks})
	assert.ErrorContains(t, err, "has 1 chunks")
	_, err = validatePyUDFInputs([]*arrow.Chunked{valid, badRows})
	assert.ErrorContains(t, err, "has 1 rows")
}

func TestValidatePyUDFOutputs(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	validFloat := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}, {3}})
	defer validFloat.Release()
	validString := newPyUDFStringChunked(pool, [][]string{{"a", "b"}, {"c"}})
	defer validString.Release()
	unsupported := newPyUDFUint64Chunked(pool, [][]uint64{{1, 2}, {3}})
	defer unsupported.Release()
	badChunks := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2, 3}})
	defer badChunks.Release()
	badRows := newPyUDFFloat32Chunked(pool, [][]float32{{1}, {2}})
	defer badRows.Release()

	assert.NoError(t, validatePyUDFOutputs([]*arrow.Chunked{}, []int{2, 1}))
	assert.NoError(t, validatePyUDFOutputs([]*arrow.Chunked{validFloat, validString}, []int{2, 1}))
	assert.Error(t, validatePyUDFOutputs(nil, []int{2, 1}))
	assert.Error(t, validatePyUDFOutputs([]*arrow.Chunked{nil}, []int{2, 1}))
	assert.ErrorContains(t, validatePyUDFOutputs([]*arrow.Chunked{unsupported}, []int{2, 1}), "unsupported type")
	assert.ErrorContains(t, validatePyUDFOutputs([]*arrow.Chunked{badChunks}, []int{2, 1}), "expected 2")
	assert.ErrorContains(t, validatePyUDFOutputs([]*arrow.Chunked{badRows}, []int{2, 1}), "expected 2")
}

func TestPyUDFExprValidationErrorReleasesOutputs(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)

	input := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}})
	defer input.Release()
	validOutput := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}})
	invalidOutput := newPyUDFUint64Chunked(pool, [][]uint64{{1, 2}})
	lease := &fakePyUDFLease{run: func(context.Context, *schemapb.FunctionParamObject, []*arrow.Chunked) ([]*arrow.Chunked, error) {
		return []*arrow.Chunked{validOutput, invalidOutput}, nil
	}}
	runtime := &fakePyUDFRuntime{acquire: func(context.Context, string, string) (pyudf.Lease, error) {
		return lease, nil
	}}
	expr, err := NewPyUDFExpr("rank_udf", nil, runtime)
	require.NoError(t, err)

	_, err = expr.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrFunctionFailed)
	assert.Equal(t, 1, lease.releaseCount)
}

func pyUDFColumnArg(name string) *schemapb.FunctionChainExprArg {
	return &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: name}}}
}

func pyUDFLiteralArg(value *schemapb.FunctionParamValue) *schemapb.FunctionChainExprArg {
	return &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Literal{Literal: value}}
}

func objectParam(fields map[string]*schemapb.FunctionParamValue) *schemapb.FunctionParamValue {
	return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{Fields: fields}}}
}

func newPyUDFFloat32Chunked(pool memory.Allocator, values [][]float32) *arrow.Chunked {
	chunks := make([]arrow.Array, 0, len(values))
	for _, chunkValues := range values {
		builder := array.NewFloat32Builder(pool)
		builder.AppendValues(chunkValues, nil)
		chunks = append(chunks, builder.NewArray())
		builder.Release()
	}
	return newPyUDFChunked(arrow.PrimitiveTypes.Float32, chunks)
}

func newPyUDFInt64Chunked(pool memory.Allocator, values [][]int64) *arrow.Chunked {
	chunks := make([]arrow.Array, 0, len(values))
	for _, chunkValues := range values {
		builder := array.NewInt64Builder(pool)
		builder.AppendValues(chunkValues, nil)
		chunks = append(chunks, builder.NewArray())
		builder.Release()
	}
	return newPyUDFChunked(arrow.PrimitiveTypes.Int64, chunks)
}

func newPyUDFStringChunked(pool memory.Allocator, values [][]string) *arrow.Chunked {
	chunks := make([]arrow.Array, 0, len(values))
	for _, chunkValues := range values {
		builder := array.NewStringBuilder(pool)
		builder.AppendValues(chunkValues, nil)
		chunks = append(chunks, builder.NewArray())
		builder.Release()
	}
	return newPyUDFChunked(arrow.BinaryTypes.String, chunks)
}

func newPyUDFUint64Chunked(pool memory.Allocator, values [][]uint64) *arrow.Chunked {
	chunks := make([]arrow.Array, 0, len(values))
	for _, chunkValues := range values {
		builder := array.NewUint64Builder(pool)
		builder.AppendValues(chunkValues, nil)
		chunks = append(chunks, builder.NewArray())
		builder.Release()
	}
	return newPyUDFChunked(arrow.PrimitiveTypes.Uint64, chunks)
}

func newPyUDFChunked(dataType arrow.DataType, chunks []arrow.Array) *arrow.Chunked {
	chunked := arrow.NewChunked(dataType, chunks)
	for _, chunk := range chunks {
		chunk.Release()
	}
	return chunked
}
