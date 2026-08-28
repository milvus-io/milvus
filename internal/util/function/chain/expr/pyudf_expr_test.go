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
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakePyUDFClient struct {
	execute func(context.Context, pyudf.ExecuteRequest) ([]*arrow.Chunked, error)
}

func (c *fakePyUDFClient) Execute(ctx context.Context, req pyudf.ExecuteRequest) ([]*arrow.Chunked, error) {
	return c.execute(ctx, req)
}

func mockPyUDFPath(t *testing.T, err error) {
	t.Helper()
	patch := mockey.Mock(pyudf.ResolveResourcePath).Return("/tmp/rank.whl", err).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func enablePyUDFForTest(t *testing.T) {
	t.Helper()
	setPyUDFEnabledForTest(t, "true")
	t.Cleanup(func() { require.NoError(t, pyudf.CloseClients()) })
}

func setPyUDFEnabledForTest(t *testing.T, value string) {
	t.Helper()
	var err error
	if value != "true" {
		err = merr.WrapErrParameterInvalidMsg("py_udf: function.pyUDF.enabled is false")
	}
	patch := mockey.Mock(pyudf.CheckEnabled).Return(err).Build()
	t.Cleanup(func() { patch.UnPatch() })
}

func TestPyUDFExprStageAndEnabledBeforeClient(t *testing.T) {
	for _, enabled := range []string{"false", "true"} {
		t.Run(enabled, func(t *testing.T) {
			setPyUDFEnabledForTest(t, enabled)
			configCalls, clientCalls := 0, 0
			defer mockey.Mock(pyudf.RuntimeConfig).To(func() (pyudf.Config, error) {
				configCalls++
				return pyudf.Config{}, merr.ErrServiceInternal
			}).Build().UnPatch()
			defer mockey.Mock(pyudf.NewClient).To(func(pyudf.Config) (*pyudf.Client, error) {
				clientCalls++
				return nil, merr.ErrServiceUnavailable
			}).Build().UnPatch()
			e, err := NewPyUDFExpr("rank_udf", nil, nil)
			require.NoError(t, err)
			require.Nil(t, e.client)
			require.True(t, e.IsRunnable(types.StageL2Rerank))
			for _, stage := range []string{types.StageL0Rerank, types.StageL1Rerank} {
				require.False(t, e.IsRunnable(stage))
				_, err := e.Execute(types.NewFuncContextFull(context.Background(), memory.DefaultAllocator, stage), nil)
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.ErrorContains(t, err, "stage")
				require.NotContains(t, err.Error(), "enabled")
			}
			if enabled == "false" {
				_, err := e.Execute(types.NewFuncContextFull(context.Background(), memory.DefaultAllocator, types.StageL2Rerank), nil)
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.ErrorContains(t, err, "function.pyUDF.enabled is false")
				status := merr.Status(err)
				require.EqualValues(t, 1100, status.Code)
				require.Equal(t, "true", status.ExtraInfo[merr.InputErrorFlagKey])
				require.False(t, status.Retriable)
			}
			require.Zero(t, configCalls)
			require.Zero(t, clientCalls)
		})
	}
}

func TestNewPyUDFExprFromParams(t *testing.T) {
	enablePyUDFForTest(t)
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
	assert.Nil(t, expr.client)
	assert.True(t, expr.IsRunnable(types.StageL2Rerank))
	assert.False(t, expr.IsRunnable(types.StageL0Rerank))
	assert.Nil(t, expr.OutputDataTypes())
	assert.True(t, types.HasFunction(PyUDFFuncName))
}

func TestNewPyUDFExprFromParamsDefaults(t *testing.T) {
	enablePyUDFForTest(t)
	fn, err := NewPyUDFExprFromParams(types.FunctionBuildContext{}, types.FunctionConfig{Params: map[string]*schemapb.FunctionParamValue{
		pyUDFParamResourceName: stringParam("rank_udf"),
	}})
	require.NoError(t, err)
	expr := fn.(*PyUDFExpr)
	require.NotNil(t, expr.udfParams)
	assert.Empty(t, expr.udfParams.GetFields())
}

func TestNewPyUDFExprFromParamsInvalid(t *testing.T) {
	enablePyUDFForTest(t)
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
	enablePyUDFForTest(t)
	expr, err := NewPyUDFExpr("rank_udf", nil, nil)
	require.NoError(t, err)

	assert.Nil(t, expr.client)
	assert.Error(t, expr.ValidateArgs(nil))
	assert.Error(t, expr.ValidateArgs([]*schemapb.FunctionChainExprArg{nil}))
	assert.Error(t, expr.ValidateArgs([]*schemapb.FunctionChainExprArg{pyUDFLiteralArg(stringParam("value"))}))
	assert.NoError(t, expr.ValidateArgs([]*schemapb.FunctionChainExprArg{
		pyUDFColumnArg("a"), pyUDFColumnArg("a"), pyUDFColumnArg("b"),
	}))
}

func TestPyUDFExprExecute(t *testing.T) {
	enablePyUDFForTest(t)
	mockPyUDFPath(t, nil)
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}, {3}})
	defer input.Release()
	params := &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{"mode": stringParam("add")}}
	client := &fakePyUDFClient{execute: func(ctx context.Context, req pyudf.ExecuteRequest) ([]*arrow.Chunked, error) {
		assert.Equal(t, "rank_udf", req.ResourceName)
		assert.Equal(t, "/tmp/rank.whl", req.UDFPath)
		assert.Equal(t, types.StageL2Rerank, req.Stage)
		assert.Same(t, params, req.Params)
		require.Len(t, req.Inputs, 2)
		assert.Same(t, input, req.Inputs[0])
		assert.Same(t, input, req.Inputs[1])
		return []*arrow.Chunked{newPyUDFFloat32Chunked(pool, [][]float32{{2, 4}, {6}})}, nil
	}}
	e, err := NewPyUDFExpr("rank_udf", params, client)
	require.NoError(t, err)
	outputs, err := e.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input, input})
	require.NoError(t, err)
	require.Len(t, outputs, 1)
	releasePyUDFOutputs(outputs)
}

func TestPyUDFExprCreatesClientOnlyForExecution(t *testing.T) {
	enablePyUDFForTest(t)
	defer mockey.Mock(pyudf.RuntimeConfig).Return(pyudf.Config{Enabled: true}, nil).Build().UnPatch()
	mockPyUDFPath(t, nil)
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}})
	defer input.Release()
	client := &pyudf.Client{}
	created, executed := 0, 0
	defer mockey.Mock(pyudf.NewClient).To(func(config pyudf.Config) (*pyudf.Client, error) {
		created++
		require.True(t, config.Enabled)
		return client, nil
	}).Build().UnPatch()
	defer mockey.Mock((*pyudf.Client).Execute).To(func(c *pyudf.Client, _ context.Context, request pyudf.ExecuteRequest) ([]*arrow.Chunked, error) {
		require.Same(t, client, c)
		require.Equal(t, "/tmp/rank.whl", request.UDFPath)
		executed++
		return []*arrow.Chunked{newPyUDFFloat32Chunked(pool, [][]float32{{2, 4}})}, nil
	}).Build().UnPatch()
	e, err := NewPyUDFExpr("rank_udf", nil, nil)
	require.NoError(t, err)
	require.True(t, e.IsRunnable(types.StageL2Rerank))
	require.Zero(t, created)
	ctx := types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank)
	for range 2 {
		outputs, err := e.Execute(ctx, []*arrow.Chunked{input})
		require.NoError(t, err)
		require.Len(t, outputs, 1)
		releasePyUDFOutputs(outputs)
	}
	require.Equal(t, 2, created)
	require.Equal(t, 2, executed)
	require.Nil(t, e.client, "execution must not mutate the expression's client field")
}

func TestPyUDFExprClientInitializationErrorsRemainSystemErrors(t *testing.T) {
	enablePyUDFForTest(t)
	mockPyUDFPath(t, nil)
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := newPyUDFFloat32Chunked(pool, [][]float32{{1}})
	defer input.Release()
	for _, tc := range []struct {
		name      string
		configErr error
		clientErr error
		want      error
	}{
		{"configuration", merr.ErrServiceInternal, nil, merr.ErrServiceInternal},
		{"client", nil, merr.ErrServiceUnavailable, merr.ErrServiceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer mockey.Mock(pyudf.RuntimeConfig).Return(pyudf.Config{}, tc.configErr).Build().UnPatch()
			defer mockey.Mock(pyudf.NewClient).Return(nil, tc.clientErr).Build().UnPatch()
			e, err := NewPyUDFExpr("rank_udf", nil, nil)
			require.NoError(t, err)
			_, err = e.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
			require.ErrorIs(t, err, tc.want)
			status := merr.Status(err)
			require.Equal(t, merr.Code(tc.want), status.Code)
			require.NotEqual(t, "true", status.ExtraInfo[merr.InputErrorFlagKey])
			require.Equal(t, tc.want == merr.ErrServiceUnavailable, status.Retriable)
		})
	}
}

func TestPyUDFExprExecuteErrors(t *testing.T) {
	enablePyUDFForTest(t)
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}})
	defer input.Release()
	for _, tc := range []struct {
		name     string
		err      error
		expected error
	}{
		{"unavailable", merr.ErrServiceUnavailable, merr.ErrServiceUnavailable},
		{"udf", merr.ErrFunctionFailed, merr.ErrFunctionFailed},
		{"cancel", context.Canceled, context.Canceled},
		{"deadline", context.DeadlineExceeded, context.DeadlineExceeded},
		{"unexpected", errors.New("unexpected client failure"), merr.ErrServiceInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockPyUDFPath(t, nil)
			e, err := NewPyUDFExpr("rank_udf", nil, &fakePyUDFClient{execute: func(context.Context, pyudf.ExecuteRequest) ([]*arrow.Chunked, error) { return nil, tc.err }})
			require.NoError(t, err)
			outputs, err := e.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
			require.Nil(t, outputs)
			require.ErrorIs(t, err, tc.expected)
			require.ErrorIs(t, err, tc.err)
			assert.Equal(t, merr.Code(tc.expected), merr.Code(err))
		})
	}
	t.Run("missing_snapshot", func(t *testing.T) {
		mockPyUDFPath(t, merr.ErrServiceUnavailable)
		e, err := NewPyUDFExpr("rank_udf", nil, &fakePyUDFClient{execute: func(context.Context, pyudf.ExecuteRequest) ([]*arrow.Chunked, error) {
			t.Fatal("must not execute before resource sync")
			return nil, nil
		}})
		require.NoError(t, err)
		_, err = e.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})
	t.Run("context_and_stage", func(t *testing.T) {
		e, err := NewPyUDFExpr("rank_udf", nil, &fakePyUDFClient{})
		require.NoError(t, err)
		_, err = e.Execute(nil, []*arrow.Chunked{input})
		require.ErrorContains(t, err, "function context is nil")
		_, err = e.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL0Rerank), []*arrow.Chunked{input})
		require.ErrorContains(t, err, "is not supported")
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
	enablePyUDFForTest(t)
	mockPyUDFPath(t, nil)
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer pool.AssertSize(t, 0)
	input := newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}})
	defer input.Release()
	e, err := NewPyUDFExpr("rank_udf", nil, &fakePyUDFClient{execute: func(context.Context, pyudf.ExecuteRequest) ([]*arrow.Chunked, error) {
		return []*arrow.Chunked{newPyUDFFloat32Chunked(pool, [][]float32{{1, 2}}), newPyUDFFloat32Chunked(pool, [][]float32{{1}})}, nil
	}})
	require.NoError(t, err)
	outputs, err := e.Execute(types.NewFuncContextFull(context.Background(), pool, types.StageL2Rerank), []*arrow.Chunked{input})
	require.Nil(t, outputs)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
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
