/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRegisteredFilterPreservesFunctionErrorAndReleasesOutputs(t *testing.T) {
	_, registered := GetOperatorFactory(types.OpTypeFilter)
	require.True(t, registered, "filter must be available to submitted chains")
	for _, original := range []error{merr.ErrFunctionFailed, merr.ErrIoTooManyRequests, merr.ErrServiceMemoryLimitExceeded, merr.ErrParameterInvalid, context.Canceled, context.DeadlineExceeded} {
		t.Run(original.Error(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			input := mapScoreTestFrame(t, pool)
			defer input.Release()
			fn := &operatorTransformExpr{name: "failing_filter", stages: []string{types.StageL2Rerank}, outputs: []arrow.DataType{arrow.FixedWidthTypes.Boolean}, execute: func(ctx *types.FuncContext, _ []*arrow.Chunked) ([]*arrow.Chunked, error) {
				// An adapter may return a partially constructed output together with its
				// error. Neither Filter nor the chain may publish or leak that output.
				b := array.NewBooleanBuilder(ctx.Pool())
				b.Append(true)
				data := b.NewArray()
				b.Release()
				output := arrow.NewChunked(arrow.FixedWidthTypes.Boolean, []arrow.Array{data})
				data.Release()
				return []*arrow.Chunked{output}, original
			}}
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StageL2Rerank).Filter(fn, []string{types.ScoreFieldName}).Execute(input)
			require.Nil(t, output)
			require.ErrorIs(t, err, original)
			require.Equal(t, merr.Code(original), merr.Code(err))
			require.Equal(t, merr.Status(original).GetRetriable(), merr.Status(err).GetRetriable())
			require.Equal(t, merr.Status(original).GetExtraInfo(), merr.Status(err).GetExtraInfo())
		})
	}
}

func TestFilterFactoryPreservesFunctionConstructionError(t *testing.T) {
	const name = "filter_factory_unavailable"
	original := merr.WrapErrServiceUnavailableMsg("function resource is not ready")
	types.RegisterFunction(name, func(types.FunctionBuildContext, types.FunctionConfig) (types.FunctionExpr, error) {
		return nil, original
	})
	op, err := operatorFromRepr(&OperatorRepr{
		Type: types.OpTypeFilter, Function: &FunctionRepr{Name: name}, Inputs: []string{"score"},
	})
	require.Nil(t, op)
	require.ErrorIs(t, err, original)
	require.Equal(t, merr.Code(original), merr.Code(err))
	require.True(t, merr.Status(err).GetRetriable())
}
