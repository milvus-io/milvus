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

package expr

import (
	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	XGBoostFuncName = "xgboost"

	xgboostParamModelResource = "model_resource"
	xgboostParamOutput        = "output"
	xgboostParamFeatureNames  = "feature_names"
	xgboostParamObjective     = "objective"

	xgboostOutputDefault = "default"
	xgboostOutputRaw     = "raw"
)

type XGBoostExpr struct {
	BaseExpr

	modelResource string
	output        string
	cache         *xgboostModelCache
}

func NewXGBoostExpr(modelResource string, output string, cache *xgboostModelCache) (*XGBoostExpr, error) {
	if modelResource == "" {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: model_resource is required")
	}
	if output == "" {
		output = xgboostOutputDefault
	}
	if output != xgboostOutputDefault && output != xgboostOutputRaw {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: output must be one of [%s, %s], got %q", xgboostOutputDefault, xgboostOutputRaw, output)
	}
	if cache == nil {
		cache = globalXGBoostModelCache
	}
	return &XGBoostExpr{
		BaseExpr:      *NewBaseExpr(XGBoostFuncName, []string{types.StageL0Rerank}),
		modelResource: modelResource,
		output:        output,
		cache:         cache,
	}, nil
}

func NewXGBoostExprFromParams(_ types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
	reader := types.NewParamReader(XGBoostFuncName, cfg.Params)
	if err := validateXGBoostParams(cfg.Params); err != nil {
		return nil, err
	}
	modelResource, err := reader.String(xgboostParamModelResource, true)
	if err != nil {
		return nil, err
	}
	output, err := reader.String(xgboostParamOutput, false)
	if err != nil {
		return nil, err
	}
	return NewXGBoostExpr(modelResource, output, nil)
}

func validateXGBoostParams(params map[string]*schemapb.FunctionParamValue) error {
	allowed := map[string]struct{}{
		xgboostParamModelResource: {},
		xgboostParamOutput:        {},
	}
	for key := range params {
		if key == xgboostParamFeatureNames || key == xgboostParamObjective {
			return merr.WrapErrParameterInvalidMsg("xgboost: parameter %q is not supported", key)
		}
		if _, ok := allowed[key]; !ok {
			return merr.WrapErrParameterInvalidMsg("xgboost: unknown parameter %q", key)
		}
	}
	return nil
}

func (e *XGBoostExpr) ValidateArgs(args []*schemapb.FunctionChainExprArg) error {
	if len(args) == 0 {
		return merr.WrapErrParameterInvalidMsg("xgboost: expected at least one feature column")
	}
	return e.BaseExpr.ValidateArgs(args)
}

func (e *XGBoostExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

func (e *XGBoostExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if e.cache == nil {
		return nil, merr.WrapErrServiceInternalMsg("xgboost: model cache is nil")
	}
	if len(inputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: expected at least one input column")
	}
	lease, err := e.cache.acquireByResourceName(e.modelResource)
	if err != nil {
		return nil, err
	}
	defer lease.Release()

	model := lease.Model()
	if model == nil {
		return nil, merr.WrapErrServiceInternalMsg("xgboost: model handle is nil")
	}
	if model.numFeatures > 0 && len(inputs) != model.numFeatures {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: expected %d feature columns, got %d", model.numFeatures, len(inputs))
	}
	if err := validateXGBoostInputChunks(inputs); err != nil {
		return nil, err
	}
	output, err := predictXGBoostArrowChunks(model, inputs, e.output == xgboostOutputDefault, ctx.Pool())
	if err != nil {
		return nil, err
	}
	return []*arrow.Chunked{output}, nil
}

func validateXGBoostInputChunks(inputs []*arrow.Chunked) error {
	if len(inputs) == 0 {
		return merr.WrapErrParameterInvalidMsg("xgboost: expected at least one input column")
	}
	if inputs[0] == nil {
		return merr.WrapErrServiceInternalMsg("xgboost: input column 0 is nil")
	}
	numChunks := len(inputs[0].Chunks())
	for colIdx, input := range inputs {
		if input == nil {
			return merr.WrapErrServiceInternalMsg("xgboost: input column %d is nil", colIdx)
		}
		if len(input.Chunks()) != numChunks {
			return merr.WrapErrServiceInternalMsg("xgboost: input column 0 has %d chunks but column %d has %d chunks", numChunks, colIdx, len(input.Chunks()))
		}
	}
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		baseChunk := inputs[0].Chunk(chunkIdx)
		if baseChunk == nil {
			return merr.WrapErrServiceInternalMsg("xgboost: input column 0 chunk %d is nil", chunkIdx)
		}
		chunkLen := baseChunk.Len()
		for colIdx := 1; colIdx < len(inputs); colIdx++ {
			chunk := inputs[colIdx].Chunk(chunkIdx)
			if chunk == nil {
				return merr.WrapErrServiceInternalMsg("xgboost: input column %d chunk %d is nil", colIdx, chunkIdx)
			}
			if chunk.Len() != chunkLen {
				return merr.WrapErrServiceInternalMsg("xgboost: input column 0 chunk %d has %d rows but column %d chunk %d has %d rows", chunkIdx, chunkLen, colIdx, chunkIdx, chunk.Len())
			}
		}
	}
	return nil
}

func init() {
	types.MustRegisterFunction(XGBoostFuncName, NewXGBoostExprFromParams)
}
