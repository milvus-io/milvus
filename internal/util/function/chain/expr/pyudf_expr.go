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
	"strings"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/pyudf"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	PyUDFFuncName = "py_udf"

	pyUDFParamResourceName = "resource_name"
	pyUDFParamUDFParams    = "udf_params"
)

type PyUDFExpr struct {
	BaseExpr

	resourceName string
	udfParams    *schemapb.FunctionParamObject
	runtime      pyudf.Runtime
}

func NewPyUDFExpr(resourceName string, udfParams *schemapb.FunctionParamObject, runtime pyudf.Runtime) (*PyUDFExpr, error) {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("py_udf: resource_name is required")
	}
	if udfParams == nil {
		udfParams = &schemapb.FunctionParamObject{}
	}
	if runtime == nil {
		runtime = globalPyUDFRuntime
	}
	return &PyUDFExpr{
		BaseExpr:     *NewBaseExpr(PyUDFFuncName, []string{types.StageL2Rerank}),
		resourceName: resourceName,
		udfParams:    udfParams,
		runtime:      runtime,
	}, nil
}

func NewPyUDFExprFromParams(_ types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
	if err := validatePyUDFParams(cfg.Params); err != nil {
		return nil, err
	}
	reader := types.NewParamReader(PyUDFFuncName, cfg.Params)
	resourceName, err := reader.String(pyUDFParamResourceName, true)
	if err != nil {
		return nil, err
	}
	udfParams, err := reader.Object(pyUDFParamUDFParams, false)
	if err != nil {
		return nil, err
	}
	return NewPyUDFExpr(resourceName, udfParams, nil)
}

func validatePyUDFParams(params map[string]*schemapb.FunctionParamValue) error {
	for key := range params {
		switch key {
		case pyUDFParamResourceName, pyUDFParamUDFParams:
		default:
			return merr.WrapErrParameterInvalidMsg("py_udf: unknown parameter %q", key)
		}
	}
	return nil
}

func (e *PyUDFExpr) ValidateArgs(args []*schemapb.FunctionChainExprArg) error {
	if len(args) == 0 {
		return merr.WrapErrParameterInvalidMsg("py_udf: expected at least one input column")
	}
	return e.BaseExpr.ValidateArgs(args)
}

func (e *PyUDFExpr) OutputDataTypes() []arrow.DataType {
	return nil
}

func (e *PyUDFExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: function context is nil")
	}
	if !e.IsRunnable(ctx.Stage()) {
		return nil, merr.WrapErrParameterInvalidMsg("py_udf: stage %q is not supported", ctx.Stage())
	}
	if e.runtime == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: runtime is nil")
	}
	chunkSizes, err := validatePyUDFInputs(inputs)
	if err != nil {
		return nil, err
	}

	lease, err := e.runtime.Acquire(ctx.Context(), e.resourceName, ctx.Stage())
	if err != nil {
		return nil, wrapPyUDFRuntimeError(err, "acquire resource")
	}
	if lease == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: runtime returned nil lease")
	}
	defer lease.Release()

	outputs, err := lease.Run(ctx.Context(), e.udfParams, inputs)
	if err != nil {
		return nil, wrapPyUDFRuntimeError(err, "run resource")
	}
	if err := validatePyUDFOutputs(outputs, chunkSizes); err != nil {
		releasePyUDFOutputs(outputs)
		return nil, err
	}
	return outputs, nil
}

func validatePyUDFInputs(inputs []*arrow.Chunked) ([]int, error) {
	if len(inputs) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("py_udf: expected at least one input column")
	}
	if inputs[0] == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: input column 0 is nil")
	}

	numChunks := len(inputs[0].Chunks())
	chunkSizes := make([]int, numChunks)
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		chunk := inputs[0].Chunk(chunkIdx)
		if chunk == nil {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: input column 0 chunk %d is nil", chunkIdx)
		}
		chunkSizes[chunkIdx] = chunk.Len()
	}

	for colIdx := 1; colIdx < len(inputs); colIdx++ {
		input := inputs[colIdx]
		if input == nil {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: input column %d is nil", colIdx)
		}
		if len(input.Chunks()) != numChunks {
			return nil, merr.WrapErrServiceInternalMsg("py_udf: input column 0 has %d chunks but column %d has %d chunks", numChunks, colIdx, len(input.Chunks()))
		}
		for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
			chunk := input.Chunk(chunkIdx)
			if chunk == nil {
				return nil, merr.WrapErrServiceInternalMsg("py_udf: input column %d chunk %d is nil", colIdx, chunkIdx)
			}
			if chunk.Len() != chunkSizes[chunkIdx] {
				return nil, merr.WrapErrServiceInternalMsg("py_udf: input column 0 chunk %d has %d rows but column %d chunk %d has %d rows", chunkIdx, chunkSizes[chunkIdx], colIdx, chunkIdx, chunk.Len())
			}
		}
	}
	return chunkSizes, nil
}

func validatePyUDFOutputs(outputs []*arrow.Chunked, chunkSizes []int) error {
	if outputs == nil {
		return merr.WrapErrFunctionFailedMsg("py_udf: runtime returned nil outputs")
	}
	for outputIdx, output := range outputs {
		if output == nil {
			return merr.WrapErrFunctionFailedMsg("py_udf: output column %d is nil", outputIdx)
		}
		if !isPyUDFOutputTypeSupported(output.DataType().ID()) {
			return merr.WrapErrFunctionFailedMsg("py_udf: output column %d has unsupported type %s", outputIdx, output.DataType())
		}
		if len(output.Chunks()) != len(chunkSizes) {
			return merr.WrapErrFunctionFailedMsg("py_udf: output column %d has %d chunks, expected %d", outputIdx, len(output.Chunks()), len(chunkSizes))
		}
		for chunkIdx, expectedRows := range chunkSizes {
			chunk := output.Chunk(chunkIdx)
			if chunk == nil {
				return merr.WrapErrFunctionFailedMsg("py_udf: output column %d chunk %d is nil", outputIdx, chunkIdx)
			}
			if chunk.Len() != expectedRows {
				return merr.WrapErrFunctionFailedMsg("py_udf: output column %d chunk %d has %d rows, expected %d", outputIdx, chunkIdx, chunk.Len(), expectedRows)
			}
		}
	}
	return nil
}

func isPyUDFOutputTypeSupported(dataType arrow.Type) bool {
	switch dataType {
	case arrow.BOOL,
		arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.FLOAT32, arrow.FLOAT64,
		arrow.STRING:
		return true
	default:
		return false
	}
}

func releasePyUDFOutputs(outputs []*arrow.Chunked) {
	for _, output := range outputs {
		if output != nil {
			output.Release()
		}
	}
}

func wrapPyUDFRuntimeError(err error, action string) error {
	if merr.IsMilvusError(err) || merr.IsCanceledOrTimeout(err) {
		return merr.Wrapf(err, "py_udf: %s", action)
	}
	return merr.WrapErrFunctionFailed(err, "py_udf: %s", action)
}

func init() {
	types.MustRegisterFunction(PyUDFFuncName, NewPyUDFExprFromParams)
}
