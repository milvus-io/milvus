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

type pyUDFClient interface {
	Execute(context.Context, pyudf.ExecuteRequest) ([]*arrow.Chunked, error)
}

type PyUDFExpr struct {
	BaseExpr

	resourceName string
	udfParams    *schemapb.FunctionParamObject
	client       pyUDFClient
}

func NewPyUDFExpr(resourceName string, udfParams *schemapb.FunctionParamObject, client pyUDFClient) (*PyUDFExpr, error) {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("py_udf: resource_name is required")
	}
	if udfParams == nil {
		udfParams = &schemapb.FunctionParamObject{}
	}
	// Keep construction independent of runtime configuration and connections so
	// callers can validate IsRunnable before checking enabled or creating a client.
	return &PyUDFExpr{
		BaseExpr:     *NewBaseExpr(PyUDFFuncName, []string{types.StageL2Rerank}),
		resourceName: resourceName,
		udfParams:    udfParams,
		client:       client,
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
	if err := pyudf.CheckEnabled(); err != nil {
		return nil, err
	}
	chunkSizes, err := validatePyUDFInputs(inputs)
	if err != nil {
		return nil, err
	}

	path, err := pyudf.ResolveResourcePath(e.resourceName)
	if err != nil {
		return nil, merr.Wrap(err, "py_udf: resolve resource")
	}
	// NewClient returns the process-shared client. Keep it local: expressions
	// can execute concurrently and must not lazily mutate e.client.
	client := e.client
	if client == nil {
		config, err := pyudf.RuntimeConfig()
		if err != nil {
			return nil, err
		}
		client, err = pyudf.NewClient(config)
		if err != nil {
			return nil, merr.Wrap(err, "py_udf: create client")
		}
	}
	outputs, err := client.Execute(ctx.Context(), pyudf.ExecuteRequest{
		ResourceName: e.resourceName, UDFPath: path, Stage: ctx.Stage(), Params: e.udfParams, Inputs: inputs,
	})
	if err != nil {
		if merr.IsMilvusError(err) || merr.IsCanceledOrTimeout(err) {
			return nil, merr.Wrap(err, "py_udf: Execute")
		}
		return nil, merr.WrapErrServiceInternalErr(err, "py_udf: Execute")
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
		return merr.WrapErrFunctionFailedMsg("py_udf: client returned nil outputs")
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

func init() {
	types.MustRegisterFunction(PyUDFFuncName, NewPyUDFExprFromParams)
}
