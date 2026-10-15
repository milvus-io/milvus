// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain

import (
	"errors"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/grep"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() {
	MustRegisterOperator(types.OpTypeGrep, NewGrepOpFromReprWithContext)
}

// GrepOp selects rows and emits matching details. It consumes only offsets;
// the public text-field input is a native dependency resolved during preparation.
// Each instance binds one borrowed segment and snapshots its Go parameters.
// Native compilation and mutable execution state are local to that segment
// call. No Close is required; the caller keeps the segment pinned.
type GrepOp struct {
	BaseOp
	segment native.Segment
	request native.ScanRequest
}

func (*GrepOp) Name() string                 { return "Grep" }
func (*GrepOp) String() string               { return "Grep" }
func (*GrepOp) IsRunnable(stage string) bool { return stage == types.StageQueryScan }

// validateGrepRepr does not parse rg syntax or compile regexes. It is shared by
// schema-aware static planning and execution-node operator construction.
func validateGrepRepr(repr *OperatorRepr, maximum int64) (string, int64, error) {
	if repr == nil || repr.Type != types.OpTypeGrep || repr.Function != nil {
		return "", 0, merr.WrapErrParameterInvalidMsg("grep requires an independent operator without an expression")
	}
	if len(repr.Inputs) != 1 || len(repr.Outputs) != 1 {
		return "", 0, merr.WrapErrParameterInvalidMsg("grep requires exactly one input and one output")
	}
	for _, name := range []string{repr.Inputs[0], repr.Outputs[0]} {
		if name == "" || strings.TrimSpace(name) != name || strings.HasPrefix(name, "$") {
			return "", 0, merr.WrapErrParameterInvalidMsg("invalid grep field name %q", name)
		}
	}
	if repr.Inputs[0] == repr.Outputs[0] {
		return "", 0, merr.WrapErrParameterInvalidMsg("grep output must not overwrite its input field")
	}
	arguments, ok := repr.Params["arguments"].GetValue().(*schemapb.FunctionParamValue_StringValue)
	if !ok || arguments == nil {
		return "", 0, merr.WrapErrParameterInvalidMsg("grep arguments must be a string")
	}
	limitValue, ok := repr.Params["limit"].GetValue().(*schemapb.FunctionParamValue_Int64Value)
	if !ok || limitValue == nil || limitValue.Int64Value <= 0 {
		return "", 0, merr.WrapErrParameterInvalidMsg("grep limit must be an explicit positive int64")
	}
	if len(repr.Params) != 2 {
		return "", 0, merr.WrapErrParameterInvalidMsg("grep accepts only arguments and limit parameters")
	}
	if limitValue.Int64Value > maximum {
		return "", 0, merr.WrapErrParameterInvalidMsg("grep limit %d exceeds function.grep.maxLimit %d", limitValue.Int64Value, maximum)
	}
	return arguments.StringValue, limitValue.Int64Value, nil
}

func NewGrepOpFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	maximum := paramtable.Get().FunctionCfg.GrepMaxLimit.GetAsInt64()
	arguments, limit, err := validateGrepRepr(repr, maximum)
	if err != nil {
		return nil, err
	}
	binding := buildCtx.QueryScan
	if binding == nil {
		return nil, merr.WrapErrServiceInternalMsg("grep preparation requires a QueryScan binding")
	}
	if binding.Segment == nil {
		return nil, merr.WrapErrServiceInternalMsg("grep preparation requires a segment binding")
	}
	maxResultBytes := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	if maxResultBytes <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("grep execution budgets must be positive")
	}
	config := &paramtable.Get().FunctionCfg
	compileLimits := native.CompileLimits{
		MaxRegexBytes:    uint64(config.GrepMaxRegexBytes.GetAsInt64()),
		MaxDFACacheBytes: uint64(config.GrepMaxDFACacheBytes.GetAsInt64()),
	}
	spec, err := grep.Parse(arguments, config.GrepMaxArgumentBytes.GetAsInt64())
	if err != nil {
		if errors.Is(err, merr.ErrParameterTooLarge) {
			return nil, merr.Wrap(err, "prepare grep arguments (function.grep.maxArgumentBytes)")
		}
		return nil, merr.Wrap(err, "prepare grep arguments")
	}
	executionLimits := native.ExecutionLimits{
		BatchSize:      config.GrepBatchSize.GetAsInt64(),
		MaxResultBytes: uint64(maxResultBytes),
	}
	return &GrepOp{
		BaseOp:  BaseOp{inputs: []string{types.SegOffsetFieldName}, outputs: append([]string(nil), repr.Outputs...)},
		segment: binding.Segment,
		request: native.ScanRequest{
			FieldID: binding.FieldID, Spec: *spec, Limit: limit,
			CompileLimits: compileLimits, ExecutionLimits: executionLimits,
		},
	}, nil
}

func (o *GrepOp) Execute(ctx *types.FuncContext, input *DataFrame) (*OperatorResult, error) {
	if ctx == nil || o == nil {
		return nil, merr.WrapErrServiceInternalMsg("grep execution requires context and operator")
	}
	if o.segment == nil {
		return nil, merr.WrapErrServiceInternalMsg("grep execution requires a segment binding")
	}
	if !o.IsRunnable(ctx.Stage()) {
		return nil, merr.WrapErrServiceInternalMsg("grep executed outside QueryScan")
	}
	if err := ctx.Context().Err(); err != nil {
		return nil, merr.Wrap(err, "execute grep")
	}
	if err := validateFrameLayout(input); err != nil {
		return nil, err
	}
	if input.NumChunks() != 1 {
		return nil, merr.WrapErrServiceInternalMsg("grep requires one segment and one logical query chunk")
	}
	if input.HasColumn(o.outputs[0]) {
		return nil, merr.WrapErrServiceInternalMsg("grep output conflicts with an existing runtime column")
	}
	columns, err := o.ReadInputColumns("grep", input)
	if err != nil {
		return nil, err
	}
	offsets, ok := columns[0].Chunk(0).(*array.Int64)
	if !ok || offsets.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("grep offsets must be non-null Int64")
	}
	for i, offset := range offsets.Int64Values() {
		if i%1024 == 0 {
			if err := ctx.Context().Err(); err != nil {
				return nil, merr.Wrap(err, "validate grep offsets")
			}
		}
		if offset < 0 {
			return nil, merr.WrapErrServiceInternalMsg("grep offset must be non-negative")
		}
	}
	// A private slice prevents adapters from changing another execution's
	// configuration. Strings and scalar options are immutable/value data.
	req := o.request
	req.Spec.Patterns = append([]string(nil), o.request.Spec.Patterns...)
	result, err := o.segment.ScanGrep(ctx.Context(), offsets, req)
	defer result.Release()
	if err != nil {
		return nil, merr.Wrap(err, "execute grep runner")
	}
	if err := ctx.Context().Err(); err != nil {
		return nil, merr.Wrap(err, "execute grep runner")
	}
	if result == nil || result.Details == nil || !arrow.TypeEqual(result.Details.DataType(), native.GrepDetailsType()) {
		return nil, merr.WrapErrServiceInternalMsg("grep runner returned an invalid detail column")
	}
	if int64(len(result.Positions)) > o.request.Limit || result.Details.Len() != len(result.Positions) || result.Details.NullN() != 0 {
		return nil, merr.WrapErrServiceInternalMsg("grep runner returned inconsistent rows or exceeded the matching limit")
	}
	details, ok := result.Details.(*array.List)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("grep runner returned an invalid Arrow list implementation")
	}
	indices := make([]int, len(result.Positions))
	previous := int64(-1)
	for i, position := range result.Positions {
		if position <= previous || position >= int64(offsets.Len()) {
			return nil, merr.WrapErrServiceInternalMsg("grep runner positions must be increasing input positions")
		}
		start, end := details.ValueOffsets(i)
		if start == end {
			return nil, merr.WrapErrServiceInternalMsg("grep runner selected a row without matching lines")
		}
		indices[i] = int(position)
		previous = position
	}
	return &OperatorResult{
		Selection: &RowSelection{Chunks: []ChunkSelection{RowIndices(indices)}},
		Columns: []NamedColumn{{
			Field: arrow.Field{Name: o.outputs[0], Type: native.GrepDetailsType(), Nullable: false},
			Data:  arrow.NewChunked(native.GrepDetailsType(), []arrow.Array{result.Details}),
		}},
	}, nil
}
