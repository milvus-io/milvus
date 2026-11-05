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

package milvusclient

import (
	"math"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// WithFunctionChains replaces the ordinary Search chains. Empty input clears
// them. Builders are read when Request is called; do not mutate them concurrently.
func (opt *searchOption) WithFunctionChains(chains ...*entity.FunctionChain) *searchOption {
	opt.annRequest.WithFunctionChains(chains...)
	return opt
}

// WithFunctionChains replaces this ANN leg's chains. HybridSearch accepts L0/L1
// here, independently for each leg; cross-leg fusion belongs on the Hybrid option.
func (r *AnnRequest) WithFunctionChains(chains ...*entity.FunctionChain) *AnnRequest {
	r.functionChains = slices.Clone(chains)
	return r
}

// WithFunctionChains replaces the Hybrid fusion/rerank plan. Exactly one L2
// chain with a leading merge is required; legacy rankers cannot be combined.
func (opt *hybridSearchOption) WithFunctionChains(chains ...*entity.FunctionChain) *hybridSearchOption {
	opt.functionChains = slices.Clone(chains)
	return opt
}

// WithFunctionChains replaces the QueryScan plan. Requires a server supporting
// QueryScan; older servers may ignore the field. Count, aggregation, order-by
// and iterators cannot be combined with QueryScan.
func (opt *queryOption) WithFunctionChains(chains ...*entity.FunctionChain) *queryOption {
	opt.functionChains = slices.Clone(chains)
	return opt
}

func functionChainProtos(chains []*entity.FunctionChain, stages ...entity.FunctionChainStage) ([]*schemapb.FunctionChain, error) {
	var result []*schemapb.FunctionChain
	seen := make(map[entity.FunctionChainStage]bool)
	for i, chain := range chains {
		pb, err := chain.ProtoMessage()
		if err != nil {
			return nil, errors.Wrapf(err, "function chain[%d]", i)
		}
		if !slices.Contains(stages, pb.Stage) {
			return nil, merr.WrapErrParameterInvalidMsg("function chain stage %s is not supported in this request", pb.Stage)
		}
		if seen[pb.Stage] {
			return nil, merr.WrapErrParameterInvalidMsg("function chain stage %s appears more than once", pb.Stage)
		}
		seen[pb.Stage] = true
		if len(pb.Ops) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("function chain must contain at least one op")
		}
		for _, op := range pb.Ops {
			if pb.Stage == entity.FunctionChainStageL0Rerank && op.Op != "map" {
				return nil, merr.WrapErrParameterInvalidMsg("L0 function chains support only map")
			}
			if pb.Stage == entity.FunctionChainStageL1Rerank && !slices.Contains([]string{"map", "sort", "limit"}, op.Op) {
				return nil, merr.WrapErrParameterInvalidMsg("L1 function chains support only map, sort and limit")
			}
			if op.Op == "grep" && pb.Stage != entity.FunctionChainStageQueryScan {
				return nil, merr.WrapErrParameterInvalidMsg("grep requires QueryScan stage")
			}
			if op.Expr != nil && slices.Contains([]string{"rerank_model", "py_udf"}, op.Expr.Name) && pb.Stage != entity.FunctionChainStageL2Rerank {
				return nil, merr.WrapErrParameterInvalidMsg("%s requires L2 stage", op.Expr.Name)
			}
		}
		result = append(result, pb)
	}
	// Execution order is stage-owned, not caller-supplied order.
	slices.SortStableFunc(result, func(a, b *schemapb.FunctionChain) int { return int(a.Stage - b.Stage) })
	return result, nil
}

func validateSearchChains(chains []*schemapb.FunctionChain, score *entity.FunctionScore, params map[string]string) error {
	if len(chains) == 0 {
		return nil
	}
	if score != nil {
		return merr.WrapErrParameterInvalidMsg("function_chains and function_score cannot be used together")
	}
	for _, key := range []string{spOrderByFields, "iterator", "search_iter_v2"} {
		if value := strings.TrimSpace(params[key]); value != "" && value != "false" {
			return merr.WrapErrParameterInvalidMsg("function_chains cannot be used with %s", key)
		}
	}
	for _, chain := range chains {
		for _, op := range chain.Ops {
			if op.Op == "merge" {
				return merr.WrapErrParameterInvalidMsg("merge is supported only by top-level HybridSearch chains")
			}
		}
	}
	return nil
}

func validateHybridChains(chains []*schemapb.FunctionChain, inputCount int) error {
	if len(chains) == 0 {
		return nil
	}
	// Cardinality and L2 placement were checked by functionChainProtos.
	ops := chains[0].Ops
	if ops[0].Op != "merge" {
		return merr.WrapErrParameterInvalidMsg("hybrid function chain must start with merge")
	}
	for _, op := range ops[1:] {
		if op.Op == "merge" {
			return merr.WrapErrParameterInvalidMsg("hybrid function chain must contain exactly one merge")
		}
	}
	op := ops[0]
	if op.Expr != nil || len(op.Inputs) > 0 || len(op.Outputs) > 0 {
		return merr.WrapErrParameterInvalidMsg("merge does not accept expr, inputs or outputs")
	}
	strategy := op.Params["strategy"].GetStringValue()
	allowed := []string{"strategy"}
	switch strategy {
	case "rrf":
		allowed = append(allowed, "k", "weights")
	case "weighted":
		allowed = append(allowed, "weights", "norm_score")
	case "max", "sum", "avg":
		allowed = append(allowed, "norm_score")
	default:
		return merr.WrapErrParameterInvalidMsg("unsupported merge strategy %q", strategy)
	}
	for key := range op.Params {
		if !slices.Contains(allowed, key) {
			return merr.WrapErrParameterInvalidMsg("merge parameter %q is not supported for %s", key, strategy)
		}
	}
	if k, exists := op.Params["k"]; exists {
		value, ok := functionChainNumber(k)
		if !ok || value <= 0 || value >= 16384 {
			return merr.WrapErrParameterInvalidMsg("merge k must be finite and in (0, 16384)")
		}
	}
	if norm, exists := op.Params["norm_score"]; exists {
		if _, ok := norm.GetValue().(*schemapb.FunctionParamValue_BoolValue); !ok {
			return merr.WrapErrParameterInvalidMsg("merge norm_score must be bool")
		}
	}
	weights, exists := op.Params["weights"]
	if exists || strategy == "weighted" {
		values := weights.GetArrayValue().GetValues()
		if len(values) == 0 || len(values) != inputCount {
			return merr.WrapErrParameterInvalidMsg("merge weights must match the sub-search count")
		}
		for _, weight := range values {
			value, ok := functionChainNumber(weight)
			if !ok || value < 0 || value > 1 {
				return merr.WrapErrParameterInvalidMsg("merge weights must be finite and in [0, 1]")
			}
		}
	}
	return nil
}

func functionChainNumber(p *schemapb.FunctionParamValue) (float64, bool) {
	var value float64
	switch v := p.GetValue().(type) {
	case *schemapb.FunctionParamValue_Int64Value:
		value = float64(v.Int64Value)
	case *schemapb.FunctionParamValue_DoubleValue:
		value = v.DoubleValue
	default:
		return 0, false
	}
	return value, !math.IsNaN(value) && !math.IsInf(value, 0)
}

func validateQueryChains(chains []*schemapb.FunctionChain, outputFields []string, params map[string]string) error {
	if len(chains) == 0 {
		return nil
	}
	for _, field := range outputFields {
		// Parentheses inside a JSON path key are not an aggregate call.
		prefix, _, _ := strings.Cut(field, "[")
		if strings.Contains(prefix, "(") {
			return merr.WrapErrParameterInvalidMsg("query function chains cannot be used with aggregation or count")
		}
	}
	for _, key := range []string{spOrderByFields, "iterator", "group_by_fields", "group_by_field"} {
		if value := strings.TrimSpace(params[key]); value != "" && value != "false" {
			return merr.WrapErrParameterInvalidMsg("query function chains cannot be used with %s", key)
		}
	}
	pb := chains[0]
	if len(pb.Ops) != 1 || pb.Ops[0].Op != "grep" {
		return merr.WrapErrParameterInvalidMsg("QueryScan currently supports exactly one grep operator")
	}
	op := pb.Ops[0]
	if op.Expr != nil || len(op.Inputs) != 1 || len(op.Outputs) != 1 {
		return merr.WrapErrParameterInvalidMsg("grep requires one input, one output and no expression")
	}
	input, output := op.Inputs[0], op.Outputs[0]
	for _, name := range []string{input, output} {
		if strings.TrimSpace(name) != name || strings.HasPrefix(name, "$") {
			return merr.WrapErrParameterInvalidMsg("invalid grep field name %q", name)
		}
	}
	if input == output {
		return merr.WrapErrParameterInvalidMsg("grep output cannot overwrite input")
	}
	if _, ok := op.Params["arguments"].GetValue().(*schemapb.FunctionParamValue_StringValue); !ok || len(op.Params) != 1 {
		return merr.WrapErrParameterInvalidMsg("grep accepts only a string arguments parameter")
	}
	for _, name := range pb.OutputColumns {
		if name != output {
			return merr.WrapErrParameterInvalidMsg("QueryScan output column %q is not produced by grep", name)
		}
	}
	return nil
}
