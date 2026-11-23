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
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	boostScoreColumnPrefix = "boost_score_"
	functionScoreColumn    = "function_score"
)

func boostScoreColumn(index int) string {
	return fmt.Sprintf("%s%d", boostScoreColumnPrefix, index)
}

func extractPlanScorers(serializedPlan []byte) ([]*planpb.ScoreFunction, error) {
	plan, err := extractPlanWithScorers(serializedPlan)
	if err != nil || plan == nil {
		return nil, err
	}
	return plan.GetScorers(), nil
}

func extractPlanWithScorers(serializedPlan []byte) (*planpb.PlanNode, error) {
	if len(serializedPlan) == 0 {
		return nil, nil
	}

	plan := &planpb.PlanNode{}
	if err := proto.Unmarshal(serializedPlan, plan); err != nil {
		return nil, err
	}
	return plan, nil
}

func functionModeToScoreCombineMode(mode planpb.FunctionMode) (string, error) {
	switch mode {
	case planpb.FunctionMode_FunctionModeMultiply:
		return expr.ModeMultiply, nil
	case planpb.FunctionMode_FunctionModeSum:
		return expr.ModeSum, nil
	default:
		return "", merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: unknown function mode %s", mode.String()))
	}
}

func boostModeToScoreCombineMode(mode planpb.BoostMode) (string, error) {
	switch mode {
	case planpb.BoostMode_BoostModeMultiply:
		return expr.ModeMultiply, nil
	case planpb.BoostMode_BoostModeSum:
		return expr.ModeSum, nil
	default:
		return "", merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: unknown boost mode %s", mode.String()))
	}
}

// prepareBoostScore is the legacy request adapter. After this conversion the
// worker retains only a ChainRepr and uses the ordinary L0 execution path.
func prepareBoostScore(plan *planpb.PlanNode) (*chain.ChainRepr, error) {
	if plan == nil || len(plan.GetScorers()) == 0 {
		return nil, nil
	}
	functionMode, err := functionModeToScoreCombineMode(plan.GetScoreOption().GetFunctionMode())
	if err != nil {
		return nil, err
	}
	boostMode, err := boostModeToScoreCombineMode(plan.GetScoreOption().GetBoostMode())
	if err != nil {
		return nil, err
	}
	return legacyBoostChainRepr(plan.GetScorers(), functionMode, boostMode)
}

func legacyBoostChainRepr(scorers []*planpb.ScoreFunction, functionMode, boostMode string) (*chain.ChainRepr, error) {
	repr := &chain.ChainRepr{Name: "l0-rerank", Stage: types.StageL0Rerank}
	columns := make([]string, 0, len(scorers))
	for i, scorer := range scorers {
		if scorer == nil {
			return nil, merr.WrapErrServiceInternal("boost_score: compiled scorer is nil")
		}
		prepared := proto.Clone(scorer).(*planpb.ScoreFunction)
		output := boostScoreColumn(i)
		repr.Operators = append(repr.Operators, chain.OperatorRepr{
			Type:     types.OpTypeMap,
			Function: &chain.FunctionRepr{Name: expr.BoostScoreFuncName, Prepared: prepared},
			Outputs:  []string{output},
		})
		columns = append(columns, output)
	}
	if len(columns) == 0 {
		return nil, merr.WrapErrServiceInternal("boost_score: no compiled scorers")
	}
	appendCombine := func(inputs []string, output, mode string) error {
		args := make([]*schemapb.FunctionChainExprArg, 0, len(inputs))
		for _, input := range inputs {
			args = append(args, &schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: input}}})
		}
		fn := &chain.FunctionRepr{Name: expr.NumCombineFuncName, Args: args, Params: map[string]*schemapb.FunctionParamValue{
			types.NumCombineParamMode:       {Value: &schemapb.FunctionParamValue_StringValue{StringValue: mode}},
			types.NumCombineParamNullPolicy: {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "skip"}},
		}}
		if _, err := chain.FunctionFromReprWithContext(fn, types.FunctionBuildContext{}); err != nil {
			return err
		}
		repr.Operators = append(repr.Operators, chain.OperatorRepr{Type: types.OpTypeMap, Function: fn, Inputs: inputs, Outputs: []string{output}})
		return nil
	}
	combined := columns[0]
	if len(columns) > 1 {
		if err := appendCombine(columns, functionScoreColumn, functionMode); err != nil {
			return nil, err
		}
		combined = functionScoreColumn
	}
	if err := appendCombine([]string{types.ScoreFieldName, combined}, types.ScoreFieldName, boostMode); err != nil {
		return nil, err
	}
	if err := repr.RefreshInfo(); err != nil {
		return nil, err
	}
	return repr, nil
}
