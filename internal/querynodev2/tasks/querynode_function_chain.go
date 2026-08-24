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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type preparedQueryNodeFunctionChains struct {
	l0 *preparedL0Rerank
	l1 *preparedL1FunctionChain
}

type preparedL0Rerank struct {
	chain         *chain.ChainRepr
	inputFieldIDs []int64
	boostScore    *preparedBoostScore
}

type preparedL1FunctionChain struct {
	chain         *chain.ChainRepr
	inputFieldIDs []int64
}

func prepareQueryNodeFunctionChains(serializedPlan []byte, schema *schemapb.CollectionSchema) (*preparedQueryNodeFunctionChains, error) {
	plan, err := extractPlanWithScorers(serializedPlan)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "querynode function chain: failed to parse search plan")
	}
	return prepareQueryNodeFunctionChainsFromPlan(plan, schema)
}

func prepareQueryNodeFunctionChainsFromPlan(plan *planpb.PlanNode, schema *schemapb.CollectionSchema) (*preparedQueryNodeFunctionChains, error) {
	prepared := &preparedQueryNodeFunctionChains{}
	if plan == nil {
		return prepared, nil
	}
	if len(plan.GetScorers()) > 0 {
		if len(plan.GetQuerynodeFunctionChains()) > 0 {
			return nil, merr.WrapErrParameterInvalidMsg("boost score and querynode rerank function chains cannot be used together")
		}
		boostScore, err := prepareBoostScore(plan)
		if err != nil {
			return nil, err
		}
		prepared.l0 = &preparedL0Rerank{boostScore: boostScore}
		return prepared, nil
	}
	if len(plan.GetQuerynodeFunctionChains()) == 0 {
		return prepared, nil
	}

	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "querynode function chain: failed to create schema helper")
	}

	seenStages := make(map[schemapb.FunctionChainStage]struct{}, len(plan.GetQuerynodeFunctionChains()))
	for i, chainPB := range plan.GetQuerynodeFunctionChains() {
		if chainPB == nil {
			return nil, merr.WrapErrParameterInvalidMsg("querynode function chain[%d] is nil", i)
		}
		stage := chainPB.GetStage()
		if _, ok := seenStages[stage]; ok {
			return nil, merr.WrapErrParameterInvalidMsg("querynode function chain stage %s appears more than once", stage.String())
		}
		seenStages[stage] = struct{}{}

		if len(chainPB.GetOps()) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("querynode function chain[%d] must contain at least one op", i)
		}

		repr, err := chain.ProtoChainToRepr(chainPB)
		if err != nil {
			return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
		}

		switch stage {
		case schemapb.FunctionChainStage_FunctionChainStageL0Rerank:
			if err := validateL0FunctionChainOps(repr); err != nil {
				return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
			}
			if err := validateL0FunctionChainSystemOutputs(repr); err != nil {
				return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
			}
			inputFieldIDs, err := planQueryNodeFunctionChainInputs(repr, schemaHelper, stage)
			if err != nil {
				return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
			}
			prepared.l0 = &preparedL0Rerank{
				chain:         repr,
				inputFieldIDs: inputFieldIDs,
			}
		case schemapb.FunctionChainStage_FunctionChainStageL1Rerank:
			if err := validateL1FunctionChain(repr); err != nil {
				return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
			}
			inputFieldIDs, err := planQueryNodeFunctionChainInputs(repr, schemaHelper, stage)
			if err != nil {
				return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
			}
			prepared.l1 = &preparedL1FunctionChain{
				chain:         repr,
				inputFieldIDs: inputFieldIDs,
			}
		default:
			return nil, merr.WrapErrParameterInvalidMsg("querynode function chain[%d] stage %s is not supported", i, stage.String())
		}
	}
	return prepared, nil
}

func validateQueryNodeFunctionChainSystemOutputs(repr *chain.ChainRepr, level string) error {
	if repr == nil {
		return merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	for opIdx, op := range repr.Info.Ops {
		for _, output := range op.WriteNames {
			if !chain.IsFunctionChainSystemName(output) {
				continue
			}
			if output != chaintypes.ScoreFieldName {
				return merr.WrapErrParameterInvalidMsg("op[%d] system output %q is not writable by %s rerank function chain", opIdx, output, level)
			}
		}
	}
	return nil
}

func planQueryNodeFunctionChainInputs(
	repr *chain.ChainRepr,
	schemaHelper *typeutil.SchemaHelper,
	stage schemapb.FunctionChainStage,
) ([]int64, error) {
	if repr == nil {
		return nil, merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}

	level := "L0"
	if stage == schemapb.FunctionChainStage_FunctionChainStageL1Rerank {
		level = "L1"
	}

	inputFieldIDs := make([]int64, 0)
	seenInputFields := make(map[string]struct{})
	for _, input := range repr.Info.RequiredInputs {
		if chain.IsFunctionChainSystemName(input) {
			if !isReadableQueryNodeSystemInput(input) {
				return nil, merr.WrapErrParameterInvalidMsg("system input %q is not readable by %s rerank function chain", input, level)
			}
			continue
		}
		if _, ok := seenInputFields[input]; ok {
			continue
		}

		field, err := schemaHelper.GetFieldFromName(input)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("function chain input %q is neither a previous output nor a collection field", input)
		}
		if _, err := chain.ToArrowType(field.GetDataType()); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("function chain input %q has unsupported field type %s", input, field.GetDataType().String())
		}

		seenInputFields[input] = struct{}{}
		inputFieldIDs = append(inputFieldIDs, field.GetFieldID())
	}
	return inputFieldIDs, nil
}

func isReadableQueryNodeSystemInput(input string) bool {
	switch input {
	case chaintypes.IDFieldName, chaintypes.ScoreFieldName:
		return true
	default:
		return false
	}
}
