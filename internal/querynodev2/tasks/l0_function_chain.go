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
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type preparedQueryNodeFunctionChains struct {
	plan          *planpb.PlanNode
	l0Chains      []*chain.ChainRepr
	extraFieldIDs []int64
}

func prepareQueryNodeFunctionChains(serializedPlan []byte, schema *schemapb.CollectionSchema) (*preparedQueryNodeFunctionChains, error) {
	plan, err := extractPlanWithScorers(serializedPlan)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "querynode function chain: failed to parse search plan")
	}
	return prepareQueryNodeFunctionChainsFromPlan(plan, schema)
}

func prepareQueryNodeFunctionChainsFromPlan(plan *planpb.PlanNode, schema *schemapb.CollectionSchema) (*preparedQueryNodeFunctionChains, error) {
	prepared := &preparedQueryNodeFunctionChains{plan: plan}
	if plan == nil || len(plan.GetQuerynodeFunctionChains()) == 0 {
		return prepared, nil
	}
	if len(plan.GetScorers()) > 0 {
		return nil, merr.WrapErrParameterInvalidMsg("boost score and L0 rerank function chain cannot be used together")
	}

	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "querynode function chain: failed to create schema helper")
	}

	seenStages := make(map[schemapb.FunctionChainStage]struct{}, len(plan.GetQuerynodeFunctionChains()))
	seenInputFields := make(map[string]struct{})
	for i, chainPB := range plan.GetQuerynodeFunctionChains() {
		if chainPB == nil {
			return nil, merr.WrapErrParameterInvalidMsg("querynode function chain[%d] is nil", i)
		}
		stage := chainPB.GetStage()
		if _, ok := seenStages[stage]; ok {
			return nil, merr.WrapErrParameterInvalidMsg("querynode function chain stage %s appears more than once", stage.String())
		}
		seenStages[stage] = struct{}{}

		if stage != schemapb.FunctionChainStage_FunctionChainStageL0Rerank {
			return nil, merr.WrapErrParameterInvalidMsg("querynode function chain[%d] stage %s is not supported", i, stage.String())
		}
		if len(chainPB.GetOps()) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("querynode function chain[%d] must contain at least one op", i)
		}

		repr, err := chain.ProtoChainToRepr(chainPB)
		if err != nil {
			return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
		}
		if err := validateL0FunctionChainOps(repr); err != nil {
			return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
		}
		if err := validateL0FunctionChainSystemOutputs(repr); err != nil {
			return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
		}

		extraFieldIDs, err := planL0FunctionChainInputs(repr, schemaHelper, seenInputFields)
		if err != nil {
			return nil, merr.Wrapf(err, "querynode function chain[%d]", i)
		}
		prepared.extraFieldIDs = append(prepared.extraFieldIDs, extraFieldIDs...)
		prepared.l0Chains = append(prepared.l0Chains, repr)
	}
	return prepared, nil
}

type l0RerankChainBuilder func(context.Context, int, *chain.DataFrame) (*chain.FuncChain, error)

func appendL0RerankReduceContract(fc *chain.FuncChain) *chain.FuncChain {
	fc.Sort(chaintypes.ScoreFieldName, true, chaintypes.IDFieldName)
	return fc
}

func executeL0RerankChains(ctx context.Context, segDFs []*chain.DataFrame, buildChain l0RerankChainBuilder, errPrefix string) error {
	rerankedDFs := make([]*chain.DataFrame, len(segDFs))
	executeOneSegment := func(ctx context.Context, i int) error {
		df := segDFs[i]
		if df == nil {
			return merr.WrapErrServiceInternal(fmt.Sprintf("%s: DataFrame %d is nil", errPrefix, i))
		}

		fc, err := buildChain(ctx, i, df)
		if err != nil {
			return err
		}
		if fc == nil {
			return merr.WrapErrServiceInternal(fmt.Sprintf("%s: function chain %d is nil", errPrefix, i))
		}

		reranked, err := fc.ExecuteWithOptions(ctx, chain.ExecuteOptions{EnableColumnPruning: true}, df)
		if err != nil {
			return err
		}
		rerankedDFs[i] = reranked
		return nil
	}

	if len(segDFs) == 1 {
		if err := executeOneSegment(ctx, 0); err != nil {
			return err
		}
	} else {
		errGroup, groupCtx := errgroup.WithContext(ctx)
		for i := range segDFs {
			idx := i
			errGroup.Go(func() error {
				return executeOneSegment(groupCtx, idx)
			})
		}
		if err := errGroup.Wait(); err != nil {
			for _, reranked := range rerankedDFs {
				if reranked != nil {
					reranked.Release()
				}
			}
			return err
		}
	}

	for i, reranked := range rerankedDFs {
		segDFs[i].Release()
		segDFs[i] = reranked
	}
	return nil
}

func (t *SearchTask) applyL0Rerank(segDFs []*chain.DataFrame, prepared *preparedQueryNodeFunctionChains, searchedSegments []segments.Segment, searchReq *segcore.SearchRequest) error {
	var plan *planpb.PlanNode
	if prepared != nil {
		if len(prepared.l0Chains) > 0 {
			return t.applyPublicL0Rerank(segDFs, prepared)
		}
		plan = prepared.plan
	}
	return t.applyBoostScoresWithPlan(segDFs, plan, searchedSegments, searchReq)
}

func (t *SearchTask) applyPublicL0Rerank(segDFs []*chain.DataFrame, prepared *preparedQueryNodeFunctionChains) error {
	if len(segDFs) == 0 {
		return nil
	}
	if segDFs[0] == nil {
		return merr.WrapErrServiceInternal("l0_rerank: DataFrame 0 is nil")
	}
	if len(prepared.l0Chains) != 1 {
		return merr.WrapErrServiceInternal(fmt.Sprintf("l0_rerank: expected one L0 function chain, got %d", len(prepared.l0Chains)))
	}

	repr := prepared.l0Chains[0]
	// Public L0 avoids reparsing proto by reusing the prepared ChainRepr, but builds
	// a fresh FuncChain for each segment so operator/function execution state is not
	// shared across concurrent per-segment execution.
	return executeL0RerankChains(t.ctx, segDFs, func(context.Context, int, *chain.DataFrame) (*chain.FuncChain, error) {
		fc, err := chain.FuncChainFromReprWithContext(repr, defaultAllocator, chaintypes.FunctionBuildContext{})
		if err != nil {
			return nil, err
		}
		return appendL0RerankReduceContract(fc), nil
	}, "l0_rerank")
}

func validateL0FunctionChainOps(repr *chain.ChainRepr) error {
	if repr == nil {
		return merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	for opIdx, op := range repr.Operators {
		if op.Type != chaintypes.OpTypeMap {
			return merr.WrapErrParameterInvalidMsg("op[%d] type %q is not supported by L0 rerank function chain", opIdx, op.Type)
		}
	}
	return nil
}

func validateL0FunctionChainSystemOutputs(repr *chain.ChainRepr) error {
	if repr == nil {
		return merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	for opIdx, op := range repr.Info.Ops {
		for _, output := range op.WriteNames {
			if !chain.IsFunctionChainSystemName(output) {
				continue
			}
			if output != chaintypes.ScoreFieldName {
				return merr.WrapErrParameterInvalidMsg("op[%d] system output %q is not writable by L0 rerank function chain", opIdx, output)
			}
		}
	}
	return nil
}

func planL0FunctionChainInputs(repr *chain.ChainRepr, schemaHelper *typeutil.SchemaHelper, seenInputFields map[string]struct{}) ([]int64, error) {
	if repr == nil {
		return nil, merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}

	inputFieldIDs := make([]int64, 0)
	for _, input := range repr.Info.RequiredInputs {
		if chain.IsFunctionChainSystemName(input) {
			if !isReadableL0SystemInput(input) {
				return nil, merr.WrapErrParameterInvalidMsg("system input %q is not readable by L0 rerank function chain", input)
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

func (p *preparedQueryNodeFunctionChains) GetPlan() *planpb.PlanNode {
	if p == nil {
		return nil
	}
	return p.plan
}

func isReadableL0SystemInput(input string) bool {
	switch input {
	case chaintypes.IDFieldName, chaintypes.ScoreFieldName:
		return true
	default:
		return false
	}
}
