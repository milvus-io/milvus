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

	"github.com/apache/arrow/go/v17/arrow"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
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

var boostScoreRunnerFactory = newSegmentBoostScoreRunner

type boostScoreFunc func(context.Context, segments.Segment, *segcore.SearchRequest, *planpb.ScoreFunction, *arrow.Chunked) (*arrow.Chunked, error)

func newSegmentBoostScoreRunner(scoreFunc boostScoreFunc, segment segments.Segment, searchReq *segcore.SearchRequest, scorer *planpb.ScoreFunction) expr.BoostScoreRunner {
	return func(ctx context.Context, offsets *arrow.Chunked) (*arrow.Chunked, error) {
		return scoreFunc(ctx, segment, searchReq, scorer, offsets)
	}
}

func buildBoostScoreChain(
	df *chain.DataFrame,
	segment segments.Segment,
	searchReq *segcore.SearchRequest,
	scorers []*planpb.ScoreFunction,
	scoreFunc boostScoreFunc,
	functionMode string,
	boostMode string,
) (*chain.FuncChain, error) {
	boostChain := chain.NewFuncChainWithAllocator(defaultAllocator).
		SetName("l0-rerank").
		SetStage(types.StageL0Rerank)

	boostScoreColumns, err := appendBoostScoreColumns(boostChain, segment, searchReq, scorers, scoreFunc)
	if err != nil {
		return nil, err
	}

	functionScoreCol, err := appendFunctionScoreColumn(boostChain, boostScoreColumns, functionMode)
	if err != nil {
		return nil, err
	}

	if err := appendFinalBoostScore(boostChain, functionScoreCol, boostMode); err != nil {
		return nil, err
	}

	return appendL0RerankReduceContract(boostChain), nil
}

func appendBoostScoreColumns(
	boostChain *chain.FuncChain,
	segment segments.Segment,
	searchReq *segcore.SearchRequest,
	scorers []*planpb.ScoreFunction,
	scoreFunc boostScoreFunc,
) ([]string, error) {
	boostScoreColumns := make([]string, 0, len(scorers))
	for scorerIdx, scorer := range scorers {
		outputCol := boostScoreColumn(scorerIdx)
		boostExpr, err := expr.NewBoostScoreExpr(boostScoreRunnerFactory(scoreFunc, segment, searchReq, scorer))
		if err != nil {
			return nil, err
		}
		boostChain.Map(boostExpr, []string{types.SegOffsetFieldName}, []string{outputCol})
		boostScoreColumns = append(boostScoreColumns, outputCol)
	}
	return boostScoreColumns, nil
}

func appendFunctionScoreColumn(boostChain *chain.FuncChain, boostScoreColumns []string, functionMode string) (string, error) {
	if len(boostScoreColumns) == 1 {
		return boostScoreColumns[0], nil
	}

	functionCombineExpr, err := expr.NewNumCombineExpr(functionMode, nil, expr.WithNullPolicy(expr.NumCombineNullSkip))
	if err != nil {
		return "", err
	}
	boostChain.Map(functionCombineExpr, boostScoreColumns, []string{functionScoreColumn})
	return functionScoreColumn, nil
}

func appendFinalBoostScore(boostChain *chain.FuncChain, functionScoreCol string, boostMode string) error {
	finalCombineExpr, err := expr.NewNumCombineExpr(boostMode, nil, expr.WithNullPolicy(expr.NumCombineNullSkip))
	if err != nil {
		return err
	}
	boostChain.Map(finalCombineExpr,
		[]string{types.ScoreFieldName, functionScoreCol},
		[]string{types.ScoreFieldName})
	return nil
}

func (t *SearchTask) applyBoostScores(segDFs []*chain.DataFrame, searchedSegments []segments.Segment, searchReq *segcore.SearchRequest) error {
	plan, err := extractPlanWithScorers(t.req.GetReq().GetSerializedExprPlan())
	if err != nil {
		return merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: failed to parse search plan scorers: %v", err))
	}
	return t.applyBoostScoresWithPlan(segDFs, plan, searchedSegments, searchReq)
}

func (t *SearchTask) applyBoostScoresWithPlan(segDFs []*chain.DataFrame, plan *planpb.PlanNode, searchedSegments []segments.Segment, searchReq *segcore.SearchRequest) error {
	if len(segDFs) != len(searchedSegments) {
		return merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: DataFrame count %d does not match segment count %d", len(segDFs), len(searchedSegments)))
	}
	if plan == nil || len(plan.GetScorers()) == 0 {
		return nil
	}

	scorers := plan.GetScorers()
	functionMode, err := functionModeToScoreCombineMode(plan.GetScoreOption().GetFunctionMode())
	if err != nil {
		return err
	}
	boostMode, err := boostModeToScoreCombineMode(plan.GetScoreOption().GetBoostMode())
	if err != nil {
		return err
	}

	scoreFunc := segments.AsyncComputeScorerScoresOnChunkedOffsets
	return executeL0RerankChains(t.ctx, segDFs, func(_ context.Context, i int, df *chain.DataFrame) (*chain.FuncChain, error) {
		return buildBoostScoreChain(df, searchedSegments[i], searchReq, scorers, scoreFunc, functionMode, boostMode)
	}, "boost_score")
}
