/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * # or implied. See the License for the specific language governing
 * # permissions and limitations under the License.
 */

package expr

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const BoostScoreFuncName = "boost_score"

type BoostScoreRunner func(ctx context.Context, offsets *arrow.Chunked) (*arrow.Chunked, error)

type BoostScoreExpr struct {
	BaseExpr
	runner BoostScoreRunner
}

func NewBoostScoreExpr(runner BoostScoreRunner) (*BoostScoreExpr, error) {
	if runner == nil {
		return nil, merr.WrapErrParameterInvalidMsg("boost_score: runner is nil")
	}
	return &BoostScoreExpr{
		BaseExpr: *NewBaseExpr(BoostScoreFuncName, []string{types.StageL0Rerank}),
		runner:   runner,
	}, nil
}

func (e *BoostScoreExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

func (e *BoostScoreExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if len(inputs) != 1 {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: expected 1 input column, got %d", len(inputs)))
	}
	if e.runner == nil {
		return nil, merr.WrapErrServiceInternal("boost_score: runner is nil")
	}

	offsets := inputs[0]
	if offsets == nil {
		return nil, merr.WrapErrServiceInternal("boost_score: offset column is nil")
	}
	if offsets.DataType().ID() != arrow.INT64 {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: offset column must be Int64, got %s", offsets.DataType()))
	}

	scores, err := e.runner(ctx.Context(), offsets)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: %v", err))
	}
	if err := validateBoostScoreOutput(offsets, scores); err != nil {
		if scores != nil {
			scores.Release()
		}
		return nil, err
	}

	return []*arrow.Chunked{scores}, nil
}

func validateBoostScoreOutput(offsets, scores *arrow.Chunked) error {
	if scores == nil {
		return merr.WrapErrServiceInternal("boost_score: runner returned nil scores")
	}
	if scores.DataType().ID() != arrow.FLOAT32 {
		return merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: score column must be Float32, got %s", scores.DataType()))
	}
	if len(scores.Chunks()) != len(offsets.Chunks()) {
		return merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: score chunks %d does not match offset chunks %d", len(scores.Chunks()), len(offsets.Chunks())))
	}
	for i := range offsets.Chunks() {
		if scores.Chunk(i).Len() != offsets.Chunk(i).Len() {
			return merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: score chunk %d length %d does not match offset chunk length %d", i, scores.Chunk(i).Len(), offsets.Chunk(i).Len()))
		}
	}
	return nil
}
