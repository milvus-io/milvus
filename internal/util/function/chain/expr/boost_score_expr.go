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
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	boostnative "github.com/milvus-io/milvus/internal/util/function/boost/native"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const BoostScoreFuncName = "boost_score"

type BoostScoreExpr struct {
	BaseExpr
	segment boostnative.Segment
	scorer  *planpb.ScoreFunction
}

func (*BoostScoreExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{arrow.PrimitiveTypes.Float32}
}

func (e *BoostScoreExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if len(inputs) != 1 {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: expected 1 input column, got %d", len(inputs)))
	}
	if e.segment == nil {
		return nil, merr.WrapErrServiceInternal("boost_score: segment binding is nil")
	}

	offsets := inputs[0]
	if offsets == nil {
		return nil, merr.WrapErrServiceInternal("boost_score: offset column is nil")
	}
	if offsets.DataType().ID() != arrow.INT64 {
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("boost_score: offset column must be Int64, got %s", offsets.DataType()))
	}

	scores, err := e.segment.ComputeScores(ctx.Context(), offsets, e.scorer)
	if err != nil {
		if scores != nil {
			scores.Release()
		}
		return nil, merr.Wrap(err, "boost_score")
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
		return merr.WrapErrServiceInternal("boost_score: segment returned nil scores")
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

func (*BoostScoreExpr) InternalInputs() []string { return []string{types.SegOffsetFieldName} }

func (*BoostScoreExpr) ValidateArgs(args []*schemapb.FunctionChainExprArg) error {
	if len(args) != 0 {
		return merr.WrapErrParameterInvalidMsg("boost_score: does not accept public arguments")
	}
	return nil
}

func prepareBoostScoreFromParams(ctx types.FunctionBuildContext, cfg types.FunctionConfig) (any, error) {
	// Reject unsupported placement before requiring a schema or compiling filters.
	if ctx.Stage != "" && ctx.Stage != types.StageL0Rerank {
		return nil, merr.WrapErrParameterInvalidMsg("boost_score: only L0 rerank is supported, got %q", ctx.Stage)
	}
	for key := range cfg.Params {
		switch key {
		case "filter", "weight", "random_score":
		default:
			return nil, merr.WrapErrParameterInvalidMsg("boost_score: unknown parameter %q", key)
		}
	}
	reader := types.NewParamReader(BoostScoreFuncName, cfg.Params)
	weight, err := reader.Float64("weight", true, 0)
	if err != nil {
		return nil, err
	}
	if math.IsNaN(weight) || math.IsInf(weight, 0) || math.Abs(weight) > math.MaxFloat32 {
		return nil, merr.WrapErrParameterInvalidMsg("boost_score: weight must be a finite Float32 value")
	}
	params := []*commonpb.KeyValuePair{{Key: "reranker", Value: "boost"}, {Key: "weight", Value: strconv.FormatFloat(weight, 'g', -1, 32)}}
	if _, exists := cfg.Params["filter"]; !exists {
		params = append(params, &commonpb.KeyValuePair{Key: "filter", Value: "true"})
	}
	if _, exists := cfg.Params["filter"]; exists {
		filter, err := reader.String("filter", true)
		if err != nil {
			return nil, err
		}
		params = append(params, &commonpb.KeyValuePair{Key: "filter", Value: filter})
	}
	if _, exists := cfg.Params["random_score"]; exists {
		object, err := reader.Object("random_score", true)
		if err != nil {
			return nil, err
		}
		values := map[string]any{}
		r := types.NewParamReader("boost_score.random_score", object.Fields)
		for key := range object.Fields {
			switch key {
			case "field":
				value, err := r.String(key, true)
				if err != nil {
					return nil, err
				}
				values[key] = value
			case "seed":
				value, err := r.Int64(key, true, 0)
				if err != nil {
					return nil, err
				}
				values[key] = value
			default:
				return nil, merr.WrapErrParameterInvalidMsg("boost_score: unknown random_score parameter %q", key)
			}
		}
		data, err := json.Marshal(values)
		if err != nil {
			return nil, merr.WrapErrServiceInternalMsg("boost_score: cannot encode validated random parameters")
		}
		params = append(params, &commonpb.KeyValuePair{Key: "random_score", Value: string(data)})
	}
	if ctx.Schema == nil {
		return nil, merr.WrapErrServiceInternal("boost_score: schema binding is required")
	}
	scorer, err := planparserv2.CreateSearchScorer(ctx.Schema, &schemapb.FunctionSchema{Params: params}, nil, &planparserv2.ParserVisitorArgs{Timezone: ctx.Timezone})
	if err != nil {
		return nil, merr.Wrap(err, "boost_score: compile scorer")
	}
	return scorer, nil
}

// NewBoostScoreExprFromParams constructs the expression for validation or execution.
// Prepared configuration is required. Request validation can omit the segment;
// Execute requires a runtime binding.
func NewBoostScoreExprFromParams(ctx types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
	if ctx.Stage != "" && ctx.Stage != types.StageL0Rerank {
		return nil, merr.WrapErrParameterInvalidMsg("boost_score: only L0 rerank is supported, got %q", ctx.Stage)
	}
	if len(cfg.Args) != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("boost_score: does not accept public arguments")
	}
	scorer, ok := cfg.Prepared.(*planpb.ScoreFunction)
	if !ok || scorer == nil {
		return nil, merr.WrapErrServiceInternal("boost_score: missing or invalid prepared configuration")
	}
	var segment boostnative.Segment
	if ctx.L0 != nil {
		segment = ctx.L0.Segment
	}
	return &BoostScoreExpr{
		BaseExpr: *NewBaseExpr(BoostScoreFuncName, []string{types.StageL0Rerank}),
		segment:  segment,
		scorer:   scorer,
	}, nil
}

func init() {
	types.MustRegisterFunction(BoostScoreFuncName, types.FunctionDefinition{
		Factory: NewBoostScoreExprFromParams,
		Prepare: prepareBoostScoreFromParams,
	})
}
