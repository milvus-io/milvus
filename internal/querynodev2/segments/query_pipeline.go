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

package segments

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/internal/util/segcore"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RunQNQueryPipeline is the single entry point for all QN-level query reduction.
// It replaces the reducer factory pattern with direct pipeline execution.
//
// Pipeline patterns at QN level:
//
//	IgnoreNonPk=true (plain query, multi-segment):
//	  [MergeByPKWithOffsets(topK)] → [FetchFieldsData] → output
//
//	Plain / ORDER BY / GROUP BY / GROUP BY+ORDER BY:
//	  Uses BuildQueryReducePipeline (see pipeline_builders.go)
func RunQNQueryPipeline(
	ctx context.Context,
	req *querypb.QueryRequest,
	schema *schemapb.CollectionSchema,
	plan *planpb.PlanNode,
	segcoreResults []*segcorepb.RetrieveResults,
	segments []Segment,
	manager *Manager,
	retrievePlan *segcore.RetrievePlan,
) (*segcorepb.RetrieveResults, error) {
	// Early empty check before any pipeline construction
	if allSegcoreResultsEmpty(segcoreResults) {
		return emptySegcoreResult(req, schema)
	}

	// Build pipeline + input msg — the only branching point
	var pipeline *queryutil.Pipeline
	var msg queryutil.OpMsg
	var err error

	if retrievePlan.IsIgnoreNonPk() {
		pipeline, msg, err = buildIgnoreNonPkPipeline(req, segcoreResults, segments, manager, retrievePlan)
	} else {
		pipeline, msg, err = buildStandardQNPipeline(req, schema, plan, segcoreResults)
	}
	if err != nil {
		return nil, err
	}

	// Common: run pipeline
	finalMsg, err := pipeline.Run(ctx, nil, msg)
	if err != nil {
		return nil, fmt.Errorf("QN query pipeline failed: %w", err)
	}

	// Common: extract output + aggregate stats + fill empty fields
	return extractSegcoreResult(finalMsg, segcoreResults, req, schema)
}

// buildIgnoreNonPkPipeline builds the two-phase pipeline for IgnoreNonPk=true:
// [MergeByPKWithOffsets(topK)] → [FetchFieldsData] → output
func buildIgnoreNonPkPipeline(
	req *querypb.QueryRequest,
	segcoreResults []*segcorepb.RetrieveResults,
	segments []Segment,
	manager *Manager,
	retrievePlan *segcore.RetrievePlan,
) (*queryutil.Pipeline, queryutil.OpMsg, error) {
	topK := req.GetReq().GetLimit()
	reduceType := reduce.ToReduceType(req.GetReq().GetReduceType())

	// Filter valid results (with offsets and non-zero PKs) and corresponding segments
	validResults := make([]*segcorepb.RetrieveResults, 0, len(segcoreResults))
	validSegments := make([]Segment, 0, len(segments))
	for i, r := range segcoreResults {
		size := typeutil.GetSizeOfIDs(r.GetIds())
		if r == nil || len(r.GetOffset()) == 0 || size == 0 {
			continue
		}
		validResults = append(validResults, r)
		validSegments = append(validSegments, segments[i])
	}

	builder := queryutil.NewPipelineBuilder(queryutil.PipelineNameQNIgnoreNonPk)
	builder.Add(queryutil.OpMergeByPKOffsets,
		[]string{queryutil.PipelineInput},
		[]string{queryutil.ChannelMerged},
		NewMergeByPKWithOffsetsOperator(topK, reduceType),
	)
	builder.Add(queryutil.OpFetchFields,
		[]string{queryutil.ChannelMerged},
		[]string{queryutil.PipelineOutput},
		NewFetchFieldsDataOperator(validSegments, manager, retrievePlan),
	)

	msg := queryutil.OpMsg{queryutil.PipelineInput: validResults}
	return builder.Build(), msg, nil
}

// buildStandardQNPipeline builds the pipeline for non-IgnoreNonPk queries
// (plain, ORDER BY, GROUP BY, GROUP BY+ORDER BY).
func buildStandardQNPipeline(
	req *querypb.QueryRequest,
	schema *schemapb.CollectionSchema,
	plan *planpb.PlanNode,
	segcoreResults []*segcorepb.RetrieveResults,
) (*queryutil.Pipeline, queryutil.OpMsg, error) {
	topK := req.GetReq().GetLimit()

	orderByFields, err := orderby.ConvertFromPlanOrderByFields(
		req.GetReq().GetOrderByFields(), schema,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert ORDER BY fields: %w", err)
	}

	// Convert segcore results to internal format.
	// For regular queries, filter by IDs; for aggregation/count, filter by FieldsData.
	hasAggregation := len(req.GetReq().GetGroupByFieldIds()) > 0 || len(req.GetReq().GetAggregates()) > 0
	internalResults := make([]*internalpb.RetrieveResults, 0, len(segcoreResults))
	for _, res := range segcoreResults {
		if res == nil {
			continue
		}
		if hasAggregation {
			if len(res.GetFieldsData()) == 0 {
				continue
			}
		} else {
			if typeutil.GetSizeOfIDs(res.GetIds()) == 0 {
				continue
			}
		}
		internalResults = append(internalResults, &internalpb.RetrieveResults{
			Ids:              res.GetIds(),
			FieldsData:       res.GetFieldsData(),
			HasMoreResult:    res.GetHasMoreResult(),
			AllRetrieveCount: res.GetAllRetrieveCount(),
			ElementLevel:     res.GetElementLevel(),
			ElementIndices:   convertSegcoreElementIndicesToInternal(res.GetElementIndices()),
		})
	}

	reduceType := reduce.ToReduceType(req.GetReq().GetReduceType())
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	pipeline, err := queryutil.BuildQueryReducePipeline(
		queryutil.PipelineNameQN, schema, topK, reduceType,
		orderByFields,
		req.GetReq().GetGroupByFieldIds(),
		req.GetReq().GetAggregates(),
		maxOutputSize,
	)
	if err != nil {
		return nil, nil, err
	}

	msg := queryutil.OpMsg{queryutil.PipelineInput: internalResults}
	return pipeline, msg, nil
}

// extractSegcoreResult extracts the pipeline output (either segcore or internal format),
// aggregates stats from original segcore results, and fills empty fields.
func extractSegcoreResult(
	finalMsg queryutil.OpMsg,
	segcoreResults []*segcorepb.RetrieveResults,
	req *querypb.QueryRequest,
	schema *schemapb.CollectionSchema,
) (*segcorepb.RetrieveResults, error) {
	rawOutput := finalMsg[queryutil.PipelineOutput]

	var ids *schemapb.IDs
	var fieldsData []*schemapb.FieldData
	var elementLevel bool
	var elementIndices []*segcorepb.ElementIndices

	switch output := rawOutput.(type) {
	case *segcorepb.RetrieveResults:
		ids = output.GetIds()
		fieldsData = output.GetFieldsData()
		elementLevel = output.GetElementLevel()
		elementIndices = output.GetElementIndices()
	case *internalpb.RetrieveResults:
		ids = output.GetIds()
		fieldsData = output.GetFieldsData()
		elementLevel = output.GetElementLevel()
		// Convert internalpb.ElementIndices back to segcorepb.ElementIndices
		if elementLevel {
			elementIndices = make([]*segcorepb.ElementIndices, len(output.GetElementIndices()))
			for i, ei := range output.GetElementIndices() {
				if ei != nil {
					elementIndices[i] = &segcorepb.ElementIndices{Indices: ei.GetIndices()}
				}
			}
		}
	default:
		return nil, fmt.Errorf("unexpected pipeline output type: %T", rawOutput)
	}

	merged := &segcorepb.RetrieveResults{
		Ids:                ids,
		FieldsData:         fieldsData,
		ElementLevel:       elementLevel,
		ElementIndices:     elementIndices,
		AllRetrieveCount:   sumInt64Field(segcoreResults, func(r *segcorepb.RetrieveResults) int64 { return r.GetAllRetrieveCount() }),
		HasMoreResult:      anyFieldTrue(segcoreResults, func(r *segcorepb.RetrieveResults) bool { return r.GetHasMoreResult() }),
		ScannedRemoteBytes: sumInt64Field(segcoreResults, func(r *segcorepb.RetrieveResults) int64 { return r.GetScannedRemoteBytes() }),
		ScannedTotalBytes:  sumInt64Field(segcoreResults, func(r *segcorepb.RetrieveResults) int64 { return r.GetScannedTotalBytes() }),
	}

	// Only fill empty fields when result has no data at all.
	// Aggregation results (e.g., count(*)) have FieldsData but no Ids;
	// FillRetrieveResultIfEmpty would incorrectly clear FieldsData via PreHandle()
	// because it checks only Ids to determine emptiness.
	if len(fieldsData) == 0 {
		if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewSegcoreResults(merged), req.GetReq().GetOutputFieldsId(), schema); err != nil {
			return nil, err
		}
	}

	return merged, nil
}

// RunDelegatorQueryPipeline is the single entry point for Delegator-level query reduction.
// Input is already []*internalpb.RetrieveResults from multiple QN workers.
func RunDelegatorQueryPipeline(
	ctx context.Context,
	req *querypb.QueryRequest,
	schema *schemapb.CollectionSchema,
	results []*internalpb.RetrieveResults,
) (*internalpb.RetrieveResults, error) {
	topK := req.GetReq().GetLimit()

	// Read ORDER BY fields directly from RetrieveRequest (populated by proxy),
	// avoiding expensive proto.Unmarshal of the full plan on the hot path.
	orderByFields, err := orderby.ConvertFromPlanOrderByFields(
		req.GetReq().GetOrderByFields(), schema,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to convert ORDER BY fields: %w", err)
	}

	// Build and run pipeline
	reduceType := reduce.ToReduceType(req.GetReq().GetReduceType())
	maxOutputSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	pipeline, err := queryutil.BuildQueryReducePipeline(
		queryutil.PipelineNameDelegator, schema, topK, reduceType,
		orderByFields,
		req.GetReq().GetGroupByFieldIds(),
		req.GetReq().GetAggregates(),
		maxOutputSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to build delegator query pipeline: %w", err)
	}

	msg := queryutil.OpMsg{queryutil.PipelineInput: results}
	finalMsg, err := pipeline.Run(ctx, nil, msg)
	if err != nil {
		return nil, fmt.Errorf("delegator query pipeline failed: %w", err)
	}

	output := finalMsg[queryutil.PipelineOutput].(*internalpb.RetrieveResults)

	// Preserve aggregated stats from inputs
	output.AllRetrieveCount = sumInt64FieldInternal(results, func(r *internalpb.RetrieveResults) int64 { return r.GetAllRetrieveCount() })
	output.HasMoreResult = anyFieldTrueInternal(results, func(r *internalpb.RetrieveResults) bool { return r.GetHasMoreResult() })
	output.ScannedRemoteBytes = sumInt64FieldInternal(results, func(r *internalpb.RetrieveResults) int64 { return r.GetScannedRemoteBytes() })
	output.ScannedTotalBytes = sumInt64FieldInternal(results, func(r *internalpb.RetrieveResults) int64 { return r.GetScannedTotalBytes() })

	// Only fill empty fields when result has no data at all (same guard as QN level).
	if len(output.GetFieldsData()) == 0 {
		if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewInternalResult(output), req.GetReq().GetOutputFieldsId(), schema); err != nil {
			return nil, err
		}
	}

	return output, nil
}

// allSegcoreResultsEmpty checks if all segcore results have no data.
// A result is non-empty if it has IDs (regular query) or FieldsData (aggregation/count).
func allSegcoreResultsEmpty(results []*segcorepb.RetrieveResults) bool {
	for _, r := range results {
		if r != nil && (typeutil.GetSizeOfIDs(r.GetIds()) > 0 || len(r.GetFieldsData()) > 0) {
			return false
		}
	}
	return true
}

// convertSegcoreElementIndicesToInternal converts segcorepb.ElementIndices to internalpb.ElementIndices.
func convertSegcoreElementIndicesToInternal(src []*segcorepb.ElementIndices) []*internalpb.ElementIndices {
	if src == nil {
		return nil
	}
	dst := make([]*internalpb.ElementIndices, len(src))
	for i, s := range src {
		if s != nil {
			dst[i] = &internalpb.ElementIndices{Indices: s.GetIndices()}
		}
	}
	return dst
}

// emptySegcoreResult creates an empty result with proper field schema.
// For aggregation queries (GROUP BY / count(*) / sum / etc.), it generates
// a semantically correct empty aggregation result (e.g., count=0) via
// GroupAggReducer.EmptyAggResult, because FillRetrieveResultIfEmpty relies
// on OutputFieldsId which is intentionally empty for aggregation queries.
func emptySegcoreResult(req *querypb.QueryRequest, schema *schemapb.CollectionSchema) (*segcorepb.RetrieveResults, error) {
	hasAggregation := len(req.GetReq().GetGroupByFieldIds()) > 0 || len(req.GetReq().GetAggregates()) > 0
	if hasAggregation {
		reducer := agg.NewGroupAggReducer(req.GetReq().GetGroupByFieldIds(), req.GetReq().GetAggregates(), -1, schema)
		emptyAgg, err := reducer.EmptyAggResult()
		if err != nil {
			return nil, err
		}
		return agg.AggResult2segcoreResult(emptyAgg), nil
	}

	empty := &segcorepb.RetrieveResults{}
	if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewSegcoreResults(empty), req.GetReq().GetOutputFieldsId(), schema); err != nil {
		return nil, err
	}
	return empty, nil
}
