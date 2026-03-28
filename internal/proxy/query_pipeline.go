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

package proxy

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	typeutil2 "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Channel names for query pipeline data flow
const (
	chanInput   = queryutil.PipelineInput  // []*internalpb.RetrieveResults
	chanReduced = "reduced"                // *internalpb.RetrieveResults (after reduce)
	chanSorted  = "sorted"                 // *internalpb.RetrieveResults (after order/merge)
	chanSliced  = "sliced"                 // *internalpb.RetrieveResults (after slice)
	chanOutput  = queryutil.PipelineOutput // *internalpb.RetrieveResults
)

//=============================================================================
// QueryPipeline - Pipeline for query result processing at proxy level
//=============================================================================

// QueryPipeline processes query results through a composable pipeline.
//
// Pipeline patterns (proxy-side):
//
// Plain query:
//
//	input -> [sort_and_check_pk] -> [slice] -> [complement_fields] -> output
//
// ORDER BY (no aggregation):
//
//	input -> [concat_and_check_pk] -> [order] -> [slice] -> [remap] -> [complement_fields] -> output
//
// GROUP BY (no ORDER BY):
//
//	input -> [reduce_by_groups] -> [slice] -> output
//
// GROUP BY + ORDER BY:
//
//	input -> [reduce_by_groups(raw)] -> [order] -> [slice] -> [agg_remap] -> output
type QueryPipeline struct {
	pipeline       *queryutil.Pipeline
	schema         *schemapb.CollectionSchema
	outputFieldIDs []int64
}

// NewQueryPipeline dispatches to the appropriate pipeline builder based on
// query configuration. Each builder constructs a self-contained pipeline.
func NewQueryPipeline(
	schema *schemapb.CollectionSchema,
	limit, offset int64,
	reduceType reduce.IReduceType,
	orderByFields []*orderby.OrderByField,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
	outputMap *agg.AggregationFieldMap,
	outputFieldIDs []int64,
) (*QueryPipeline, error) {
	hasAggregation := len(groupByFieldIDs) > 0 || len(aggregates) > 0
	hasOrderBy := len(orderByFields) > 0

	var p *queryutil.Pipeline
	var err error
	if hasAggregation && hasOrderBy {
		p, err = buildGroupByOrderByPipeline(schema, limit, offset, orderByFields, groupByFieldIDs, aggregates, outputMap)
	} else if hasAggregation {
		p = buildGroupByPipeline(schema, limit, offset, groupByFieldIDs, aggregates, outputMap)
	} else if hasOrderBy {
		p = buildOrderByPipeline(schema, limit, offset, orderByFields, outputFieldIDs)
	} else {
		p = buildPlainQueryPipeline(schema, limit, offset, reduceType)
	}
	if err != nil {
		return nil, err
	}

	return &QueryPipeline{
		pipeline:       p,
		schema:         schema,
		outputFieldIDs: outputFieldIDs,
	}, nil
}

// buildPlainQueryPipeline: sort_and_check_pk -> slice -> complement_fields -> output
func buildPlainQueryPipeline(
	schema *schemapb.CollectionSchema,
	limit, offset int64,
	reduceType reduce.IReduceType,
) *queryutil.Pipeline {
	b := queryutil.NewPipelineBuilder("proxy-query-plain")
	b.Add(queryutil.OpReduceByPK, in(), ch(chanReduced), queryutil.NewSortAndCheckPKOperator(reduceType))
	b.Add(queryutil.OpSlice, ch(chanReduced), ch(chanSliced), queryutil.NewSliceOperator(limit, offset))
	b.Add("complement_fields", ch(chanSliced), out(), newComplementFieldOperator(schema))
	return b.Build()
}

// buildOrderByPipeline: concat_and_check_pk -> order -> slice -> remap -> complement_fields -> output
func buildOrderByPipeline(
	schema *schemapb.CollectionSchema,
	limit, offset int64,
	orderByFields []*orderby.OrderByField,
	outputFieldIDs []int64,
) *queryutil.Pipeline {
	b := queryutil.NewPipelineBuilder("proxy-query-orderby")
	b.Add(queryutil.OpConcatAndCheckPK, in(), ch(chanReduced), queryutil.NewConcatAndCheckPKOperator())
	b.Add(queryutil.OpOrderByLimit, ch(chanReduced), ch(chanSorted), queryutil.NewOrderByLimitOperator(orderByFields, offset+limit))
	b.Add(queryutil.OpSlice, ch(chanSorted), ch(chanSliced), queryutil.NewSliceOperator(limit, offset))

	// Remap by FieldID: select and reorder fields to match user's output_fields.
	// Uses FieldID matching instead of name matching to correctly handle dynamic
	// field subkeys (e.g., user requests "x" which maps to $meta's FieldID).
	b.Add(queryutil.OpRemap, ch(chanSliced), ch("remapped"), queryutil.NewFieldIDRemapOperator(outputFieldIDs))
	b.Add("complement_fields", ch("remapped"), out(), newComplementFieldOperator(schema))
	return b.Build()
}

// buildGroupByPipeline: reduce_by_groups -> slice -> output
func buildGroupByPipeline(
	schema *schemapb.CollectionSchema,
	limit, offset int64,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
	outputMap *agg.AggregationFieldMap,
) *queryutil.Pipeline {
	b := queryutil.NewPipelineBuilder("proxy-query-groupby")
	b.Add(queryutil.OpReduceByGroups, in(), ch(chanReduced), newReduceByGroupsOperator(schema, groupByFieldIDs, aggregates, outputMap))
	b.Add(queryutil.OpSlice, ch(chanReduced), out(), queryutil.NewSliceOperator(limit, offset))
	return b.Build()
}

// buildGroupByOrderByPipeline: reduce_by_groups(raw) -> order -> slice -> agg_remap -> output
func buildGroupByOrderByPipeline(
	schema *schemapb.CollectionSchema,
	limit, offset int64,
	orderByFields []*orderby.OrderByField,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
	outputMap *agg.AggregationFieldMap,
) (*queryutil.Pipeline, error) {
	// Positions based on reducer raw layout [group_cols..., agg_cols...]
	positions, err := queryutil.ComputeGroupByOrderPositions(orderByFields, groupByFieldIDs, aggregates)
	if err != nil {
		return nil, err
	}

	b := queryutil.NewPipelineBuilder("proxy-query-groupby-orderby")
	b.Add(queryutil.OpReduceByGroups, in(), ch(chanReduced), newRawReduceByGroupsOperator(schema, groupByFieldIDs, aggregates))
	b.Add(queryutil.OpOrderByLimit, ch(chanReduced), ch(chanSorted), queryutil.NewOrderByLimitOperatorWithPositions(orderByFields, positions, offset+limit))
	b.Add(queryutil.OpSlice, ch(chanSorted), ch(chanSliced), queryutil.NewSliceOperator(limit, offset))
	b.Add(queryutil.OpRemap, ch(chanSliced), out(), newAggRemapOperator(outputMap))
	return b.Build(), nil
}

// Channel helper functions for readability.
func in() []string            { return []string{chanInput} }
func out() []string           { return []string{chanOutput} }
func ch(name string) []string { return []string{name} }

// Execute runs the pipeline on input results.
func (p *QueryPipeline) Execute(ctx context.Context, results []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	_, span := otel.Tracer(typeutil.ProxyRole).Start(ctx, "QueryPipeline.Execute")
	defer span.End()

	msg := queryutil.OpMsg{chanInput: results}

	finalMsg, err := p.pipeline.Run(ctx, span, msg)
	if err != nil {
		return nil, err
	}

	output := finalMsg[chanOutput].(*internalpb.RetrieveResults)
	result := &milvuspb.QueryResults{
		Status:     merr.Success(),
		FieldsData: output.GetFieldsData(),
	}

	// Fill empty field data when result has no rows, so pymilvus gets proper field schema.
	if err := typeutil2.FillRetrieveResultIfEmpty(typeutil2.NewMilvusResult(result), p.outputFieldIDs, p.schema); err != nil {
		return nil, err
	}

	return result, nil
}

//=============================================================================
// Operators
//=============================================================================

// newComplementFieldOperator sets FieldName/Type/IsDynamic from schema and
// drops the internal timestamp column. Used for non-aggregation queries.
func newComplementFieldOperator(schema *schemapb.CollectionSchema) queryutil.Operator {
	return queryutil.NewLambdaOperator("complement_fields", func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		result := inputs[0].(*internalpb.RetrieveResults)
		if result == nil {
			return []any{result}, nil
		}

		for _, fd := range result.GetFieldsData() {
			if fd == nil {
				continue
			}
			field := typeutil.GetField(schema, fd.GetFieldId())
			if field != nil {
				fd.FieldName = field.GetName()
				fd.Type = field.GetDataType()
				fd.IsDynamic = field.GetIsDynamic()
			}
		}

		// Drop internal timestamp column (FieldID=1).
		for i := 0; i < len(result.FieldsData); i++ {
			if result.FieldsData[i] != nil && result.FieldsData[i].FieldId == common.TimeStampField {
				result.FieldsData = append(result.FieldsData[:i], result.FieldsData[i+1:]...)
				i--
			}
		}

		return []any{result}, nil
	})
}

// newReduceByGroupsOperator aggregates results and reorganizes output by
// outputMap to match the user's output_fields order.
// Used for GROUP BY queries without ORDER BY.
func newReduceByGroupsOperator(
	schema *schemapb.CollectionSchema,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
	outputMap *agg.AggregationFieldMap,
) queryutil.Operator {
	return queryutil.NewLambdaOperator(queryutil.OpReduceByGroups, func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		results := inputs[0].([]*internalpb.RetrieveResults)

		reducer := agg.NewGroupAggReducer(groupByFieldIDs, aggregates, -1, schema)
		reducedRes, err := reducer.Reduce(ctx, agg.InternalResult2AggResult(results))
		if err != nil {
			return nil, err
		}

		reducedFieldDatas := reducedRes.GetFieldDatas()
		fieldCount := outputMap.Count()
		reOrganizedFieldDatas := make([]*schemapb.FieldData, fieldCount)

		for i := 0; i < fieldCount; i++ {
			indices := outputMap.IndexesAt(i)
			if len(indices) == 0 {
				return nil, fmt.Errorf("no indices found for output field '%s'", outputMap.NameAt(i))
			} else if len(indices) == 1 {
				reOrganizedFieldDatas[i] = reducedFieldDatas[indices[0]]
				reOrganizedFieldDatas[i].FieldName = outputMap.NameAt(i)
			} else if len(indices) == 2 {
				sumFieldData := reducedFieldDatas[indices[0]]
				countFieldData := reducedFieldDatas[indices[1]]
				avgFieldData, err := agg.ComputeAvgFromSumAndCount(sumFieldData, countFieldData)
				if err != nil {
					return nil, err
				}
				avgFieldData.FieldName = outputMap.NameAt(i)
				reOrganizedFieldDatas[i] = avgFieldData
			}
		}

		return []any{&internalpb.RetrieveResults{
			FieldsData: reOrganizedFieldDatas,
		}}, nil
	})
}

// newRawReduceByGroupsOperator aggregates results and outputs the
// GroupAggReducer's raw layout [group_cols..., agg_cols...] without
// reorganization. Used for GROUP BY + ORDER BY where downstream operators
// need predictable field positions.
func newRawReduceByGroupsOperator(
	schema *schemapb.CollectionSchema,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
) queryutil.Operator {
	return queryutil.NewLambdaOperator(queryutil.OpReduceByGroups, func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		results := inputs[0].([]*internalpb.RetrieveResults)

		reducer := agg.NewGroupAggReducer(groupByFieldIDs, aggregates, -1, schema)
		reducedRes, err := reducer.Reduce(ctx, agg.InternalResult2AggResult(results))
		if err != nil {
			return nil, err
		}

		return []any{&internalpb.RetrieveResults{
			FieldsData: reducedRes.GetFieldDatas(),
		}}, nil
	})
}

// newAggRemapOperator reorganizes fields from the GroupAggReducer's raw layout
// to the user's output_fields order, computing avg from sum+count where needed.
// Used after ORDER BY + slice in the GROUP BY + ORDER BY pipeline.
func newAggRemapOperator(outputMap *agg.AggregationFieldMap) queryutil.Operator {
	return queryutil.NewLambdaOperator(queryutil.OpRemap, func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		result := inputs[0].(*internalpb.RetrieveResults)
		if result == nil || len(result.GetFieldsData()) == 0 {
			return []any{result}, nil
		}

		rawFields := result.GetFieldsData()
		fieldCount := outputMap.Count()
		remapped := make([]*schemapb.FieldData, fieldCount)

		for i := 0; i < fieldCount; i++ {
			indices := outputMap.IndexesAt(i)
			if len(indices) == 0 {
				return nil, fmt.Errorf("no indices found for output field '%s'", outputMap.NameAt(i))
			} else if len(indices) == 1 {
				remapped[i] = rawFields[indices[0]]
				remapped[i].FieldName = outputMap.NameAt(i)
			} else if len(indices) == 2 {
				avgFieldData, err := agg.ComputeAvgFromSumAndCount(rawFields[indices[0]], rawFields[indices[1]])
				if err != nil {
					return nil, err
				}
				avgFieldData.FieldName = outputMap.NameAt(i)
				remapped[i] = avgFieldData
			}
		}

		return []any{&internalpb.RetrieveResults{
			FieldsData: remapped,
		}}, nil
	})
}
