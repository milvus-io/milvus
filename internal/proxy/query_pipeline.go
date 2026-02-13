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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
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
	chanSliced  = "sliced"                 // *internalpb.RetrieveResults (after slice, before remap)
	chanOutput  = queryutil.PipelineOutput // *internalpb.RetrieveResults
)

//=============================================================================
// QueryPipeline - Pipeline for query result processing at proxy level
//=============================================================================

// QueryPipeline processes query results through a composable pipeline.
//
// Pipeline patterns (proxy-side):
//
// Simple query (no aggregation, no ORDER BY):
//
//	input -> [reduce_by_pk] -> [slice] -> output
//
// Query with ORDER BY (no aggregation):
//
//	input -> [reduce_by_pk] -> [order] -> [slice] -> [remap] -> output
//
// Query with GROUP BY (aggregation, no ORDER BY):
//
//	input -> [reduce_by_groups] -> [slice] -> output
//
// Query with GROUP BY + ORDER BY:
//
//	input -> [reduce_by_groups] -> [order] -> [slice] -> output
//
// Operators:
//   - reduce_by_pk: Merge by PK with deduplication
//   - reduce_by_groups: Aggregate reduce for GROUP BY
//   - order: Sort a single result by ORDER BY fields
//   - slice: Apply offset/limit (proxy-side only)
//   - remap: Reorder FieldsData from segcore positional layout to user's output order
type QueryPipeline struct {
	pipeline *queryutil.Pipeline
}

// NewQueryPipeline creates a pipeline based on query configuration.
func NewQueryPipeline(
	schema *schemapb.CollectionSchema,
	limit, offset int64,
	orderByFields []*orderby.OrderByField,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
	outputMap *agg.AggregationFieldMap,
	pkFieldName string,
	outputFieldNames []string,
	userOutputFields []string,
) *QueryPipeline {
	hasAggregation := len(groupByFieldIDs) > 0 || len(aggregates) > 0
	hasOrderBy := len(orderByFields) > 0
	needsRemap := hasOrderBy && !hasAggregation

	builder := queryutil.NewPipelineBuilder("proxy-query")

	// Step 1: Add reduce operator (mutually exclusive)
	if hasAggregation {
		builder.Add(queryutil.OpReduceByGroups,
			[]string{chanInput},
			[]string{chanReduced},
			newReduceByGroupsOperator(schema, groupByFieldIDs, aggregates, outputMap),
		)
	} else {
		builder.Add(queryutil.OpReduceByPK,
			[]string{chanInput},
			[]string{chanReduced},
			queryutil.NewReduceByPKOperator(),
		)
	}

	// Step 2: Optionally add order operator
	sliceInput := chanReduced
	if hasOrderBy {
		builder.Add(queryutil.OpOrderBy,
			[]string{chanReduced},
			[]string{chanSorted},
			queryutil.NewOrderOperator(orderByFields, limit, offset),
		)
		sliceInput = chanSorted
	}

	// Step 3: Add slice operator
	sliceOutput := chanOutput
	if needsRemap {
		sliceOutput = chanSliced
	}
	builder.Add(queryutil.OpSlice,
		[]string{sliceInput},
		[]string{sliceOutput},
		queryutil.NewSliceOperator(limit, offset),
	)

	// Step 4: Optionally add remap operator for ORDER BY without aggregation.
	// Remap reorders FieldsData from segcore positional layout [pk, orderby, output]
	// to the user's requested outputFields order.
	if needsRemap {
		orderByFieldNames := make([]string, len(orderByFields))
		for i, f := range orderByFields {
			orderByFieldNames[i] = f.FieldName
		}
		outputIndices := queryutil.BuildRemapIndices(pkFieldName, orderByFieldNames, outputFieldNames, userOutputFields)

		builder.Add(queryutil.OpRemap,
			[]string{chanSliced},
			[]string{chanOutput},
			queryutil.NewRemapOperator(outputIndices),
		)
	}

	return &QueryPipeline{pipeline: builder.Build()}
}

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
	return &milvuspb.QueryResults{
		Status:     merr.Success(),
		FieldsData: output.GetFieldsData(),
	}, nil
}

//=============================================================================
// Operators
//=============================================================================

// newReduceByGroupsOperator creates an operator for GROUP BY aggregation.
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

		fieldCount := outputMap.Count()
		reOrganizedFieldDatas := make([]*schemapb.FieldData, fieldCount)
		reducedFieldDatas := reducedRes.GetFieldDatas()

		for i := 0; i < fieldCount; i++ {
			indices := outputMap.IndexesAt(i)
			if len(indices) == 0 {
				continue
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
