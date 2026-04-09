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

package queryutil

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/agg"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// NewDeduplicateByGroupsOperator creates a pipeline operator that performs
// GROUP BY aggregation on multiple RetrieveResults.
//
// Input[0]: []*internalpb.RetrieveResults (from multiple segments/workers)
// Output[0]: *internalpb.RetrieveResults (aggregated result)
//
// Output FieldsData layout: [group_col1, ..., group_colN, agg1, agg2, ...]
// Note: aggregation columns (count/sum/min/max) have FieldId=0 (not set by
// the agg reducer). This is safe because no downstream operator matches on
// FieldId for aggregation columns — GROUP BY+ORDER BY uses explicit position
// mapping via NewOrderByLimitOperatorWithPositions, and proxy's schema override
// in PostExecute skips FieldId=0 (not in schema map).
//
// Unlike proxy's version, this does NOT reorganize output fields (no outputMap).
// groupLimit: -1 for unlimited (when ORDER BY will do topK downstream),
// >0 to truncate groups.
func NewDeduplicateByGroupsOperator(
	schema *schemapb.CollectionSchema,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
	groupLimit int64,
) Operator {
	return NewLambdaOperator(OpReduceByGroups, func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
		results := inputs[0].([]*internalpb.RetrieveResults)

		reducer := agg.NewGroupAggReducer(groupByFieldIDs, aggregates, groupLimit, schema)
		reducedRes, err := reducer.Reduce(ctx, agg.InternalResult2AggResult(results))
		if err != nil {
			return nil, err
		}

		return []any{agg.AggResult2internalResult(reducedRes)}, nil
	})
}
