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
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// BuildQueryReducePipeline builds a unified pipeline for QN/Delegator query reduction.
// It covers all query types by composing operators based on the query configuration:
//
//	Plain:           [ReduceByPK] → output
//	ORDER BY:        [DeduplicatePK] → [OrderByLimit(UserFields, topK)] → output
//	GROUP BY:        [DeduplicateByGroups(limit)] → output
//	GROUP BY+ORDER:  [DeduplicateByGroups(unlimited)] → [OrderByLimit(UserFields, topK)] → output
//
// For IgnoreNonPk=true (QN-only), the caller should handle field data retrieval
// before or after the pipeline. The pipeline operates on whatever data it receives.
//
// topK should be offset+limit (already set by proxy in req.Limit).
func BuildQueryReducePipeline(
	name string,
	schema *schemapb.CollectionSchema,
	topK int64,
	reduceType reduce.IReduceType,
	orderByFields []*orderby.OrderByField,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
) *Pipeline {
	hasGroupBy := len(groupByFieldIDs) > 0 || len(aggregates) > 0
	hasOrderBy := len(orderByFields) > 0

	builder := NewPipelineBuilder(name)

	if hasGroupBy {
		// GROUP BY path: aggregate first, optionally sort
		groupLimit := topK
		if hasOrderBy {
			groupLimit = -1 // ORDER BY needs all groups for sorting
		}

		outputChan := PipelineOutput
		if hasOrderBy {
			outputChan = ChannelReduced
		}

		builder.Add(OpReduceByGroups,
			[]string{PipelineInput},
			[]string{outputChan},
			NewDeduplicateByGroupsOperator(schema, groupByFieldIDs, aggregates, groupLimit),
		)

		if hasOrderBy {
			positions := ComputeGroupByOrderPositions(orderByFields, groupByFieldIDs, aggregates)
			builder.Add(OpOrderByLimit,
				[]string{ChannelReduced},
				[]string{PipelineOutput},
				NewOrderByLimitOperatorWithPositions(orderByFields, positions, topK),
			)
		}
	} else if hasOrderBy {
		// ORDER BY path: concat+dedup (no PK-sort assumption), then sort by user fields
		builder.Add(OpDeduplicatePK,
			[]string{PipelineInput},
			[]string{ChannelDeduped},
			NewDeduplicatePKOperator(),
		)
		builder.Add(OpOrderByLimit,
			[]string{ChannelDeduped},
			[]string{PipelineOutput},
			NewOrderByLimitOperator(orderByFields, topK),
		)
	} else {
		// Plain query path: k-way merge by PK with timestamp dedup
		// (inputs are PK-sorted from segcore; same PK may exist across segments,
		// keep the version with highest timestamp)
		builder.Add(OpReduceByPKTS,
			[]string{PipelineInput},
			[]string{PipelineOutput},
			NewReduceByPKWithTimestampOperator(reduceType),
		)
	}

	return builder.Build()
}

// ComputeGroupByOrderPositions maps ORDER BY fields to their column indices
// in the aggregation output layout: [group_col1, ..., group_colN, agg1, agg2, ...]
func ComputeGroupByOrderPositions(
	orderByFields []*orderby.OrderByField,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
) []int {
	positions := make([]int, len(orderByFields))
	for i, obf := range orderByFields {
		found := false
		// Check group columns
		for j, gid := range groupByFieldIDs {
			if obf.FieldID == gid {
				positions[i] = j
				found = true
				break
			}
		}
		if found {
			continue
		}
		// Check aggregates (ORDER BY on aggregation field)
		for j, aggDef := range aggregates {
			if obf.FieldID == aggDef.GetFieldId() {
				positions[i] = len(groupByFieldIDs) + j
				break
			}
		}
	}
	return positions
}

// getPKOrderByField extracts the PK field from schema and returns it as an
// OrderByField for PK-based sorting (ascending, nulls last).
func getPKOrderByField(schema *schemapb.CollectionSchema) *orderby.OrderByField {
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			return orderby.NewOrderByField(
				field.FieldID,
				field.Name,
				field.DataType,
			)
		}
	}
	// Should never happen with a valid schema
	return orderby.NewOrderByField(0, "pk", schemapb.DataType_Int64)
}
