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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/internal/util/reduce/orderby"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// BuildQueryReducePipeline builds a pipeline for QN/Delegator query reduction.
// It dispatches to a pattern-specific builder based on query configuration.
//
// Pipeline patterns:
//
//	Plain:           [ReduceByPKTS(topK)] → output
//	ORDER BY:        [DeduplicatePK] → [OrderByLimit(topK)] → output
//	GROUP BY:        [DeduplicateByGroups(topK)] → output
//	GROUP BY+ORDER:  [DeduplicateByGroups(unlimited)] → [OrderByLimit(topK)] → output
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
	maxOutputSize int64,
) (*Pipeline, error) {
	hasGroupBy := len(groupByFieldIDs) > 0 || len(aggregates) > 0
	hasOrderBy := len(orderByFields) > 0

	if hasGroupBy && hasOrderBy {
		return buildGroupByOrderByReducePipeline(name, schema, topK, orderByFields, groupByFieldIDs, aggregates)
	} else if hasGroupBy {
		return buildGroupByReducePipeline(name, schema, topK, groupByFieldIDs, aggregates), nil
	} else if hasOrderBy {
		return buildOrderByReducePipeline(name, schema, topK, orderByFields, maxOutputSize), nil
	}
	return buildPlainReducePipeline(name, schema, topK, reduceType, maxOutputSize), nil
}

// buildPlainReducePipeline: [ReduceByPKTS(topK)] → output
func buildPlainReducePipeline(name string, schema *schemapb.CollectionSchema, topK int64, reduceType reduce.IReduceType, maxOutputSize int64) *Pipeline {
	b := NewPipelineBuilder(name)
	b.Add(OpReduceByPKTS, pin(), pout(), NewReduceByPKWithTimestampOperator(reduceType, maxOutputSize, topK, schema))
	return b.Build()
}

// buildOrderByReducePipeline: [DeduplicatePK] → [OrderByLimit(topK)] → output
func buildOrderByReducePipeline(name string, schema *schemapb.CollectionSchema, topK int64, orderByFields []*orderby.OrderByField, maxOutputSize int64) *Pipeline {
	b := NewPipelineBuilder(name)
	b.Add(OpDeduplicatePK, pin(), pch(ChannelDeduped), NewDeduplicatePKOperator(maxOutputSize, schema))
	b.Add(OpOrderByLimit, pch(ChannelDeduped), pout(), NewOrderByLimitOperator(orderByFields, topK))
	return b.Build()
}

// buildGroupByReducePipeline: [DeduplicateByGroups(topK)] → output
func buildGroupByReducePipeline(name string, schema *schemapb.CollectionSchema, topK int64, groupByFieldIDs []int64, aggregates []*planpb.Aggregate) *Pipeline {
	b := NewPipelineBuilder(name)
	b.Add(OpReduceByGroups, pin(), pout(), NewDeduplicateByGroupsOperator(schema, groupByFieldIDs, aggregates, topK))
	return b.Build()
}

// buildGroupByOrderByReducePipeline: [DeduplicateByGroups(unlimited)] → [OrderByLimit(topK)] → output
func buildGroupByOrderByReducePipeline(name string, schema *schemapb.CollectionSchema, topK int64, orderByFields []*orderby.OrderByField, groupByFieldIDs []int64, aggregates []*planpb.Aggregate) (*Pipeline, error) {
	positions, err := ComputeGroupByOrderPositions(orderByFields, groupByFieldIDs, aggregates)
	if err != nil {
		return nil, err
	}

	b := NewPipelineBuilder(name)
	b.Add(OpReduceByGroups, pin(), pch(ChannelReduced), NewDeduplicateByGroupsOperator(schema, groupByFieldIDs, aggregates, -1))
	b.Add(OpOrderByLimit, pch(ChannelReduced), pout(), NewOrderByLimitOperatorWithPositions(orderByFields, positions, topK))
	return b.Build(), nil
}

// Channel helper functions for readability.
func pin() []string            { return []string{PipelineInput} }
func pout() []string           { return []string{PipelineOutput} }
func pch(name string) []string { return []string{name} }

// ComputeGroupByOrderPositions maps ORDER BY fields to their column indices
// in the aggregation output layout: [group_col1, ..., group_colN, agg1, agg2, ...]
func ComputeGroupByOrderPositions(
	orderByFields []*orderby.OrderByField,
	groupByFieldIDs []int64,
	aggregates []*planpb.Aggregate,
) ([]int, error) {
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
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("ORDER BY field '%s' (ID=%d) not found in GROUP BY columns or aggregates", obf.FieldName, obf.FieldID)
		}
	}
	return positions, nil
}
