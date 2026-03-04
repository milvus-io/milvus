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

package sqlparser

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SqlToQueryPlan is the convenience entry point: SQL string -> PlanNode in one call.
func SqlToQueryPlan(sql string, schema *typeutil.SchemaHelper) (*planpb.PlanNode, error) {
	comp, err := ExtractSqlComponents(sql)
	if err != nil {
		return nil, fmt.Errorf("SQL parse error: %w", err)
	}
	return BuildQueryPlan(comp, schema)
}

// BuildQueryPlan converts SqlComponents IR into a Milvus PlanNode proto.
// It reuses planparserv2 for WHERE expression parsing and SchemaHelper for field_id resolution.
func BuildQueryPlan(comp *SqlComponents, schema *typeutil.SchemaHelper) (*planpb.PlanNode, error) {
	// 1. Parse WHERE -> Expr proto via planparserv2
	var predicates *planpb.Expr
	if comp.Where != nil && comp.Where.MilvusExpr != "" {
		plan, err := planparserv2.CreateRetrievePlan(schema, comp.Where.MilvusExpr, nil)
		if err != nil {
			return nil, fmt.Errorf("WHERE parse error: %w", err)
		}
		predicates = plan.GetQuery().GetPredicates()
	}

	query := &planpb.QueryPlanNode{
		Predicates: predicates,
	}

	// 2. Build AggregateNode (GROUP BY + aggregates)
	aggNode, err := buildAggregateNode(comp, schema)
	if err != nil {
		return nil, err
	}
	if aggNode != nil {
		query.AggregateNode = aggNode
	}

	// 3. Build SortNode (ORDER BY)
	sortNode := buildSortNode(comp)
	if sortNode != nil {
		query.SortNode = sortNode
	}

	// 4. Build ProjectNode (post-agg projections)
	projectNode := buildProjectNode(comp)
	if projectNode != nil {
		query.ProjectNode = projectNode
	}

	// 5. LIMIT
	if comp.Limit > 0 {
		query.Limit = comp.Limit
	}

	// 6. Resolve output field IDs
	outputFieldIDs, err := resolveOutputFieldIDs(comp, schema)
	if err != nil {
		return nil, err
	}

	return &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: query,
		},
		OutputFieldIds: outputFieldIDs,
	}, nil
}

// BuildSearchPlan converts SqlComponents with a VectorSearchDef into a search PlanNode.
// The returned plan uses PlaceholderTag "$0" -- the caller must provide a matching
// PlaceholderGroup (use BuildPlaceholderGroup to create one from the query vector).
func BuildSearchPlan(comp *SqlComponents, schema *typeutil.SchemaHelper) (*planpb.PlanNode, error) {
	if comp.VectorSearch == nil {
		return nil, fmt.Errorf("no vector search definition found in SQL components")
	}
	vs := comp.VectorSearch

	// Resolve vector field from schema
	field, err := schema.GetFieldFromNameDefaultJSON(vs.FieldName)
	if err != nil {
		return nil, fmt.Errorf("vector field %q not found: %w", vs.FieldName, err)
	}

	vectorType, err := dataTypeToVectorType(field.GetDataType())
	if err != nil {
		return nil, err
	}

	// Parse WHERE -> predicates
	var predicates *planpb.Expr
	if comp.Where != nil && comp.Where.MilvusExpr != "" {
		plan, err := planparserv2.CreateRetrievePlan(schema, comp.Where.MilvusExpr, nil)
		if err != nil {
			return nil, fmt.Errorf("WHERE parse error: %w", err)
		}
		predicates = plan.GetQuery().GetPredicates()
	}

	topK := vs.TopK
	if topK <= 0 {
		topK = 10
	}

	metricType := vs.MetricType
	if metricType == "" {
		metricType = "L2"
	}

	queryInfo := &planpb.QueryInfo{
		Topk:         topK,
		MetricType:   metricType,
		SearchParams: `{"nprobe": 10}`, // default search params
		RoundDecimal: -1,
	}

	// Resolve output field IDs
	outputFieldIDs, err := resolveSearchOutputFieldIDs(comp, schema)
	if err != nil {
		return nil, err
	}

	// Store resolved field ID back into VectorSearchDef for callers
	vs.FieldID = field.GetFieldID()

	return &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{
			VectorAnns: &planpb.VectorANNS{
				VectorType:     vectorType,
				FieldId:        field.GetFieldID(),
				Predicates:     predicates,
				QueryInfo:      queryInfo,
				PlaceholderTag: "$0",
			},
		},
		OutputFieldIds: outputFieldIDs,
	}, nil
}

// BuildPlaceholderGroup creates a serialized PlaceholderGroup from a float32 query vector.
// This is used with BuildSearchPlan to provide the query vector for search.
func BuildPlaceholderGroup(vectors [][]float32) ([]byte, error) {
	if len(vectors) == 0 {
		return nil, fmt.Errorf("at least one query vector is required")
	}

	var values [][]byte
	for _, vec := range vectors {
		bs := make([]byte, len(vec)*4)
		for i, v := range vec {
			binary.LittleEndian.PutUint32(bs[i*4:], math.Float32bits(v))
		}
		values = append(values, bs)
	}

	pg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   commonpb.PlaceholderType_FloatVector,
				Values: values,
			},
		},
	}
	return proto.Marshal(pg)
}

// resolveSearchOutputFieldIDs collects output field IDs for a search plan.
// Includes all non-vector SELECT columns plus the primary key.
func resolveSearchOutputFieldIDs(comp *SqlComponents, schema *typeutil.SchemaHelper) ([]int64, error) {
	seen := make(map[int64]bool)
	var ids []int64

	addField := func(col *ColumnRef) error {
		if col == nil {
			return nil
		}
		fieldID, err := resolveFieldID(col, schema)
		if err != nil {
			return err
		}
		if !seen[fieldID] {
			seen[fieldID] = true
			ids = append(ids, fieldID)
		}
		return nil
	}

	for _, si := range comp.SelectItems {
		switch si.Type {
		case SelectColumn:
			if err := addField(si.Column); err != nil {
				return nil, err
			}
		case SelectVectorDist:
			// The vector field itself is handled by the search engine;
			// no need to add it to output fields.
		}
	}

	// Ensure primary key is always in output
	pkField, err := schema.GetPrimaryKeyField()
	if err == nil && pkField != nil {
		pkID := pkField.GetFieldID()
		if !seen[pkID] {
			seen[pkID] = true
			ids = append(ids, pkID)
		}
	}

	return ids, nil
}

// dataTypeToVectorType maps schemapb.DataType to planpb.VectorType.
func dataTypeToVectorType(dt schemapb.DataType) (planpb.VectorType, error) {
	switch dt {
	case schemapb.DataType_FloatVector:
		return planpb.VectorType_FloatVector, nil
	case schemapb.DataType_BinaryVector:
		return planpb.VectorType_BinaryVector, nil
	case schemapb.DataType_Float16Vector:
		return planpb.VectorType_Float16Vector, nil
	case schemapb.DataType_BFloat16Vector:
		return planpb.VectorType_BFloat16Vector, nil
	case schemapb.DataType_SparseFloatVector:
		return planpb.VectorType_SparseFloatVector, nil
	case schemapb.DataType_Int8Vector:
		return planpb.VectorType_Int8Vector, nil
	default:
		return 0, fmt.Errorf("field type %s is not a vector type", dt.String())
	}
}

// buildAggregateNode builds the AggregateNode from GROUP BY + SELECT aggregates.
func buildAggregateNode(comp *SqlComponents, schema *typeutil.SchemaHelper) (*planpb.AggregateNode, error) {
	var groupBy []*planpb.Aggregate
	var aggregates []*planpb.Aggregate

	// GROUP BY keys -> AggregateNode.group_by
	for _, gb := range comp.GroupBy {
		col := gb.Column
		if col == nil && gb.Alias != "" {
			// Alias reference -- resolve from SELECT items
			for _, si := range comp.SelectItems {
				if si.Alias != gb.Alias {
					continue
				}
				if si.Column != nil {
					col = si.Column
				} else if si.Type == SelectComputed {
					// Computed expression (e.g. extract(hour FROM ...)):
					// try to extract the innermost column reference from the raw expression
					col = extractDeepColumnRef(si.RawExpr)
				}
				break
			}
		}
		if col == nil {
			// If still nil, the GROUP BY references a computed expression without a resolvable column.
			// Skip adding to group_by proto (it will be handled by ProjectNode at execution time).
			continue
		}

		fieldID, err := resolveFieldID(col, schema)
		if err != nil {
			return nil, fmt.Errorf("GROUP BY field %q: %w", col.FieldName, err)
		}

		groupBy = append(groupBy, &planpb.Aggregate{
			FieldId:    fieldID,
			NestedPath: col.NestedPath,
		})
	}

	// SELECT aggregates -> AggregateNode.aggregates
	for _, si := range comp.SelectItems {
		if si.Type != SelectAgg || si.AggFunc == nil {
			continue
		}

		agg, err := buildAggregate(si.AggFunc, si.RawExpr, schema)
		if err != nil {
			return nil, fmt.Errorf("aggregate %s: %w", si.AggFunc.FuncName, err)
		}
		aggregates = append(aggregates, agg)
	}

	// Scan SelectComputed items for nested aggregates (e.g., Q5's max/min inside arithmetic).
	// These must be registered so segcore computes them and the reducer includes them.
	for _, si := range comp.SelectItems {
		if si.Type != SelectComputed || si.RawExpr == nil {
			continue
		}
		for _, na := range ExtractNestedAggregates(si.RawExpr) {
			op, err := aggFuncNameToOp(na.FuncName, false)
			if err != nil {
				return nil, fmt.Errorf("nested aggregate %s: %w", na.FuncName, err)
			}
			fieldID, err := resolveFieldID(na.Column, schema)
			if err != nil {
				return nil, fmt.Errorf("nested aggregate %s(%s): %w", na.FuncName, na.Column.MilvusExpr(), err)
			}
			aggregates = append(aggregates, &planpb.Aggregate{
				Op:         op,
				FieldId:    fieldID,
				NestedPath: na.Column.NestedPath,
			})
		}
	}

	if len(groupBy) == 0 && len(aggregates) == 0 {
		return nil, nil
	}

	return &planpb.AggregateNode{
		GroupBy:    groupBy,
		Aggregates: aggregates,
	}, nil
}

// buildAggregate converts an AggFuncCall IR to an Aggregate proto.
// rawExpr is the original AST expression from the SelectItem, used as fallback
// when Arg is nil (e.g. min(to_timestamp(data->>'time_us'))).
func buildAggregate(afc *AggFuncCall, rawExpr *pg_query.Node, schema *typeutil.SchemaHelper) (*planpb.Aggregate, error) {
	op, err := aggFuncNameToOp(afc.FuncName, afc.Distinct)
	if err != nil {
		return nil, err
	}

	agg := &planpb.Aggregate{Op: op}

	col := afc.Arg
	// If Arg is nil (nested function like min(to_timestamp(...))),
	// try to extract the innermost column reference from the raw AST.
	if col == nil && rawExpr != nil {
		col = extractDeepColumnRef(rawExpr)
	}

	if col != nil {
		fieldID, err := resolveFieldID(col, schema)
		if err != nil {
			return nil, err
		}
		agg.FieldId = fieldID
		agg.NestedPath = col.NestedPath
	}

	return agg, nil
}

// buildSortNode builds the SortNode from ORDER BY items.
func buildSortNode(comp *SqlComponents) *planpb.SortNode {
	if len(comp.OrderBy) == 0 {
		return nil
	}

	var items []*planpb.SortItem
	for _, ob := range comp.OrderBy {
		items = append(items, &planpb.SortItem{
			Field: ob.Ref,
			Desc:  ob.Desc,
		})
	}
	return &planpb.SortNode{Items: items}
}

// buildProjectNode builds the ProjectNode for post-aggregation computed columns.
// Handles both named functions (extract, to_timestamp) and arithmetic operators (+, -, *, /).
func buildProjectNode(comp *SqlComponents) *planpb.ProjectNode {
	var items []*planpb.ProjectItem

	for _, si := range comp.SelectItems {
		if si.Type != SelectComputed || si.Computed == nil {
			continue
		}
		// Skip computed expressions that contain nested aggregates (e.g.,
		// max(x) - min(x)). These are evaluated by ApplyProjection on the
		// proxy side using the reducer output. Sending them to segcore's
		// ComputeProjectNode would fail because it can't parse aggregate
		// function references as column names.
		if si.RawExpr != nil && len(ExtractNestedAggregates(si.RawExpr)) > 0 {
			continue
		}
		item := &planpb.ProjectItem{
			FunctionName: si.Computed.FuncName,
			Alias:        si.Alias,
		}
		for _, arg := range si.Computed.Args {
			item.Args = append(item.Args, fmt.Sprintf("%v", arg))
		}
		items = append(items, item)
	}

	if len(items) == 0 {
		return nil
	}
	return &planpb.ProjectNode{Items: items}
}

// extractDeepColumnRef recursively descends into an AST expression tree
// to find the innermost JSON column reference. This handles patterns like:
//
//	min(to_timestamp((data->>'time_us')::bigint / 1000000.0))
//	-> extracts ColumnRef{FieldName: "data", NestedPath: ["time_us"]}
func extractDeepColumnRef(node *pg_query.Node) *ColumnRef {
	if node == nil {
		return nil
	}

	// FuncCall: descend into arguments
	if fc := node.GetFuncCall(); fc != nil {
		for _, arg := range fc.Args {
			if col := extractDeepColumnRef(arg); col != nil {
				return col
			}
		}
		return nil
	}

	// A_Expr: check for JSON path or descend into arithmetic
	if ae := node.GetAExpr(); ae != nil {
		opName := getAExprOpName(ae)
		if opName == "->" || opName == "->>" {
			col, err := extractJSONPath(ae)
			if err == nil {
				return col
			}
		}
		// Try left then right for arithmetic
		if col := extractDeepColumnRef(ae.Lexpr); col != nil {
			return col
		}
		return extractDeepColumnRef(ae.Rexpr)
	}

	// TypeCast: descend
	if tc := node.GetTypeCast(); tc != nil {
		return extractDeepColumnRef(tc.Arg)
	}

	// ColumnRef: base case
	if cr := node.GetColumnRef(); cr != nil {
		col, err := columnRefToColumnRef(cr)
		if err == nil {
			return col
		}
	}

	return nil
}

// resolveOutputFieldIDs collects the field IDs needed in the output.
func resolveOutputFieldIDs(comp *SqlComponents, schema *typeutil.SchemaHelper) ([]int64, error) {
	seen := make(map[int64]bool)
	var ids []int64

	addField := func(col *ColumnRef) error {
		if col == nil {
			return nil
		}
		fieldID, err := resolveFieldID(col, schema)
		if err != nil {
			return err
		}
		if !seen[fieldID] {
			seen[fieldID] = true
			ids = append(ids, fieldID)
		}
		return nil
	}

	for _, si := range comp.SelectItems {
		switch si.Type {
		case SelectColumn:
			if err := addField(si.Column); err != nil {
				return nil, err
			}
		case SelectAgg:
			if si.AggFunc != nil {
				if err := addField(si.AggFunc.Arg); err != nil {
					return nil, err
				}
			}
		}
	}

	// GROUP BY fields also need to be in output
	for _, gb := range comp.GroupBy {
		col := gb.Column
		if col == nil && gb.Alias != "" {
			// Resolve alias -> column from SELECT items
			for _, si := range comp.SelectItems {
				if si.Alias == gb.Alias && si.Column != nil {
					col = si.Column
					break
				}
			}
		}
		if col != nil {
			if err := addField(col); err != nil {
				return nil, err
			}
		}
	}

	return ids, nil
}

// resolveFieldID looks up a ColumnRef's field ID from the schema.
func resolveFieldID(col *ColumnRef, schema *typeutil.SchemaHelper) (int64, error) {
	field, err := schema.GetFieldFromNameDefaultJSON(col.FieldName)
	if err != nil {
		return 0, fmt.Errorf("field %q not found in schema: %w", col.FieldName, err)
	}
	return field.GetFieldID(), nil
}

// aggFuncNameToOp converts a function name string to AggregateOp proto enum.
func aggFuncNameToOp(name string, distinct bool) (planpb.AggregateOp, error) {
	lower := strings.ToLower(name)
	if distinct && lower == "count" {
		return planpb.AggregateOp_count_distinct, nil
	}
	switch lower {
	case "count":
		return planpb.AggregateOp_count, nil
	case "sum":
		return planpb.AggregateOp_sum, nil
	case "avg":
		return planpb.AggregateOp_avg, nil
	case "min":
		return planpb.AggregateOp_min, nil
	case "max":
		return planpb.AggregateOp_max, nil
	default:
		return 0, fmt.Errorf("unsupported aggregate function: %s", name)
	}
}
