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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// supportedAggFuncs lists the aggregate functions we support.
var supportedAggFuncs = map[string]bool{
	"count": true,
	"sum":   true,
	"avg":   true,
	"min":   true,
	"max":   true,
}

// vectorDistFuncs maps function names to vector distance operators.
// These functions are used as an alternative to pgvector operators (<-> <=> <#>).
var vectorDistFuncs = map[string]VectorDistOp{
	"l2_distance":     VectorDistL2,
	"cosine_distance": VectorDistCosine,
	"ip_distance":     VectorDistIP,
}

// vectorDistOps maps pgvector operator strings to vector distance operators.
var vectorDistOps = map[string]VectorDistOp{
	"<->": VectorDistL2,
	"<=>": VectorDistCosine,
	"<#>": VectorDistIP,
}

// ExtractSqlComponents parses a SQL string and extracts its components
// into the SqlComponents IR. This is the main entry point for the parser.
func ExtractSqlComponents(sql string) (*SqlComponents, error) {
	rawStmt, err := ParseOne(sql)
	if err != nil {
		return nil, err
	}

	stmt := rawStmt.Stmt.GetSelectStmt()
	if stmt == nil {
		return nil, fmt.Errorf("expected SELECT statement, got %T", rawStmt.Stmt.Node)
	}

	comp := &SqlComponents{
		Limit:     -1, // default: no limit
		QueryType: QueryTypeQuery,
	}

	// F1.1: Extract FROM clause -> collection name
	if err := extractFrom(stmt, comp); err != nil {
		return nil, err
	}

	// F1.1 + F1.2 + F1.4 + F1.8: Extract SELECT list
	if err := extractSelectList(stmt, comp); err != nil {
		return nil, err
	}

	// F1.3: Extract WHERE clause
	if err := extractWhere(stmt, comp); err != nil {
		return nil, err
	}

	// F1.5: Extract GROUP BY
	if err := extractGroupBy(stmt, comp); err != nil {
		return nil, err
	}

	// F1.6: Extract ORDER BY (from SelectStmt itself, SortClause is on the same node)
	if err := extractOrderBy(stmt, comp); err != nil {
		return nil, err
	}

	// F1.7: Extract LIMIT
	if err := extractLimit(stmt, comp); err != nil {
		return nil, err
	}

	// Detect vector search from SELECT items
	detectVectorSearch(comp)

	return comp, nil
}

// extractFrom extracts the collection name from the FROM clause.
func extractFrom(stmt *pg_query.SelectStmt, comp *SqlComponents) error {
	if len(stmt.FromClause) == 0 {
		return fmt.Errorf("FROM clause is required")
	}
	if len(stmt.FromClause) > 1 {
		return fmt.Errorf("JOIN not supported: only single table queries are allowed")
	}

	rv := stmt.FromClause[0].GetRangeVar()
	if rv == nil {
		return fmt.Errorf("expected table name, got %T (subqueries in FROM not supported)", stmt.FromClause[0].Node)
	}

	comp.Collection = rv.Relname
	return nil
}

// extractSelectList processes the SELECT expression list.
func extractSelectList(stmt *pg_query.SelectStmt, comp *SqlComponents) error {
	for _, node := range stmt.TargetList {
		item, err := extractSelectExpr(node)
		if err != nil {
			return fmt.Errorf("SELECT item error: %w", err)
		}
		comp.SelectItems = append(comp.SelectItems, *item)
	}
	return nil
}

// extractSelectExpr converts a single ResTarget node to a SelectItem.
func extractSelectExpr(node *pg_query.Node) (*SelectItem, error) {
	rt := node.GetResTarget()
	if rt == nil {
		return nil, fmt.Errorf("expected ResTarget, got %T", node.Node)
	}
	alias := rt.Name
	val := rt.Val

	// Check for SELECT *
	if cr := val.GetColumnRef(); cr != nil {
		if len(cr.Fields) > 0 && cr.Fields[0].GetAStar() != nil {
			return &SelectItem{Type: SelectStar, Alias: alias, RawExpr: val}, nil
		}
	}

	// Check for function call (aggregate, vector dist, or scalar)
	if fc := val.GetFuncCall(); fc != nil {
		funcName := getFuncName(fc)

		// Check for vector distance function: l2_distance, cosine_distance, ip_distance
		if distOp, isVecDist := vectorDistFuncs[funcName]; isVecDist {
			vde, err := extractVectorDistFunc(fc, distOp)
			if err != nil {
				return nil, err
			}
			return &SelectItem{Type: SelectVectorDist, Distance: vde, Alias: alias, RawExpr: val}, nil
		}

		// Check for aggregate function
		if supportedAggFuncs[funcName] {
			agg, err := extractAggFunc(fc)
			if err != nil {
				return nil, err
			}
			return &SelectItem{Type: SelectAgg, AggFunc: agg, Alias: alias, RawExpr: val}, nil
		}

		// Non-aggregate function -> computed expression
		computed, err := extractFunctionCall(fc)
		if err != nil {
			return nil, err
		}
		return &SelectItem{Type: SelectComputed, Computed: computed, Alias: alias, RawExpr: val}, nil
	}

	// Check for A_Expr (arithmetic, JSON, pgvector operators)
	if aexpr := val.GetAExpr(); aexpr != nil {
		opName := getAExprOpName(aexpr)

		// pgvector operators: <->, <=>, <#>
		if distOp, isVecDist := vectorDistOps[opName]; isVecDist {
			vde, err := extractVectorDistOp(aexpr, distOp)
			if err != nil {
				return nil, err
			}
			return &SelectItem{Type: SelectVectorDist, Distance: vde, Alias: alias, RawExpr: val}, nil
		}

		// JSON operators -> treat as column ref
		if opName == "->" || opName == "->>" {
			col, err := extractJSONPath(aexpr)
			if err != nil {
				return nil, err
			}
			if col != nil {
				return &SelectItem{Type: SelectColumn, Column: col, Alias: alias, RawExpr: val}, nil
			}
		}

		// Arithmetic -> SelectComputed
		computed := &FunctionCall{
			FuncName: opName,
			Args:     []interface{}{aexpr.Lexpr, aexpr.Rexpr},
		}
		return &SelectItem{Type: SelectComputed, Computed: computed, Alias: alias, RawExpr: val}, nil
	}

	// Try as column reference (plain or with type cast)
	col, err := extractColumnRef(val)
	if err != nil {
		return nil, err
	}
	if col != nil {
		return &SelectItem{Type: SelectColumn, Column: col, Alias: alias, RawExpr: val}, nil
	}

	// Fallback: treat as computed expression
	return &SelectItem{Type: SelectComputed, Alias: alias, RawExpr: val}, nil
}

// extractAggFunc extracts an AggFuncCall from a FuncCall.
func extractAggFunc(fc *pg_query.FuncCall) (*AggFuncCall, error) {
	funcName := getFuncName(fc)

	agg := &AggFuncCall{
		FuncName: funcName,
		Distinct: fc.AggDistinct,
	}

	// count(*) - uses AggStar
	if fc.AggStar || len(fc.Args) == 0 {
		if funcName != "count" {
			return nil, fmt.Errorf("%s(*) is not supported, only count(*) is allowed", funcName)
		}
		return agg, nil
	}

	// Single argument
	if len(fc.Args) != 1 {
		return nil, fmt.Errorf("aggregate %s expects 1 argument, got %d", funcName, len(fc.Args))
	}

	arg := fc.Args[0]

	// Check for nested aggregation
	if innerFC := arg.GetFuncCall(); innerFC != nil {
		innerName := getFuncName(innerFC)
		if supportedAggFuncs[innerName] {
			return nil, fmt.Errorf("nested aggregation not supported: %s(%s(...))", funcName, innerName)
		}
	}

	col, err := extractColumnRef(arg)
	if err != nil {
		return nil, fmt.Errorf("aggregate %s argument: %w", funcName, err)
	}
	if col != nil {
		agg.Arg = col
		return agg, nil
	}

	// Allow function expressions as aggregate arguments (e.g. min(to_timestamp(...)))
	// In this case, we keep Arg as nil and the caller handles via RawExpr
	return agg, nil
}

// extractFunctionCall extracts a FunctionCall from a FuncCall.
func extractFunctionCall(fc *pg_query.FuncCall) (*FunctionCall, error) {
	result := &FunctionCall{
		FuncName: getFuncName(fc),
	}
	for _, arg := range fc.Args {
		result.Args = append(result.Args, arg)
	}
	return result, nil
}

// extractWhere converts the WHERE clause to a Milvus expression.
func extractWhere(stmt *pg_query.SelectStmt, comp *SqlComponents) error {
	if stmt.WhereClause == nil {
		return nil
	}
	milvusExpr, err := ConvertWhereToMilvusExpr(stmt.WhereClause)
	if err != nil {
		return fmt.Errorf("WHERE conversion error: %w", err)
	}
	comp.Where = &WhereClause{
		MilvusExpr: milvusExpr,
		RawExpr:    stmt.WhereClause,
	}
	return nil
}

// extractGroupBy extracts GROUP BY items.
func extractGroupBy(stmt *pg_query.SelectStmt, comp *SqlComponents) error {
	if len(stmt.GroupClause) == 0 {
		return nil
	}

	for _, gbNode := range stmt.GroupClause {
		item, err := extractGroupByItem(gbNode, comp.SelectItems)
		if err != nil {
			return fmt.Errorf("GROUP BY error: %w", err)
		}
		comp.GroupBy = append(comp.GroupBy, *item)
	}
	return nil
}

// extractGroupByItem processes a single GROUP BY expression.
func extractGroupByItem(node *pg_query.Node, selectItems []SelectItem) (*GroupByItem, error) {
	// First: check if it's a simple column ref that matches a SELECT alias
	if cr := node.GetColumnRef(); cr != nil {
		if len(cr.Fields) == 1 {
			if s := cr.Fields[0].GetString_(); s != nil {
				aliasName := s.Sval
				for _, si := range selectItems {
					if si.Alias == aliasName {
						return &GroupByItem{Alias: aliasName, Column: si.Column}, nil
					}
				}
				// Not an alias -> treat as direct column reference
				return &GroupByItem{Column: &ColumnRef{FieldName: aliasName}}, nil
			}
		}
	}

	// Try as JSON path column reference
	col, err := extractColumnRef(node)
	if err != nil {
		return nil, err
	}
	if col != nil {
		return &GroupByItem{Column: col}, nil
	}

	return nil, fmt.Errorf("unsupported GROUP BY expression: %T", node.Node)
}

// extractOrderBy extracts ORDER BY items from the SelectStmt.
func extractOrderBy(stmt *pg_query.SelectStmt, comp *SqlComponents) error {
	if len(stmt.SortClause) == 0 {
		return nil
	}

	for _, node := range stmt.SortClause {
		sb := node.GetSortBy()
		if sb == nil {
			continue
		}
		item, err := extractOrderItem(sb, comp.SelectItems)
		if err != nil {
			return fmt.Errorf("ORDER BY error: %w", err)
		}
		comp.OrderBy = append(comp.OrderBy, *item)
	}
	return nil
}

// extractOrderItem processes a single ORDER BY element.
func extractOrderItem(sb *pg_query.SortBy, selectItems []SelectItem) (*OrderItem, error) {
	desc := sb.SortbyDir == pg_query.SortByDir_SORTBY_DESC

	// Try to get the reference name
	ref := ""
	sortNode := sb.Node

	if cr := sortNode.GetColumnRef(); cr != nil {
		if len(cr.Fields) > 0 {
			if s := cr.Fields[0].GetString_(); s != nil {
				ref = s.Sval
			}
		}
	} else if ae := sortNode.GetAExpr(); ae != nil {
		opName := getAExprOpName(ae)
		if opName == "->" || opName == "->>" {
			// JSON path in ORDER BY
			col, err := extractJSONPath(ae)
			if err != nil {
				return nil, err
			}
			if col != nil {
				ref = col.MilvusExpr()
			}
		} else {
			ref = fmt.Sprintf("%s", sortNode)
		}
	} else {
		ref = fmt.Sprintf("%s", sortNode)
	}

	// Check if this references a distance column
	isDistance := false
	for _, si := range selectItems {
		if si.Type == SelectVectorDist && si.Alias == ref {
			isDistance = true
			break
		}
	}

	return &OrderItem{
		Ref:        ref,
		Desc:       desc,
		IsDistance: isDistance,
	}, nil
}

// extractLimit extracts the LIMIT value.
func extractLimit(stmt *pg_query.SelectStmt, comp *SqlComponents) error {
	if stmt.LimitCount == nil {
		return nil
	}

	ac := stmt.LimitCount.GetAConst()
	if ac == nil {
		return fmt.Errorf("unsupported LIMIT expression: %T", stmt.LimitCount.Node)
	}

	if iv := ac.GetIval(); iv != nil {
		comp.Limit = int64(iv.Ival)
		return nil
	}
	if fv := ac.GetFval(); fv != nil {
		val, err := strconv.ParseInt(fv.Fval, 10, 64)
		if err != nil {
			return fmt.Errorf("LIMIT must be an integer: %w", err)
		}
		comp.Limit = val
		return nil
	}

	return fmt.Errorf("unsupported LIMIT value type")
}

// extractVectorDistFunc extracts a vector distance function call into a VectorDistExpr.
func extractVectorDistFunc(fc *pg_query.FuncCall, op VectorDistOp) (*VectorDistExpr, error) {
	if len(fc.Args) != 2 {
		return nil, fmt.Errorf("vector distance function %s expects 2 arguments, got %d", getFuncName(fc), len(fc.Args))
	}

	col, err := extractColumnRef(fc.Args[0])
	if err != nil || col == nil {
		return nil, fmt.Errorf("first argument of %s must be a column reference: %v", getFuncName(fc), err)
	}

	vde := &VectorDistExpr{Column: col, Operator: op}

	arg := fc.Args[1]
	if pr := arg.GetParamRef(); pr != nil {
		vde.ParamRef = fmt.Sprintf("$%d", pr.Number)
	} else {
		vector, ok, err := extractVectorLiteral(arg)
		if err != nil {
			return nil, err
		}
		if ok {
			vde.Vector = vector
		} else {
			vde.ParamRef = fmt.Sprintf("%s", arg)
		}
	}

	return vde, nil
}

// extractVectorDistOp extracts a pgvector operator expression (<-> <=> <#>) into a VectorDistExpr.
func extractVectorDistOp(ae *pg_query.A_Expr, op VectorDistOp) (*VectorDistExpr, error) {
	col, err := extractColumnRef(ae.Lexpr)
	if err != nil || col == nil {
		return nil, fmt.Errorf("left operand of pgvector operator must be a column reference: %v", err)
	}

	vde := &VectorDistExpr{Column: col, Operator: op}

	rexpr := ae.Rexpr
	if pr := rexpr.GetParamRef(); pr != nil {
		vde.ParamRef = fmt.Sprintf("$%d", pr.Number)
	} else {
		vector, ok, err := extractVectorLiteral(rexpr)
		if err != nil {
			return nil, err
		}
		if ok {
			vde.Vector = vector
		} else {
			vde.ParamRef = fmt.Sprintf("%s", rexpr)
		}
	}

	return vde, nil
}

// extractVectorLiteral extracts a pgvector string literal such as
// '[1, 2, 3]' or '[1, 2, 3]'::vector into float32 values.
func extractVectorLiteral(node *pg_query.Node) ([]float32, bool, error) {
	if node == nil {
		return nil, false, nil
	}

	if tc := node.GetTypeCast(); tc != nil {
		return extractVectorLiteral(tc.Arg)
	}

	ac := node.GetAConst()
	if ac == nil {
		return nil, false, nil
	}
	sv := ac.GetSval()
	if sv == nil {
		return nil, false, nil
	}

	vector, err := parseVectorLiteral(sv.Sval)
	if err != nil {
		return nil, true, err
	}
	return vector, true, nil
}

func parseVectorLiteral(literal string) ([]float32, error) {
	literal = strings.TrimSpace(literal)
	if literal == "" {
		return nil, fmt.Errorf("vector literal cannot be empty")
	}

	var values []float64
	if err := json.Unmarshal([]byte(literal), &values); err != nil {
		return nil, fmt.Errorf("invalid vector literal %q: %w", literal, err)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("vector literal cannot be empty")
	}

	vector := make([]float32, len(values))
	for i, v := range values {
		vector[i] = float32(v)
	}
	return vector, nil
}

// detectVectorSearch scans SELECT items for vector distance expressions
// and populates the VectorSearchDef on the SqlComponents.
func detectVectorSearch(comp *SqlComponents) {
	for _, si := range comp.SelectItems {
		if si.Type != SelectVectorDist || si.Distance == nil {
			continue
		}

		topK := comp.Limit
		if topK <= 0 {
			topK = 10 // default topK
		}

		comp.VectorSearch = &VectorSearchDef{
			FieldName:  si.Distance.Column.FieldName,
			MetricType: si.Distance.Operator.MetricType(),
			Vector:     si.Distance.Vector,
			ParamRef:   si.Distance.ParamRef,
			TopK:       topK,
			DistAlias:  si.Alias,
		}

		if comp.Where != nil && comp.Where.MilvusExpr != "" {
			comp.QueryType = QueryTypeHybrid
		} else {
			comp.QueryType = QueryTypeSearch
		}
		return
	}
}
