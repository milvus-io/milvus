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
	"fmt"
	"strconv"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ConvertWhereToMilvusExpr converts a PG AST WHERE expression to a Milvus
// filter expression string.
//
// Mapping rules:
//   - = -> ==, <> -> !=, <, >, <=, >= -> same
//   - AND -> and, OR -> or, NOT -> not
//   - data->>'key' = 'val' -> data["key"] == "val"
//   - IN ('a', 'b') -> in ["a", "b"]
//   - IS NULL -> == null, IS NOT NULL -> != null
//   - LIKE -> like
//   - BETWEEN a AND b -> (col >= a and col <= b)
func ConvertWhereToMilvusExpr(node *pg_query.Node) (string, error) {
	if node == nil {
		return "", nil
	}
	return convertExpr(node)
}

func convertExpr(node *pg_query.Node) (string, error) {
	if node == nil {
		return "", fmt.Errorf("nil expression node")
	}

	// BoolExpr unifies AND/OR/NOT
	if be := node.GetBoolExpr(); be != nil {
		switch be.Boolop {
		case pg_query.BoolExprType_AND_EXPR:
			return convertBoolOp(be, "and")
		case pg_query.BoolExprType_OR_EXPR:
			return convertBoolOp(be, "or")
		case pg_query.BoolExprType_NOT_EXPR:
			return convertNot(be)
		}
	}

	// A_Expr unifies comparisons, IN, LIKE, BETWEEN
	if ae := node.GetAExpr(); ae != nil {
		switch ae.Kind {
		case pg_query.A_Expr_Kind_AEXPR_OP:
			return convertSimpleOp(ae)
		case pg_query.A_Expr_Kind_AEXPR_IN:
			return convertIn(ae)
		case pg_query.A_Expr_Kind_AEXPR_LIKE:
			return convertLike(ae)
		case pg_query.A_Expr_Kind_AEXPR_BETWEEN:
			return convertBetween(ae, false)
		case pg_query.A_Expr_Kind_AEXPR_NOT_BETWEEN:
			return convertBetween(ae, true)
		default:
			return "", fmt.Errorf("unsupported A_Expr kind: %v", ae.Kind)
		}
	}

	// NullTest (IS NULL / IS NOT NULL)
	if nt := node.GetNullTest(); nt != nil {
		return convertNullTest(nt)
	}

	// TypeCast in WHERE: ignore the cast, convert the inner expression
	if tc := node.GetTypeCast(); tc != nil {
		return convertExpr(tc.Arg)
	}

	return "", fmt.Errorf("unsupported WHERE expression type: %T", node.Node)
}

// convertBoolOp handles AND/OR with N args (PG AST can chain them).
func convertBoolOp(be *pg_query.BoolExpr, op string) (string, error) {
	if len(be.Args) == 0 {
		return "", fmt.Errorf("BoolExpr %s with no args", op)
	}
	if len(be.Args) == 1 {
		return convertExpr(be.Args[0])
	}

	var parts []string
	for _, arg := range be.Args {
		s, err := convertExpr(arg)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("(%s)", s))
	}
	return strings.Join(parts, " "+op+" "), nil
}

// convertNot handles NOT expression.
func convertNot(be *pg_query.BoolExpr) (string, error) {
	if len(be.Args) != 1 {
		return "", fmt.Errorf("NOT expression expects 1 argument, got %d", len(be.Args))
	}
	inner, err := convertExpr(be.Args[0])
	if err != nil {
		return "", err
	}
	// Avoid double parentheses: if inner already starts with '(', don't wrap again
	if strings.HasPrefix(inner, "(") {
		return fmt.Sprintf("not %s", inner), nil
	}
	return fmt.Sprintf("not (%s)", inner), nil
}

// convertSimpleOp handles AEXPR_OP: =, <>, <, >, <=, >=
func convertSimpleOp(ae *pg_query.A_Expr) (string, error) {
	left, err := convertOperand(ae.Lexpr)
	if err != nil {
		return "", err
	}
	right, err := convertOperand(ae.Rexpr)
	if err != nil {
		return "", err
	}

	opName := getAExprOpName(ae)
	milvusOp := aexprOpToMilvus(opName)
	return fmt.Sprintf("%s %s %s", left, milvusOp, right), nil
}

// convertIn handles AEXPR_IN: IN and NOT IN.
// IN: Name[0] = "=", NOT IN: Name[0] = "<>"
func convertIn(ae *pg_query.A_Expr) (string, error) {
	left, err := convertOperand(ae.Lexpr)
	if err != nil {
		return "", err
	}

	// Right side should be a List
	list := ae.Rexpr.GetList()
	if list == nil {
		return "", fmt.Errorf("IN expression requires list, got %T", ae.Rexpr.Node)
	}

	var values []string
	for _, item := range list.Items {
		v, err := convertOperand(item)
		if err != nil {
			return "", err
		}
		values = append(values, v)
	}

	opName := getAExprOpName(ae)
	op := "in"
	if opName == "<>" {
		op = "not in"
	}
	return fmt.Sprintf("%s %s [%s]", left, op, strings.Join(values, ", ")), nil
}

// convertLike handles AEXPR_LIKE: LIKE and NOT LIKE.
// LIKE: Name[0] = "~~", NOT LIKE: Name[0] = "!~~"
func convertLike(ae *pg_query.A_Expr) (string, error) {
	left, err := convertOperand(ae.Lexpr)
	if err != nil {
		return "", err
	}
	right, err := convertOperand(ae.Rexpr)
	if err != nil {
		return "", err
	}

	opName := getAExprOpName(ae)
	op := "like"
	if opName == "!~~" {
		op = "not like"
	}
	return fmt.Sprintf("%s %s %s", left, op, right), nil
}

// convertBetween handles AEXPR_BETWEEN and AEXPR_NOT_BETWEEN.
// Rexpr is a List with 2 elements: [from, to].
func convertBetween(ae *pg_query.A_Expr, not bool) (string, error) {
	col, err := convertOperand(ae.Lexpr)
	if err != nil {
		return "", err
	}

	// Rexpr should be a List with exactly 2 elements
	list := ae.Rexpr.GetList()
	if list == nil || len(list.Items) != 2 {
		return "", fmt.Errorf("BETWEEN requires 2 bounds")
	}

	from, err := convertOperand(list.Items[0])
	if err != nil {
		return "", err
	}
	to, err := convertOperand(list.Items[1])
	if err != nil {
		return "", err
	}

	if not {
		return fmt.Sprintf("(%s < %s or %s > %s)", col, from, col, to), nil
	}
	return fmt.Sprintf("(%s >= %s and %s <= %s)", col, from, col, to), nil
}

// convertNullTest handles IS NULL and IS NOT NULL.
func convertNullTest(nt *pg_query.NullTest) (string, error) {
	left, err := convertOperand(nt.Arg)
	if err != nil {
		return "", err
	}

	switch nt.Nulltesttype {
	case pg_query.NullTestType_IS_NULL:
		return fmt.Sprintf("%s == null", left), nil
	case pg_query.NullTestType_IS_NOT_NULL:
		return fmt.Sprintf("%s != null", left), nil
	default:
		return "", fmt.Errorf("unexpected NullTestType: %v", nt.Nulltesttype)
	}
}

// convertOperand converts an expression used as a comparison operand
// to a Milvus expression string.
func convertOperand(node *pg_query.Node) (string, error) {
	if node == nil {
		return "", fmt.Errorf("nil operand")
	}

	// ColumnRef
	if cr := node.GetColumnRef(); cr != nil {
		col, err := columnRefToColumnRef(cr)
		if err != nil {
			return "", err
		}
		return col.MilvusExpr(), nil
	}

	// A_Expr: could be JSON path or arithmetic
	if ae := node.GetAExpr(); ae != nil {
		opName := getAExprOpName(ae)
		if opName == "->" || opName == "->>" {
			col, err := extractJSONPath(ae)
			if err != nil {
				return "", err
			}
			return col.MilvusExpr(), nil
		}
		// Arithmetic in WHERE: col + 1 etc.
		left, err := convertOperand(ae.Lexpr)
		if err != nil {
			return "", err
		}
		right, err := convertOperand(ae.Rexpr)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s %s %s", left, opName, right), nil
	}

	// TypeCast: ignore the cast, use inner expression
	if tc := node.GetTypeCast(); tc != nil {
		return convertOperand(tc.Arg)
	}

	// String constant
	if ac := node.GetAConst(); ac != nil {
		if sv := ac.GetSval(); sv != nil {
			return fmt.Sprintf(`"%s"`, sv.Sval), nil
		}
		if iv := ac.GetIval(); iv != nil {
			return strconv.FormatInt(int64(iv.Ival), 10), nil
		}
		if fv := ac.GetFval(); fv != nil {
			return fv.Fval, nil
		}
		if ac.Isnull {
			return "null", nil
		}
		return "", fmt.Errorf("unsupported A_Const type")
	}

	// FuncCall in WHERE
	if fc := node.GetFuncCall(); fc != nil {
		return "", fmt.Errorf("function calls in WHERE not yet supported: %s", getFuncName(fc))
	}

	// List (tuple for IN expressions)
	if list := node.GetList(); list != nil {
		var parts []string
		for _, item := range list.Items {
			p, err := convertOperand(item)
			if err != nil {
				return "", err
			}
			parts = append(parts, p)
		}
		return fmt.Sprintf("[%s]", strings.Join(parts, ", ")), nil
	}

	// ParamRef
	if pr := node.GetParamRef(); pr != nil {
		return fmt.Sprintf("$%d", pr.Number), nil
	}

	return "", fmt.Errorf("unsupported operand type: %T", node.Node)
}

// aexprOpToMilvus converts a PG operator name string to a Milvus operator string.
func aexprOpToMilvus(opName string) string {
	switch opName {
	case "=":
		return "=="
	case "<>":
		return "!="
	case "<":
		return "<"
	case "<=":
		return "<="
	case ">":
		return ">"
	case ">=":
		return ">="
	default:
		return opName
	}
}
