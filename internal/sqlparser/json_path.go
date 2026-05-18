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
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// getAExprOpName extracts the operator name string from an A_Expr.
// Returns empty string if no name is available.
func getAExprOpName(ae *pg_query.A_Expr) string {
	if len(ae.Name) > 0 {
		if s := ae.Name[0].GetString_(); s != nil {
			return s.Sval
		}
	}
	return ""
}

// getFuncName extracts the function name from a FuncCall.
// Takes the LAST element of Funcname because built-in functions like
// EXTRACT have Funcname = ["pg_catalog", "extract"].
func getFuncName(fc *pg_query.FuncCall) string {
	if len(fc.Funcname) == 0 {
		return ""
	}
	last := fc.Funcname[len(fc.Funcname)-1]
	if s := last.GetString_(); s != nil {
		return strings.ToLower(s.Sval)
	}
	return ""
}

// extractColumnRef attempts to extract a ColumnRef from a pg_query Node.
// It handles:
//   - Simple column references (ColumnRef)
//   - JSON operators: -> (JSONFetchVal) and ->> (JSONFetchText) via A_Expr
//   - Type casts: (expr)::type via TypeCast
//
// Returns nil if the node is not a column reference.
func extractColumnRef(node *pg_query.Node) (*ColumnRef, error) {
	if node == nil {
		return nil, nil
	}

	if cr := node.GetColumnRef(); cr != nil {
		return columnRefToColumnRef(cr)
	}

	if ae := node.GetAExpr(); ae != nil {
		opName := getAExprOpName(ae)
		if opName == "->" || opName == "->>" {
			return extractJSONPath(ae)
		}
		return nil, nil
	}

	if tc := node.GetTypeCast(); tc != nil {
		return extractTypeCast(tc)
	}

	return nil, nil
}

// columnRefToColumnRef converts a pg_query.ColumnRef to our ColumnRef IR type.
func columnRefToColumnRef(cr *pg_query.ColumnRef) (*ColumnRef, error) {
	if len(cr.Fields) == 0 {
		return nil, fmt.Errorf("empty column name")
	}
	// Get the first field name (ignoring table qualifiers for now)
	s := cr.Fields[0].GetString_()
	if s == nil {
		// Could be A_Star
		if cr.Fields[0].GetAStar() != nil {
			return &ColumnRef{FieldName: "*"}, nil
		}
		return nil, fmt.Errorf("unsupported column ref field type")
	}
	return &ColumnRef{FieldName: s.Sval}, nil
}

// extractJSONPath extracts a ColumnRef from a chain of -> / ->> operators.
//
// Example: data->'commit'->>'collection'
// AST:     A_Expr{ Lexpr: A_Expr{ Lexpr: ColumnRef("data"), Name: "->", Rexpr: "commit" },
//
//	Name: "->>", Rexpr: "collection" }
//
// Result:  ColumnRef{ FieldName: "data", NestedPath: ["commit", "collection"], IsText: true }
func extractJSONPath(ae *pg_query.A_Expr) (*ColumnRef, error) {
	opName := getAExprOpName(ae)
	if opName != "->" && opName != "->>" {
		return nil, nil // not a JSON operator
	}

	// Collect path elements from right to left
	var path []string
	isText := opName == "->>"

	// Extract the right operand (must be a string key)
	key, err := extractStringLiteral(ae.Rexpr)
	if err != nil {
		return nil, fmt.Errorf("JSON operator requires string key: %w", err)
	}
	path = append(path, key)

	// Walk the left side recursively
	current := ae.Lexpr
	for {
		if current == nil {
			return nil, fmt.Errorf("unexpected nil node in JSON path")
		}

		if innerAE := current.GetAExpr(); innerAE != nil {
			innerOp := getAExprOpName(innerAE)
			if innerOp != "->" && innerOp != "->>" {
				return nil, fmt.Errorf("unexpected operator in JSON path: %s", innerOp)
			}
			k, err := extractStringLiteral(innerAE.Rexpr)
			if err != nil {
				return nil, fmt.Errorf("JSON operator requires string key: %w", err)
			}
			path = append(path, k)
			current = innerAE.Lexpr
			continue
		}

		if cr := current.GetColumnRef(); cr != nil {
			col, err := columnRefToColumnRef(cr)
			if err != nil {
				return nil, err
			}
			// Reverse the path (we collected right-to-left)
			for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
				path[i], path[j] = path[j], path[i]
			}
			col.NestedPath = path
			col.IsText = isText
			return col, nil
		}

		return nil, fmt.Errorf("unexpected node type at base of JSON path: %T", current.Node)
	}
}

// extractTypeCast handles ::type cast expressions.
func extractTypeCast(tc *pg_query.TypeCast) (*ColumnRef, error) {
	col, err := extractColumnRef(tc.Arg)
	if err != nil {
		return nil, err
	}
	if col == nil {
		return nil, nil
	}

	// Extract type name from TypeName.Names
	if tc.TypeName != nil && len(tc.TypeName.Names) > 0 {
		// Take the last element of Names (e.g. ["pg_catalog", "int8"] -> "int8")
		last := tc.TypeName.Names[len(tc.TypeName.Names)-1]
		if s := last.GetString_(); s != nil {
			col.CastType = s.Sval
		}
	}
	return col, nil
}

// extractStringLiteral extracts a string value from a pg_query Node.
func extractStringLiteral(node *pg_query.Node) (string, error) {
	if node == nil {
		return "", fmt.Errorf("expected string literal, got nil")
	}
	if ac := node.GetAConst(); ac != nil {
		if sv := ac.GetSval(); sv != nil {
			return sv.Sval, nil
		}
	}
	return "", fmt.Errorf("expected string literal, got %T", node.Node)
}
