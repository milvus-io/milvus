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
	"math"
	"strconv"
	"strings"
	"time"

	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// aggFuncSet is the set of aggregate function names recognized during projection.
var aggFuncSet = map[string]bool{
	"count": true, "sum": true, "avg": true, "min": true, "max": true,
}

// ApplyProjection evaluates SelectComputed items against reducer output columns
// and assembles the final output in SELECT order with correct aliases.
//
// Non-computed items are passed through from reducerFields in order.
// Computed items are evaluated from their RawExpr AST using all reducer columns as inputs.
// Hidden columns (group-by keys and aggregates for computed expressions) are available
// to the evaluator but not consumed by the pass-through index.
func ApplyProjection(
	reducerFields []*schemapb.FieldData,
	comp *SqlComponents,
) ([]*schemapb.FieldData, error) {
	hasComputed := false
	for _, si := range comp.SelectItems {
		if si.Type == SelectComputed {
			hasComputed = true
			break
		}
	}
	if !hasComputed {
		return reducerFields, nil
	}

	ctx := newEvalContext(reducerFields)

	// Count how many reducer columns are consumed by visible (non-computed) SELECT items.
	// Hidden columns (e.g., GROUP BY keys for computed aliases) sit between visible
	// columns in the reducer output. We need to skip them during pass-through.
	visibleCount := 0
	for _, si := range comp.SelectItems {
		if si.Type != SelectComputed {
			visibleCount++
		}
	}
	hiddenCount := len(reducerFields) - visibleCount
	// Hidden columns appear after the visible group-by keys and before aggregates.
	// In practice they are appended at the end of the GROUP BY key section.
	// We track them by building a set of visible column names to identify which
	// reducer columns to skip.

	// Build ordered list: for each reducer column, determine if it's a hidden column.
	// Hidden columns are those in reducerFields that don't correspond to any
	// visible SelectItem (SelectColumn/SelectAgg/SelectStar).
	visibleNames := make(map[string]bool)
	for _, si := range comp.SelectItems {
		switch si.Type {
		case SelectColumn:
			if si.Column != nil {
				visibleNames[si.Column.MilvusExpr()] = true
			}
		case SelectAgg:
			if si.AggFunc != nil {
				// Register all possible name formats:
				// "count(*)", "min(field)", "min(data[\"time_us\"])"
				afc := si.AggFunc
				if afc.Arg == nil {
					visibleNames[fmt.Sprintf("%s(*)", afc.FuncName)] = true
				} else {
					visibleNames[fmt.Sprintf("%s(%s)", afc.FuncName, afc.Arg.MilvusExpr())] = true
				}
				// Also register resolved form (nested function like min(to_timestamp(...)))
				if col := extractDeepColumnRef(si.RawExpr); col != nil {
					visibleNames[fmt.Sprintf("%s(%s)", afc.FuncName, col.MilvusExpr())] = true
				}
			}
		case SelectStar:
			visibleNames["*"] = true
		}
	}
	_ = hiddenCount // used for documentation only

	var result []*schemapb.FieldData
	reducerIdx := 0

	for _, si := range comp.SelectItems {
		switch si.Type {
		case SelectColumn, SelectAgg, SelectStar:
			// Skip hidden reducer columns that don't match any visible SELECT item
			for reducerIdx < len(reducerFields) {
				name := reducerFields[reducerIdx].GetFieldName()
				if visibleNames[name] {
					break
				}
				reducerIdx++ // skip hidden column
			}
			if reducerIdx >= len(reducerFields) {
				return nil, fmt.Errorf("reducer output exhausted at item %q", si.Alias)
			}
			fd := reducerFields[reducerIdx]
			reducerIdx++
			if si.Alias != "" && fd.GetFieldName() != si.Alias {
				fd = shallowCopyFieldData(fd)
				fd.FieldName = si.Alias
			}
			result = append(result, fd)

		case SelectComputed:
			if si.RawExpr == nil {
				return nil, fmt.Errorf("computed item %q has no RawExpr", si.Alias)
			}
			values, err := ctx.eval(si.RawExpr)
			if err != nil {
				return nil, fmt.Errorf("project %q: %w", si.Alias, err)
			}
			alias := si.Alias
			if alias == "" {
				alias = fmt.Sprintf("_expr_%d", len(result))
			}
			result = append(result, toFieldData(alias, values))
		}
	}

	return result, nil
}

// toFieldData converts a float64 slice to an appropriate FieldData.
// Uses Int64 when all values are whole numbers, Double otherwise.
func toFieldData(name string, values []float64) *schemapb.FieldData {
	allInt := true
	for _, v := range values {
		if v != math.Trunc(v) || v > math.MaxInt64 || v < math.MinInt64 {
			allInt = false
			break
		}
	}
	if allInt {
		ints := make([]int64, len(values))
		for i, v := range values {
			ints[i] = int64(v)
		}
		return newInt64FieldData(name, ints)
	}
	return newFloat64FieldData(name, values)
}

// evalContext holds reducer output columns as float64 arrays for expression evaluation.
type evalContext struct {
	columns  map[string][]float64
	rowCount int
}

func newEvalContext(fields []*schemapb.FieldData) *evalContext {
	columns := make(map[string][]float64, len(fields)*2)
	rowCount := 0
	for _, fd := range fields {
		vals := fieldDataToFloat64(fd)
		name := fd.GetFieldName()
		columns[name] = vals
		if len(vals) > rowCount {
			rowCount = len(vals)
		}
		// Virtual column names from segcore use "field/path1/path2" format,
		// but AST expressions produce "field[\"path1\"][\"path2\"]" (MilvusExpr).
		// Register both so eval can find the column regardless of format.
		if strings.Contains(name, "/") {
			parts := strings.SplitN(name, "/", 2)
			if len(parts) == 2 {
				milvusName := parts[0]
				for _, p := range strings.Split(parts[1], "/") {
					milvusName += fmt.Sprintf("[\"%s\"]", p)
				}
				columns[milvusName] = vals
			}
		}
	}
	return &evalContext{columns: columns, rowCount: rowCount}
}

// withOverride returns a new context where colName maps to the given values,
// keeping all other columns intact.
func (ctx *evalContext) withOverride(colName string, values []float64) *evalContext {
	cols := make(map[string][]float64, len(ctx.columns)+1)
	for k, v := range ctx.columns {
		cols[k] = v
	}
	cols[colName] = values
	return &evalContext{columns: cols, rowCount: ctx.rowCount}
}

// eval evaluates a pg_query.Node to a float64 array (one value per row).
func (ctx *evalContext) eval(node *pg_query.Node) ([]float64, error) {
	if node == nil {
		return nil, fmt.Errorf("nil expression")
	}

	if fc := node.GetFuncCall(); fc != nil {
		return ctx.evalFunc(fc, node)
	}
	if ae := node.GetAExpr(); ae != nil {
		return ctx.evalAExpr(ae)
	}
	if tc := node.GetTypeCast(); tc != nil {
		return ctx.eval(tc.Arg)
	}
	if ac := node.GetAConst(); ac != nil {
		return ctx.evalConst(ac)
	}
	if cr := node.GetColumnRef(); cr != nil {
		return ctx.evalColumnRef(cr)
	}

	return nil, fmt.Errorf("unsupported expression type in projection: %T", node.Node)
}

// evalFunc evaluates a function expression.
// node is the full pg_query.Node wrapping the FuncCall (needed for extractDeepColumnRef).
func (ctx *evalContext) evalFunc(fc *pg_query.FuncCall, node *pg_query.Node) ([]float64, error) {
	name := getFuncName(fc)

	// Aggregate function -> look up in reducer output, then apply wrapping expression
	if aggFuncSet[name] {
		return ctx.evalAggRef(fc, node, name)
	}

	switch name {
	case "extract":
		return ctx.evalExtract(fc)
	case "to_timestamp":
		// to_timestamp(seconds) is an identity for our numeric representation
		if len(fc.Args) < 1 {
			return nil, fmt.Errorf("to_timestamp requires 1 argument")
		}
		return ctx.eval(fc.Args[0])
	default:
		return nil, fmt.Errorf("unsupported function in projection: %s", name)
	}
}

// evalAggRef looks up an aggregate result from the reducer output.
//
// For simple aggregates like count(*), it returns the reducer column directly.
// For aggregates wrapping expressions (e.g., max(to_timestamp(x / 1e6))),
// it returns the aggregated raw value with the wrapping expression applied.
// This works because for monotonic functions f: max(f(x)) = f(max(x)).
func (ctx *evalContext) evalAggRef(fc *pg_query.FuncCall, node *pg_query.Node, name string) ([]float64, error) {
	// count(*) with no argument
	if len(fc.Args) == 0 || (name == "count" && fc.AggStar) {
		colName := "count(*)"
		if vals, ok := ctx.columns[colName]; ok {
			return vals, nil
		}
		return nil, fmt.Errorf("aggregate column %q not found", colName)
	}

	// Find innermost column reference from the aggregate's argument tree
	innerCol := extractDeepColumnRef(node)
	if innerCol == nil {
		return nil, fmt.Errorf("cannot resolve column for %s()", name)
	}

	colExpr := innerCol.MilvusExpr()
	colName := fmt.Sprintf("%s(%s)", name, colExpr)

	aggValues, ok := ctx.columns[colName]
	if !ok {
		var keys []string
		for k := range ctx.columns {
			keys = append(keys, fmt.Sprintf("%q(len=%d)", k, len(ctx.columns[k])))
		}
		return nil, fmt.Errorf("aggregate column %q not found in reducer output (available: %v)", colName, keys)
	}

	// If the aggregate argument is a direct column ref, return the aggregated values as-is.
	if isDirectColumnRef(fc.Args[0]) {
		return aggValues, nil
	}

	// The aggregate wraps a non-trivial expression (e.g., max(to_timestamp(x / 1e6))).
	// Create an override context where the inner column resolves to the aggregated values,
	// then evaluate the wrapping expression to transform them.
	overrideCtx := ctx.withOverride(colExpr, aggValues)
	return overrideCtx.eval(fc.Args[0])
}

// isDirectColumnRef checks if a node is a plain column reference
// (possibly with JSON path) without wrapping functions or arithmetic.
func isDirectColumnRef(node *pg_query.Node) bool {
	if node == nil {
		return false
	}
	if node.GetColumnRef() != nil {
		return true
	}
	if ae := node.GetAExpr(); ae != nil {
		opName := getAExprOpName(ae)
		return opName == "->" || opName == "->>"
	}
	if tc := node.GetTypeCast(); tc != nil {
		// ::type on a column ref is still "direct" for our purposes
		return isDirectColumnRef(tc.Arg)
	}
	return false
}

// evalExtract evaluates EXTRACT(field FROM source).
// In the PG parser, EXTRACT is a FuncCall with Funcname=["pg_catalog","extract"]
// and 2 args: field name constant and source expression.
func (ctx *evalContext) evalExtract(fc *pg_query.FuncCall) ([]float64, error) {
	if len(fc.Args) < 2 {
		return nil, fmt.Errorf("extract requires 2 arguments (field, source), got %d", len(fc.Args))
	}

	field, err := extractFieldName(fc.Args[0])
	if err != nil {
		return nil, fmt.Errorf("extract field: %w", err)
	}

	source, err := ctx.eval(fc.Args[1])
	if err != nil {
		return nil, fmt.Errorf("extract source: %w", err)
	}

	result := make([]float64, len(source))
	for i, v := range source {
		result[i] = extractTimeField(field, v)
	}
	return result, nil
}

// extractFieldName gets the field name string from an extract() first argument.
func extractFieldName(node *pg_query.Node) (string, error) {
	if node == nil {
		return "", fmt.Errorf("nil extract field argument")
	}

	// ColumnRef: e.g. EXTRACT(hour FROM ...) parses "hour" as a column ref
	if cr := node.GetColumnRef(); cr != nil {
		if len(cr.Fields) > 0 {
			if s := cr.Fields[0].GetString_(); s != nil {
				return strings.ToLower(s.Sval), nil
			}
		}
	}

	// A_Const string: sometimes the field comes as a string constant
	if ac := node.GetAConst(); ac != nil {
		if sv := ac.GetSval(); sv != nil {
			return strings.ToLower(sv.Sval), nil
		}
	}

	// Try String() as fallback
	s := fmt.Sprintf("%v", node)
	if s != "" {
		return strings.ToLower(s), nil
	}
	return "", fmt.Errorf("unsupported extract field type: %T", node.Node)
}

// extractTimeField extracts a time component from a Unix timestamp (seconds).
func extractTimeField(field string, unixSeconds float64) float64 {
	switch field {
	case "epoch":
		return unixSeconds
	default:
		sec := int64(unixSeconds)
		nsec := int64((unixSeconds - float64(sec)) * 1e9)
		t := time.Unix(sec, nsec).UTC()
		switch field {
		case "hour":
			return float64(t.Hour())
		case "minute":
			return float64(t.Minute())
		case "second":
			return float64(t.Second()) + float64(t.Nanosecond())/1e9
		case "day":
			return float64(t.Day())
		case "month":
			return float64(t.Month())
		case "year":
			return float64(t.Year())
		case "dow", "day_of_week":
			return float64(t.Weekday())
		case "doy", "day_of_year":
			return float64(t.YearDay())
		default:
			return 0
		}
	}
}

// evalAExpr evaluates an A_Expr (binary operator expression).
func (ctx *evalContext) evalAExpr(ae *pg_query.A_Expr) ([]float64, error) {
	opName := getAExprOpName(ae)

	// JSON operators -> column lookup
	if opName == "->" || opName == "->>" {
		col, err := extractJSONPath(ae)
		if err != nil {
			return nil, fmt.Errorf("JSON path: %w", err)
		}
		name := col.MilvusExpr()
		if vals, ok := ctx.columns[name]; ok {
			return vals, nil
		}
		return nil, fmt.Errorf("column %q not found in reducer output", name)
	}

	// Arithmetic operators
	left, err := ctx.eval(ae.Lexpr)
	if err != nil {
		return nil, err
	}
	right, err := ctx.eval(ae.Rexpr)
	if err != nil {
		return nil, err
	}

	// Broadcast scalar to match array length
	left, right = broadcastPair(left, right)
	if len(left) != len(right) {
		return nil, fmt.Errorf("mismatched lengths: %d vs %d", len(left), len(right))
	}

	result := make([]float64, len(left))
	for i := range left {
		switch opName {
		case "+":
			result[i] = left[i] + right[i]
		case "-":
			result[i] = left[i] - right[i]
		case "*":
			result[i] = left[i] * right[i]
		case "/", "//":
			if right[i] == 0 {
				result[i] = 0
			} else {
				result[i] = left[i] / right[i]
			}
		case "%":
			if right[i] == 0 {
				result[i] = 0
			} else {
				result[i] = math.Mod(left[i], right[i])
			}
		default:
			return nil, fmt.Errorf("unsupported binary operator: %s", opName)
		}
	}
	return result, nil
}

// evalConst evaluates an A_Const (integer, float, or string literal).
func (ctx *evalContext) evalConst(ac *pg_query.A_Const) ([]float64, error) {
	if iv := ac.GetIval(); iv != nil {
		return []float64{float64(iv.Ival)}, nil
	}
	if fv := ac.GetFval(); fv != nil {
		v, err := strconv.ParseFloat(fv.Fval, 64)
		if err != nil {
			return nil, fmt.Errorf("parse float %q: %w", fv.Fval, err)
		}
		return []float64{v}, nil
	}
	if sv := ac.GetSval(); sv != nil {
		// Try parsing as number (for string literals used in numeric context)
		v, err := strconv.ParseFloat(sv.Sval, 64)
		if err != nil {
			return nil, fmt.Errorf("string %q is not numeric", sv.Sval)
		}
		return []float64{v}, nil
	}
	return nil, fmt.Errorf("unsupported A_Const type")
}

// evalColumnRef evaluates a column reference by name.
func (ctx *evalContext) evalColumnRef(cr *pg_query.ColumnRef) ([]float64, error) {
	if len(cr.Fields) == 0 {
		return nil, fmt.Errorf("empty column reference")
	}
	s := cr.Fields[0].GetString_()
	if s == nil {
		return nil, fmt.Errorf("unsupported column ref field type")
	}
	name := s.Sval
	if vals, ok := ctx.columns[name]; ok {
		return vals, nil
	}
	return nil, fmt.Errorf("column %q not found in reducer output", name)
}

// broadcastPair ensures both slices have the same length by broadcasting scalars.
func broadcastPair(a, b []float64) ([]float64, []float64) {
	if len(a) == 1 && len(b) > 1 {
		a = broadcast(a[0], len(b))
	}
	if len(b) == 1 && len(a) > 1 {
		b = broadcast(b[0], len(a))
	}
	return a, b
}

// broadcast creates a slice of n identical values.
func broadcast(val float64, n int) []float64 {
	result := make([]float64, n)
	for i := range result {
		result[i] = val
	}
	return result
}

// --- FieldData helpers ---

// fieldDataToFloat64 converts a FieldData to a float64 slice.
func fieldDataToFloat64(fd *schemapb.FieldData) []float64 {
	if fd == nil {
		return nil
	}
	switch fd.GetType() {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		intData := fd.GetScalars().GetIntData().GetData()
		result := make([]float64, len(intData))
		for i, v := range intData {
			result[i] = float64(v)
		}
		return result
	case schemapb.DataType_Int64:
		longData := fd.GetScalars().GetLongData().GetData()
		result := make([]float64, len(longData))
		for i, v := range longData {
			result[i] = float64(v)
		}
		return result
	case schemapb.DataType_Float:
		floatData := fd.GetScalars().GetFloatData().GetData()
		result := make([]float64, len(floatData))
		for i, v := range floatData {
			result[i] = float64(v)
		}
		return result
	case schemapb.DataType_Double:
		return append([]float64{}, fd.GetScalars().GetDoubleData().GetData()...)
	case schemapb.DataType_VarChar, schemapb.DataType_String:
		strData := fd.GetScalars().GetStringData().GetData()
		result := make([]float64, len(strData))
		for i, s := range strData {
			// JSON extraction may produce quoted strings like "\"1700000000\"".
			// Strip surrounding quotes before parsing as float.
			s = strings.TrimPrefix(s, "\"")
			s = strings.TrimSuffix(s, "\"")
			v, err := strconv.ParseFloat(s, 64)
			if err == nil {
				result[i] = v
			}
		}
		return result
	default:
		return nil
	}
}

// newFloat64FieldData creates a FieldData with Double type.
func newFloat64FieldData(name string, values []float64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Double,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{Data: values},
				},
			},
		},
	}
}

// newInt64FieldData creates a FieldData with Int64 type.
func newInt64FieldData(name string, values []int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: name,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: values},
				},
			},
		},
	}
}

// shallowCopyFieldData returns a copy that shares the underlying data arrays.
func shallowCopyFieldData(fd *schemapb.FieldData) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      fd.GetType(),
		FieldName: fd.GetFieldName(),
		FieldId:   fd.GetFieldId(),
		Field:     fd.GetField(),
		IsDynamic: fd.GetIsDynamic(),
		ValidData: fd.GetValidData(),
	}
}

// --- Nested aggregate extraction ---

// NestedAggregate represents an aggregate call found inside a computed expression.
type NestedAggregate struct {
	FuncName string
	Column   *ColumnRef
}

// ExtractNestedAggregates scans a computed expression tree and collects
// any aggregate function calls found inside it.
func ExtractNestedAggregates(node *pg_query.Node) []NestedAggregate {
	var result []NestedAggregate
	collectNestedAggs(node, &result)
	return result
}

func collectNestedAggs(node *pg_query.Node, result *[]NestedAggregate) {
	if node == nil {
		return
	}

	if fc := node.GetFuncCall(); fc != nil {
		name := getFuncName(fc)
		if aggFuncSet[name] {
			col := extractDeepColumnRef(node)
			if col != nil {
				*result = append(*result, NestedAggregate{FuncName: name, Column: col})
			}
			return // Don't descend into aggregate arguments
		}
		for _, arg := range fc.Args {
			collectNestedAggs(arg, result)
		}
		return
	}

	if ae := node.GetAExpr(); ae != nil {
		collectNestedAggs(ae.Lexpr, result)
		collectNestedAggs(ae.Rexpr, result)
		return
	}

	if tc := node.GetTypeCast(); tc != nil {
		collectNestedAggs(tc.Arg, result)
		return
	}
}

// ExtractDeepColumnRef is the exported version of extractDeepColumnRef.
func ExtractDeepColumnRef(node *pg_query.Node) *ColumnRef {
	return extractDeepColumnRef(node)
}
