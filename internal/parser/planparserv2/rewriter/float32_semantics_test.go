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

package rewriter_test

import (
	"fmt"
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Evaluate the original plan independently with the executor's literal
// precision. 0/1/2 represent FALSE/UNKNOWN/TRUE, including outer NOT on NULL.
func evalFloatRewritePlan(t *testing.T, e *planpb.Expr, row *float64) int {
	t.Helper()
	number := func(v *planpb.GenericValue, col *planpb.ColumnInfo, unary bool) float64 {
		n := v.GetFloatVal()
		if _, ok := v.GetVal().(*planpb.GenericValue_Int64Val); ok {
			n = float64(v.GetInt64Val())
		}
		// Scalar and element-level FLOAT predicates narrow every operator.
		// Fixed ARRAY<FLOAT> indices narrow UnaryRange only; Term and
		// BinaryRange dispatch to double-valued array executors.
		_, floatLiteral := v.GetVal().(*planpb.GenericValue_FloatVal)
		narrow := col.GetDataType() == schemapb.DataType_Float ||
			(col.GetDataType() == schemapb.DataType_Array && col.GetElementType() == schemapb.DataType_Float && (col.GetIsElementLevel() || (unary && floatLiteral)))
		if narrow {
			return float64(float32(n))
		}
		return n
	}
	truth := func(match bool) int {
		if row == nil {
			return 1
		}
		if match {
			return 2
		}
		return 0
	}
	switch n := e.GetExpr().(type) {
	case *planpb.Expr_AlwaysTrueExpr:
		return 2
	case *planpb.Expr_UnaryExpr:
		return 2 - evalFloatRewritePlan(t, n.UnaryExpr.GetChild(), row)
	case *planpb.Expr_BinaryExpr:
		l := evalFloatRewritePlan(t, n.BinaryExpr.GetLeft(), row)
		r := evalFloatRewritePlan(t, n.BinaryExpr.GetRight(), row)
		if n.BinaryExpr.GetOp() == planpb.BinaryExpr_LogicalAnd {
			return min(l, r)
		}
		return max(l, r)
	case *planpb.Expr_TermExpr:
		if row == nil {
			return 1
		}
		for _, v := range n.TermExpr.GetValues() {
			if *row == number(v, n.TermExpr.GetColumnInfo(), false) {
				return 2
			}
		}
		return 0
	case *planpb.Expr_UnaryRangeExpr:
		if row == nil {
			return 1
		}
		v := number(n.UnaryRangeExpr.GetValue(), n.UnaryRangeExpr.GetColumnInfo(), true)
		switch n.UnaryRangeExpr.GetOp() {
		case planpb.OpType_Equal:
			return truth(*row == v)
		case planpb.OpType_NotEqual:
			return truth(*row != v)
		case planpb.OpType_GreaterThan:
			return truth(*row > v)
		case planpb.OpType_GreaterEqual:
			return truth(*row >= v)
		case planpb.OpType_LessThan:
			return truth(*row < v)
		case planpb.OpType_LessEqual:
			return truth(*row <= v)
		}
	case *planpb.Expr_BinaryRangeExpr:
		if row == nil {
			return 1
		}
		r := n.BinaryRangeExpr
		l, u := number(r.GetLowerValue(), r.GetColumnInfo(), false), number(r.GetUpperValue(), r.GetColumnInfo(), false)
		return truth((*row > l || (*row == l && r.GetLowerInclusive())) && (*row < u || (*row == u && r.GetUpperInclusive())))
	}
	t.Fatalf("unsupported float oracle plan %s", e)
	return 0
}

func TestRewriteFloat32AlgebraPreservesExecutorSemantics(t *testing.T) {
	columns := []struct {
		name string
		col  *planpb.ColumnInfo
	}{
		{"scalar_float", &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Float}},
		{"element_float", &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float, IsElementLevel: true}},
		{"double", &planpb.ColumnInfo{FieldId: 103, DataType: schemapb.DataType_Double}},
		{"json", &planpb.ColumnInfo{FieldId: 104, DataType: schemapb.DataType_JSON, NestedPath: []string{"n"}}},
	}
	for _, column := range columns {
		for _, nullable := range []bool{false, true} {
			c := proto.Clone(column.col).(*planpb.ColumnInfo)
			c.Nullable = nullable
			value := edgeTestFloatValue
			unary := func(op planpb.OpType, n float64) *planpb.Expr { return edgeTestUnaryRange(c, op, value(n)) }
			term := func(ns ...float64) *planpb.Expr {
				vs := make([]*planpb.GenericValue, len(ns))
				for i, n := range ns {
					vs[i] = value(n)
				}
				return edgeTestTerm(c, vs...)
			}
			and := func(l, r *planpb.Expr) *planpb.Expr { return edgeTestLogical(planpb.BinaryExpr_LogicalAnd, l, r) }
			cases := []struct {
				name string
				expr *planpb.Expr
			}{
				{"in_equal", and(term(16777216, 2), unary(planpb.OpType_Equal, 16777217))},
				{"in_intersection", and(term(16777216, 2), term(16777217, 3))},
				{"range_tighten", and(unary(planpb.OpType_GreaterThan, 16777216), unary(planpb.OpType_GreaterEqual, 16777217))},
				{"in_range", and(term(16777217, 2), unary(planpb.OpType_GreaterThan, 16777216))},
				{"multiple_equal", and(term(16777216, 2), and(unary(planpb.OpType_Equal, 16777216), unary(planpb.OpType_Equal, 16777217)))},
				{"narrowed_duplicate_classes", and(term(16777216, 16777216.25, 16777217, 16777218), term(16777216.5, 16777217, 16777218))},
				{"binary_range", and(edgeTestBinaryRange(c, value(16777216), value(16777220)), unary(planpb.OpType_GreaterThan, 16777217))},
				{"or_range", edgeTestLogical(planpb.BinaryExpr_LogicalOr, unary(planpb.OpType_GreaterThan, 16777216), unary(planpb.OpType_GreaterEqual, 16777217))},
				{"overflow_to_infinity", and(term(1e40, 2), unary(planpb.OpType_Equal, 2e40))},
				{"nan_intersection", and(term(math.NaN(), 2), term(math.NaN(), 2, 3))},
				{"signed_zero", and(term(math.Copysign(0, -1), 2), unary(planpb.OpType_Equal, 0))},
				{"in_not_equal", and(term(16777217, 2), unary(planpb.OpType_NotEqual, 16777216))},
			}
			for _, tc := range cases {
				for _, optimize := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/nullable_%t/%s/optimize_%t", column.name, nullable, tc.name, optimize), func(t *testing.T) {
						original := proto.Clone(tc.expr).(*planpb.Expr)
						rewritten := rewriter.RewriteExprWithConfig(proto.Clone(tc.expr).(*planpb.Expr), optimize)
						rows := []float64{math.Copysign(0, -1), 0, 2, 3, 16777216, 16777218, 16777220, math.Inf(1), math.NaN()}
						for _, r := range rows {
							require.Equal(t, evalFloatRewritePlan(t, original, &r), evalFloatRewritePlan(t, rewritten, &r), "row %v; plan %s", r, rewritten)
						}
						if nullable {
							require.Equal(t, evalFloatRewritePlan(t, original, nil), evalFloatRewritePlan(t, rewritten, nil))
							original = &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{Op: planpb.UnaryExpr_Not, Child: original}}}
							rewritten = rewriter.RewriteExprWithConfig(&planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{Op: planpb.UnaryExpr_Not, Child: rewritten}}}, optimize)
							require.Equal(t, evalFloatRewritePlan(t, original, nil), evalFloatRewritePlan(t, rewritten, nil))
						}
					})
				}
			}
		}
	}
}

// Fixed ARRAY<FLOAT> access has mixed operator precision, so it cannot use a
// single column-level precision flag or serve as an equivalence control for
// scalar FLOAT rewrites.
func TestFixedArrayFloatOperatorPrecision(t *testing.T) {
	col := &planpb.ColumnInfo{FieldId: 102, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Float, NestedPath: []string{"0"}}
	row := float64(16777216)
	unary := edgeTestUnaryRange(col, planpb.OpType_GreaterEqual, edgeTestFloatValue(16777217))
	term := edgeTestTerm(col, edgeTestFloatValue(16777217))
	binary := edgeTestBinaryRange(col, edgeTestFloatValue(16777217), edgeTestFloatValue(16777220))
	require.Equal(t, 2, evalFloatRewritePlan(t, unary, &row), "UnaryRange narrows 16777217 to 16777216")
	require.Equal(t, 0, evalFloatRewritePlan(t, term, &row), "Term retains the double literal")
	require.Equal(t, 0, evalFloatRewritePlan(t, binary, &row), "BinaryRange retains the double bounds")

}

func TestRewriteFloat32ParsedAndTemplateFilters(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 110, Name: "f", DataType: schemapb.DataType_Float}}})
	require.NoError(t, err)
	for _, optimize := range []bool{false, true} {
		old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(strconv.FormatBool(optimize))
		func() {
			defer paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old)
			for _, tc := range []struct {
				filter string
				values map[string]*schemapb.TemplateValue
				want   int
			}{
				{`f IN [16777216.0,2.0] AND f == 16777217.0`, nil, 2},
				{`f > 16777216.0 AND f >= 16777217.0`, nil, 0},
				{`f IN {vs} AND f == {v}`, map[string]*schemapb.TemplateValue{"vs": {Val: &schemapb.TemplateValue_ArrayVal{ArrayVal: &schemapb.TemplateArrayValue{Data: &schemapb.TemplateArrayValue_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{16777216, 2}}}}}}, "v": {Val: &schemapb.TemplateValue_FloatVal{FloatVal: 16777217}}}, 2},
			} {
				e, err := parser.ParseExpr(helper, tc.filter, tc.values)
				require.NoError(t, err)
				row := float64(16777216)
				require.Equal(t, tc.want, evalFloatRewritePlan(t, e, &row), "optimize=%t %s", optimize, tc.filter)
			}
		}()
	}
}
