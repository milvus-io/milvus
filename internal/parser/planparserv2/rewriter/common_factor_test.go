package rewriter_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ---------------------------------------------------------------------------
// Three-valued oracle
//
// The oracle evaluates a planpb predicate against one row with SQL/Kleene
// semantics, which is what segcore's ConjunctExpr (AND/OR) and
// ThreeValuedLogicOp::Not implement. A NULL column makes a comparison UNKNOWN.
// Comparing the full TRUE/FALSE/UNKNOWN value (not only the rows that pass a
// WHERE) is what makes an equivalence safe under an outer NOT.
// ---------------------------------------------------------------------------

type tri int8

const (
	triFalse tri = iota
	triTrue
	triUnknown
)

func (v tri) String() string {
	return [...]string{"FALSE", "TRUE", "UNKNOWN"}[v]
}

func triAnd(a, b tri) tri {
	if a == triFalse || b == triFalse {
		return triFalse
	}
	if a == triTrue && b == triTrue {
		return triTrue
	}
	return triUnknown
}

func triOr(a, b tri) tri {
	if a == triTrue || b == triTrue {
		return triTrue
	}
	if a == triFalse && b == triFalse {
		return triFalse
	}
	return triUnknown
}

func triNot(a tri) tri {
	switch a {
	case triTrue:
		return triFalse
	case triFalse:
		return triTrue
	}
	return triUnknown
}

func boolTri(b bool) tri {
	if b {
		return triTrue
	}
	return triFalse
}

// oracleRow maps field ID to a Go value (int64, float64, string, bool); a
// missing or nil entry is NULL.
type oracleRow map[int64]any

// compareLiteral returns -1/0/1 comparing a row value with a literal.
func compareLiteral(t testing.TB, v any, lit *planpb.GenericValue) int {
	cmpFloat := func(a, b float64) int {
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		}
		return 0
	}
	switch x := v.(type) {
	case int64:
		switch l := lit.GetVal().(type) {
		case *planpb.GenericValue_Int64Val:
			switch {
			case x < l.Int64Val:
				return -1
			case x > l.Int64Val:
				return 1
			}
			return 0
		case *planpb.GenericValue_FloatVal:
			return cmpFloat(float64(x), l.FloatVal)
		}
	case float64:
		switch l := lit.GetVal().(type) {
		case *planpb.GenericValue_Int64Val:
			return cmpFloat(x, float64(l.Int64Val))
		case *planpb.GenericValue_FloatVal:
			return cmpFloat(x, l.FloatVal)
		}
	case string:
		if l, ok := lit.GetVal().(*planpb.GenericValue_StringVal); ok {
			return strings.Compare(x, l.StringVal)
		}
	case bool:
		if l, ok := lit.GetVal().(*planpb.GenericValue_BoolVal); ok {
			if x == l.BoolVal {
				return 0
			}
			if !x {
				return -1
			}
			return 1
		}
	}
	t.Fatalf("oracle: cannot compare %T with literal %v", v, lit)
	return 0
}

func evalCompare(t testing.TB, op planpb.OpType, v any, lit *planpb.GenericValue) tri {
	if op == planpb.OpType_PrefixMatch {
		return boolTri(strings.HasPrefix(v.(string), lit.GetStringVal()))
	}
	c := compareLiteral(t, v, lit)
	switch op {
	case planpb.OpType_GreaterThan:
		return boolTri(c > 0)
	case planpb.OpType_GreaterEqual:
		return boolTri(c >= 0)
	case planpb.OpType_LessThan:
		return boolTri(c < 0)
	case planpb.OpType_LessEqual:
		return boolTri(c <= 0)
	case planpb.OpType_Equal:
		return boolTri(c == 0)
	case planpb.OpType_NotEqual:
		return boolTri(c != 0)
	}
	t.Fatalf("oracle: unsupported op %v", op)
	return triUnknown
}

// evalOracle evaluates e on row.
func evalOracle(t testing.TB, e *planpb.Expr, row oracleRow, counter *int) tri {
	leaf := func() {
		if counter != nil {
			*counter++
		}
	}
	switch real := e.GetExpr().(type) {
	case *planpb.Expr_AlwaysTrueExpr:
		return triTrue
	case *planpb.Expr_ValueExpr:
		return boolTri(real.ValueExpr.GetValue().GetBoolVal())
	case *planpb.Expr_UnaryExpr:
		if real.UnaryExpr.GetOp() != planpb.UnaryExpr_Not {
			t.Fatalf("oracle: unsupported unary op %v", real.UnaryExpr.GetOp())
		}
		return triNot(evalOracle(t, real.UnaryExpr.GetChild(), row, counter))
	case *planpb.Expr_BinaryExpr:
		l := evalOracle(t, real.BinaryExpr.GetLeft(), row, counter)
		switch real.BinaryExpr.GetOp() {
		case planpb.BinaryExpr_LogicalAnd:
			if l == triFalse {
				return l
			}
			return triAnd(l, evalOracle(t, real.BinaryExpr.GetRight(), row, counter))
		case planpb.BinaryExpr_LogicalOr:
			if l == triTrue {
				return l
			}
			return triOr(l, evalOracle(t, real.BinaryExpr.GetRight(), row, counter))
		}
		t.Fatalf("oracle: unsupported binary op %v", real.BinaryExpr.GetOp())
	case *planpb.Expr_UnaryRangeExpr:
		leaf()
		u := real.UnaryRangeExpr
		v := row[u.GetColumnInfo().GetFieldId()]
		if v == nil {
			return triUnknown
		}
		return evalCompare(t, u.GetOp(), v, u.GetValue())
	case *planpb.Expr_BinaryRangeExpr:
		leaf()
		b := real.BinaryRangeExpr
		v := row[b.GetColumnInfo().GetFieldId()]
		if v == nil {
			return triUnknown
		}
		lo, hi := planpb.OpType_GreaterThan, planpb.OpType_LessThan
		if b.GetLowerInclusive() {
			lo = planpb.OpType_GreaterEqual
		}
		if b.GetUpperInclusive() {
			hi = planpb.OpType_LessEqual
		}
		return triAnd(evalCompare(t, lo, v, b.GetLowerValue()), evalCompare(t, hi, v, b.GetUpperValue()))
	case *planpb.Expr_TermExpr:
		leaf()
		te := real.TermExpr
		v := row[te.GetColumnInfo().GetFieldId()]
		if v == nil {
			return triUnknown
		}
		for _, val := range te.GetValues() {
			if compareLiteral(t, v, val) == 0 {
				return triTrue
			}
		}
		return triFalse
	case *planpb.Expr_NullExpr:
		leaf()
		isNull := row[real.NullExpr.GetColumnInfo().GetFieldId()] == nil
		if real.NullExpr.GetOp() == planpb.NullExpr_IsNull {
			return boolTri(isNull)
		}
		return boolTri(!isNull)
	}
	t.Fatalf("oracle: unsupported expression %T", e.GetExpr())
	return triUnknown
}

// evalCounting evaluates e on row like segcore with expression optimization
// enabled and counts the leaf predicates it runs. ConjunctExpr evaluates the
// operands of a flattened AND/OR chain with column predicates before nested
// conjunctions (ReorderConjunctExpr) and stops a row once AND is FALSE or OR is
// TRUE.
func evalCounting(t testing.TB, e *planpb.Expr, row oracleRow, counter *int) tri {
	be := e.GetBinaryExpr()
	if be == nil {
		if ue := e.GetUnaryExpr(); ue != nil && ue.GetOp() == planpb.UnaryExpr_Not {
			return triNot(evalCounting(t, ue.GetChild(), row, counter))
		}
		return evalOracle(t, e, row, counter)
	}
	var operands, nested []*planpb.Expr
	var collect func(*planpb.Expr)
	collect = func(x *planpb.Expr) {
		if xb := x.GetBinaryExpr(); xb != nil && xb.GetOp() == be.GetOp() {
			collect(xb.GetLeft())
			collect(xb.GetRight())
			return
		}
		if x.GetBinaryExpr() != nil || x.GetUnaryExpr() != nil {
			nested = append(nested, x)
		} else {
			operands = append(operands, x)
		}
	}
	collect(e)
	isAnd := be.GetOp() == planpb.BinaryExpr_LogicalAnd
	acc := triTrue
	if !isAnd {
		acc = triFalse
	}
	for _, x := range append(operands, nested...) {
		v := evalCounting(t, x, row, counter)
		if isAnd {
			acc = triAnd(acc, v)
			if acc == triFalse {
				return acc
			}
		} else {
			acc = triOr(acc, v)
			if acc == triTrue {
				return acc
			}
		}
	}
	return acc
}

// enumerateRows returns the cartesian product of the per-field domains.
func enumerateRows(fields []int64, domains map[int64][]any) []oracleRow {
	rows := []oracleRow{{}}
	for _, f := range fields {
		next := make([]oracleRow, 0, len(rows)*len(domains[f]))
		for _, r := range rows {
			for _, v := range domains[f] {
				nr := make(oracleRow, len(r)+1)
				for k, x := range r {
					nr[k] = x
				}
				nr[f] = v
				next = append(next, nr)
			}
		}
		rows = next
	}
	return rows
}

// requireSameThreeValued checks original and rewritten agree on every row,
// and that NOT of each agrees as well.
func requireSameThreeValued(t testing.TB, original, rewritten *planpb.Expr, rows []oracleRow, msg string) {
	for _, row := range rows {
		want := evalOracle(t, original, row, nil)
		got := evalOracle(t, rewritten, row, nil)
		if want != got {
			t.Fatalf("%s: row %v: original=%v rewritten=%v", msg, row, want, got)
		}
		if reordered := evalCounting(t, rewritten, row, nil); reordered != got {
			t.Fatalf("%s: row %v: reordered evaluation %v differs from %v", msg, row, reordered, got)
		}
	}
}

func countNodes(e *planpb.Expr) int {
	if e == nil {
		return 0
	}
	n := 1
	switch real := e.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		n += countNodes(real.BinaryExpr.GetLeft()) + countNodes(real.BinaryExpr.GetRight())
	case *planpb.Expr_UnaryExpr:
		n += countNodes(real.UnaryExpr.GetChild())
	}
	return n
}

// countEqual counts subtrees of e that are proto-equal to target.
func countEqual(e, target *planpb.Expr) int {
	if e == nil {
		return 0
	}
	n := 0
	if proto.Equal(e, target) {
		n++
	}
	switch real := e.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		n += countEqual(real.BinaryExpr.GetLeft(), target) + countEqual(real.BinaryExpr.GetRight(), target)
	case *planpb.Expr_UnaryExpr:
		n += countEqual(real.UnaryExpr.GetChild(), target)
	}
	return n
}

func rewriteClone(e *planpb.Expr, optimize bool) *planpb.Expr {
	return rewriter.RewriteExprWithConfig(proto.Clone(e).(*planpb.Expr), optimize)
}

// ---------------------------------------------------------------------------
// Schema and parser helpers
// ---------------------------------------------------------------------------

const (
	cfTenant int64 = 101
	cfPrice  int64 = 102
	cfRating int64 = 103
	cfName   int64 = 104
	cfFlag   int64 = 105
	cfPlain  int64 = 109
)

func buildSchemaHelperForCommonFactor(t testing.TB) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: cfTenant, Name: "tenant", DataType: schemapb.DataType_Int64, Nullable: true},
		{FieldID: cfPrice, Name: "price", DataType: schemapb.DataType_Double, Nullable: true},
		{FieldID: cfRating, Name: "rating", DataType: schemapb.DataType_Int64, Nullable: true},
		{FieldID: cfName, Name: "name", DataType: schemapb.DataType_VarChar, Nullable: true, TypeParams: []*commonpb.KeyValuePair{
			{Key: "max_length", Value: "64"},
			{Key: "enable_match", Value: "True"},
			{Key: "enable_analyzer", Value: "True"},
		}},
		{FieldID: cfFlag, Name: "flag", DataType: schemapb.DataType_Bool, Nullable: true},
		{FieldID: 106, Name: "JSONField", DataType: schemapb.DataType_JSON},
		{FieldID: 107, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true},
		{FieldID: 108, Name: "ArrayInt", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64, TypeParams: []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}}},
		{FieldID: cfPlain, Name: "plain", DataType: schemapb.DataType_Int64},
	}
	schema := &schemapb.CollectionSchema{
		Name:               "common_factor_test",
		Fields:             fields,
		EnableDynamicField: true,
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	return helper
}

// parseRaw parses an expression without running the rewriter, so the result
// is the unmodified tree used as the differential baseline.
func parseRaw(t testing.TB, helper *typeutil.SchemaHelper, expr string) *planpb.Expr {
	e, err := parser.ParseExprTemplate(helper, expr, nil)
	require.NoError(t, err, expr)
	return e
}

func commonFactorRows() []oracleRow {
	fields := []int64{cfTenant, cfPrice, cfRating, cfName, cfFlag, cfPlain}
	domains := map[int64][]any{
		cfTenant: {nil, int64(7), int64(8)},
		cfPrice:  {nil, 50.0, 150.0},
		cfRating: {nil, int64(3), int64(5)},
		cfName:   {nil, "a", "b"},
		cfFlag:   {nil, true, false},
		cfPlain:  {int64(1), int64(2)},
	}
	return enumerateRows(fields, domains)
}

// ---------------------------------------------------------------------------
// Parser-based cases
// ---------------------------------------------------------------------------

func TestRewrite_CommonFactor_Cases(t *testing.T) {
	helper := buildSchemaHelperForCommonFactor(t)
	rows := commonFactorRows()

	cases := []struct {
		name string
		expr string
		// want, when set, is an equivalent hand-factored expression; the
		// rewritten input must be proto-equal to the rewritten want.
		want string
		// atom/atomCount, when set, is the number of times the rewritten atom
		// must appear in the rewritten input.
		atom      string
		atomCount int
	}{
		{
			name: "issue example",
			expr: `(tenant == 7 and price > 100) or (tenant == 7 and rating > 4)`,
			want: `tenant == 7 and (price > 100 or rating > 4)`,
		},
		{
			name: "absorption single factor",
			expr: `tenant == 7 or (tenant == 7 and price > 100)`,
			want: `tenant == 7`,
		},
		{
			name: "absorption factor branch last",
			expr: `(tenant == 7 and price > 100) or (rating > 4 and tenant == 7) or tenant == 7`,
			want: `tenant == 7`,
		},
		{
			name: "identical branches",
			expr: `(tenant == 7 and price > 100) or (tenant == 7 and price > 100)`,
			want: `tenant == 7 and price > 100`,
		},
		{
			name:      "nonmatching branches",
			expr:      `(tenant == 7 and price > 100) or (tenant == 8 and rating > 4)`,
			atom:      `price > 100`,
			atomCount: 1,
			want:      `(tenant == 7 and price > 100) or (tenant == 8 and rating > 4)`,
		},
		{
			name:      "factor missing from one branch",
			expr:      `(tenant == 7 and price > 100) or (tenant == 7 and rating > 4) or (price > 100 and rating > 4)`,
			atom:      `tenant == 7`,
			atomCount: 2,
		},
		{
			name: "multiple factors in different order with absorption",
			expr: `(tenant == 7 and name == "a" and price > 100) or (name == "a" and tenant == 7 and rating > 4) or (tenant == 7 and name == "a")`,
			want: `tenant == 7 and name == "a"`,
		},
		{
			name: "multiple factors residual or",
			expr: `(tenant == 7 and name == "a" and price > 100) or (name == "a" and tenant == 7 and rating > 4)`,
			want: `tenant == 7 and name == "a" and (price > 100 or rating > 4)`,
		},
		{
			name: "repeated factor within a branch",
			expr: `(tenant == 7 and price > 100 and tenant == 7) or (tenant == 7 and rating > 4)`,
			want: `tenant == 7 and (price > 100 or rating > 4)`,
		},
		{
			name: "in factor with differently ordered values",
			expr: `(tenant in [8, 7] and price > 100) or (tenant in [7, 8, 7] and rating > 4)`,
			want: `tenant in [7, 8] and (price > 100 or rating > 4)`,
		},
		{
			name: "binary range factor",
			expr: `(50 < price < 200 and rating > 4) or (50 < price < 200 and tenant == 8)`,
			want: `50 < price < 200 and (rating > 4 or tenant == 8)`,
		},
		{
			name:      "binary range inclusivity differs",
			expr:      `(50 < price < 200 and rating > 4) or (50 <= price < 200 and tenant == 8)`,
			atom:      `50 < price < 200`,
			atomCount: 1,
		},
		{
			name:      "operator differs",
			expr:      `(rating > 4 and price > 100) or (rating >= 4 and tenant == 8)`,
			atom:      `rating > 4`,
			atomCount: 1,
		},
		{
			name:      "different columns same literal",
			expr:      `(tenant == 7 and price > 100) or (rating == 7 and price > 200)`,
			atom:      `tenant == 7`,
			atomCount: 1,
		},
		{
			name: "not equal factor on nullable field",
			expr: `(tenant != 7 and price > 100) or (tenant != 7 and rating > 4)`,
			want: `tenant != 7 and (price > 100 or rating > 4)`,
		},
		{
			name: "bool factor",
			expr: `(flag == true and tenant == 7) or (flag == true and price > 100)`,
			want: `flag == true and (tenant == 7 or price > 100)`,
		},
		{
			name: "string factor",
			expr: `(name == "a" and tenant == 7) or (name == "a" and price > 100)`,
			want: `name == "a" and (tenant == 7 or price > 100)`,
		},
		{
			name: "outer not",
			expr: `not ((tenant == 7 and price > 100) or (tenant == 7 and rating > 4))`,
			want: `not (tenant == 7 and (price > 100 or rating > 4))`,
		},
		{
			name: "nested inner factoring then outer factoring",
			expr: `(plain == 1 and ((tenant == 7 and price > 100) or (tenant == 7 and rating > 4))) or (plain == 1 and flag == true)`,
			want: `plain == 1 and ((tenant == 7 and (price > 100 or rating > 4)) or flag == true)`,
		},
		{
			name: "factor inside a nested residual",
			expr: `(tenant == 7 and (price > 100 or rating > 4)) or (tenant == 7 and name == "a")`,
			want: `tenant == 7 and (price > 100 or rating > 4 or name == "a")`,
		},
		{
			name: "residual atoms merge after factoring",
			expr: `(tenant == 7 and rating > 4) or (tenant == 7 and rating > 3)`,
			want: `tenant == 7 and rating > 3`,
		},
		{
			name: "factored result merges with parent and",
			expr: `rating > 1 and ((rating > 4 and tenant == 7) or (rating > 4 and price > 100))`,
			want: `rating > 4 and (tenant == 7 or price > 100)`,
		},
		{
			name:      "single term branch without common factor",
			expr:      `(tenant == 7 and price > 100) or rating > 4`,
			atom:      `tenant == 7`,
			atomCount: 1,
			want:      `(tenant == 7 and price > 100) or rating > 4`,
		},
		{
			name:      "null check is not a factor",
			expr:      `(tenant is null and price > 100) or (tenant is null and rating > 4)`,
			atom:      `tenant is null`,
			atomCount: 2,
		},
		{
			name:      "not is not a factor",
			expr:      `(not (price > 100 or rating > 4) and tenant == 7) or (not (price > 100 or rating > 4) and tenant == 8)`,
			atom:      `not (price > 100 or rating > 4)`,
			atomCount: 2,
		},
		{
			name:      "like is not a factor",
			expr:      `(name like "a%" and tenant == 7) or (name like "a%" and rating > 4)`,
			atom:      `name like "a%"`,
			atomCount: 2,
		},
		{
			name:      "non nullable column factor",
			expr:      `(plain == 1 and price > 100) or (plain == 1 and rating > 4)`,
			want:      `plain == 1 and (price > 100 or rating > 4)`,
			atom:      `plain == 1`,
			atomCount: 1,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			original := parseRaw(t, helper, c.expr)
			got := rewriteClone(original, true)
			requireSameThreeValued(t, original, got, rows, c.expr)

			again := rewriteClone(got, true)
			require.True(t, proto.Equal(got, again), "rewrite must be idempotent:\nfirst:  %v\nsecond: %v", got, again)

			if c.want != "" {
				want := rewriteClone(parseRaw(t, helper, c.want), true)
				require.True(t, proto.Equal(want, got), "want:\n%v\ngot:\n%v", want, got)
			}
			if c.atom != "" {
				atom := rewriteClone(parseRaw(t, helper, c.atom), true)
				require.Equal(t, c.atomCount, countEqual(got, atom), "atom %q in %v", c.atom, got)
			}
		})
	}
}

func TestRewrite_CommonFactor_UnsupportedColumnsAndExpressions(t *testing.T) {
	helper := buildSchemaHelperForCommonFactor(t)
	cases := []struct {
		expr string
		atom string
	}{
		{`(JSONField["a"] == 7 and tenant == 7) or (JSONField["a"] == 7 and rating > 4)`, `JSONField["a"] == 7`},
		{`(dyn == 7 and tenant == 7) or (dyn == 7 and rating > 4)`, `dyn == 7`},
		{`(ArrayInt[0] == 7 and tenant == 7) or (ArrayInt[0] == 7 and rating > 4)`, `ArrayInt[0] == 7`},
		{`(array_contains(ArrayInt, 7) and tenant == 7) or (array_contains(ArrayInt, 7) and rating > 4)`, `array_contains(ArrayInt, 7)`},
		{`(text_match(name, "a") and tenant == 7) or (text_match(name, "a") and rating > 4)`, `text_match(name, "a")`},
		{`(tenant + 1 == 8 and price > 100) or (tenant + 1 == 8 and rating > 4)`, `tenant + 1 == 8`},
		{`(tenant == rating and price > 100) or (tenant == rating and name == "a")`, `tenant == rating`},
	}
	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			original := parseRaw(t, helper, c.expr)
			got := rewriteClone(original, true)
			atom := rewriteClone(parseRaw(t, helper, c.atom), true)
			require.Equal(t, 2, countEqual(got, atom), "unsupported predicate must stay in every branch: %v", got)
			require.Equal(t, planpb.BinaryExpr_LogicalOr, got.GetBinaryExpr().GetOp())
		})
	}
}

func TestRewrite_CommonFactor_MatchExprBoundary(t *testing.T) {
	// Two identical OR-of-AND trees: one at the top level, one inside a
	// MatchExpr predicate. Optional rewrites do not descend into MatchExpr, so
	// only the top-level copy may be factored.
	col := func(id int64) *planpb.ColumnInfo {
		return &planpb.ColumnInfo{FieldId: id, DataType: schemapb.DataType_Int64, Nullable: true}
	}
	build := func() *planpb.Expr {
		return orExpr(
			andExpr(eqI64(col(1), 7), gtI64(col(2), 1)),
			andExpr(eqI64(col(1), 7), gtI64(col(3), 1)),
		)
	}
	inner := build()
	match := &planpb.Expr{Expr: &planpb.Expr_MatchExpr{MatchExpr: &planpb.MatchExpr{
		StructName: "s",
		Predicate:  inner,
		MatchType:  planpb.MatchType_MatchAll,
	}}}
	input := andExpr(match, build())
	got := rewriteClone(input, true)

	var gotMatch *planpb.MatchExpr
	for _, part := range []*planpb.Expr{got.GetBinaryExpr().GetLeft(), got.GetBinaryExpr().GetLeft().GetBinaryExpr().GetLeft()} {
		if m := part.GetMatchExpr(); m != nil {
			gotMatch = m
		}
	}
	require.NotNil(t, gotMatch, "%v", got)
	require.True(t, proto.Equal(inner, gotMatch.GetPredicate()), "MatchExpr predicate must be untouched")
	// countEqual does not descend into MatchExpr, so this counts the
	// top-level copies only: the two branches share one factor.
	require.Equal(t, 1, countEqual(got, eqI64(col(1), 7)), "%v", got)
	require.Equal(t, 2, countEqual(gotMatch.GetPredicate(), eqI64(col(1), 7)))
}

func TestRewrite_CommonFactor_TemplateValuesAfterBinding(t *testing.T) {
	helper := buildSchemaHelperForCommonFactor(t)
	tmpl := `(tenant == {a} and price > {p}) or (tenant == {b} and rating > 4)`
	i64 := func(v int64) *schemapb.TemplateValue {
		return &schemapb.TemplateValue{Val: &schemapb.TemplateValue_Int64Val{Int64Val: v}}
	}

	same, err := parser.ParseExpr(helper, tmpl, map[string]*schemapb.TemplateValue{"a": i64(7), "b": i64(7), "p": i64(100)})
	require.NoError(t, err)
	want, err := parser.ParseExpr(helper, `tenant == 7 and (price > 100 or rating > 4)`, nil)
	require.NoError(t, err)
	// Template variable names are bookkeeping kept on the bound nodes; the
	// factor is identified by its bound value.
	require.Equal(t, 1, countEqualIgnoringTemplates(same, want.GetBinaryExpr().GetLeft()), "%v", same)
	require.NotNil(t, same.GetBinaryExpr())
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, same.GetBinaryExpr().GetOp())

	diff, err := parser.ParseExpr(helper, tmpl, map[string]*schemapb.TemplateValue{"a": i64(7), "b": i64(8), "p": i64(100)})
	require.NoError(t, err)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, diff.GetBinaryExpr().GetOp(), "%v", diff)

	rows := commonFactorRows()
	for _, row := range rows {
		require.Equal(t, evalOracle(t, want, row, nil), evalOracle(t, same, row, nil))
	}
}

// countEqualIgnoringTemplates is countEqual with template variable names and
// flags cleared on both sides.
func countEqualIgnoringTemplates(e, target *planpb.Expr) int {
	strip := func(x *planpb.Expr) *planpb.Expr {
		x = proto.Clone(x).(*planpb.Expr)
		var walk func(*planpb.Expr)
		walk = func(n *planpb.Expr) {
			if n == nil {
				return
			}
			n.IsTemplate = false
			switch real := n.GetExpr().(type) {
			case *planpb.Expr_UnaryRangeExpr:
				real.UnaryRangeExpr.TemplateVariableName = ""
			case *planpb.Expr_BinaryRangeExpr:
				real.BinaryRangeExpr.LowerTemplateVariableName = ""
				real.BinaryRangeExpr.UpperTemplateVariableName = ""
			case *planpb.Expr_TermExpr:
				real.TermExpr.TemplateVariableName = ""
			case *planpb.Expr_BinaryExpr:
				walk(real.BinaryExpr.GetLeft())
				walk(real.BinaryExpr.GetRight())
			case *planpb.Expr_UnaryExpr:
				walk(real.UnaryExpr.GetChild())
			}
		}
		walk(x)
		return x
	}
	return countEqual(strip(e), strip(target))
}

func TestRewrite_CommonFactor_OptimizerDisabled(t *testing.T) {
	helper := buildSchemaHelperForCommonFactor(t)
	original := parseRaw(t, helper, `(tenant == 7 and price > 100) or (tenant == 7 and rating > 4)`)
	got := rewriteClone(original, false)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, got.GetBinaryExpr().GetOp())
	atom := rewriteClone(parseRaw(t, helper, `tenant == 7`), false)
	require.Equal(t, 2, countEqual(got, atom))
}

// ---------------------------------------------------------------------------
// Directly constructed trees
// ---------------------------------------------------------------------------

func i64Val(v int64) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: v}}
}

func f64Val(v float64) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: v}}
}

func strVal(v string) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: v}}
}

func unaryRange(col *planpb.ColumnInfo, op planpb.OpType, v *planpb.GenericValue) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
		ColumnInfo: col, Op: op, Value: v,
	}}}
}

func eqI64(col *planpb.ColumnInfo, v int64) *planpb.Expr {
	return unaryRange(col, planpb.OpType_Equal, i64Val(v))
}

func gtI64(col *planpb.ColumnInfo, v int64) *planpb.Expr {
	return unaryRange(col, planpb.OpType_GreaterThan, i64Val(v))
}

func termI64(col *planpb.ColumnInfo, vs ...int64) *planpb.Expr {
	values := make([]*planpb.GenericValue, 0, len(vs))
	for _, v := range vs {
		values = append(values, i64Val(v))
	}
	return &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{ColumnInfo: col, Values: values}}}
}

func binaryRangeI64(col *planpb.ColumnInfo, lo, hi int64, loInc, hiInc bool) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_BinaryRangeExpr{BinaryRangeExpr: &planpb.BinaryRangeExpr{
		ColumnInfo: col, LowerValue: i64Val(lo), UpperValue: i64Val(hi), LowerInclusive: loInc, UpperInclusive: hiInc,
	}}}
}

func logical(op planpb.BinaryExpr_BinaryOp, parts ...*planpb.Expr) *planpb.Expr {
	out := parts[0]
	for _, p := range parts[1:] {
		out = &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{Left: out, Right: p, Op: op}}}
	}
	return out
}

func andExpr(parts ...*planpb.Expr) *planpb.Expr {
	return logical(planpb.BinaryExpr_LogicalAnd, parts...)
}

func orExpr(parts ...*planpb.Expr) *planpb.Expr {
	return logical(planpb.BinaryExpr_LogicalOr, parts...)
}

func notExpr(e *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{Op: planpb.UnaryExpr_Not, Child: e}}}
}

func nullableInt64Col(id int64) *planpb.ColumnInfo {
	return &planpb.ColumnInfo{FieldId: id, DataType: schemapb.DataType_Int64, Nullable: true}
}

func TestRewrite_CommonFactor_TypedLiteralDistinctions(t *testing.T) {
	doubleCol := &planpb.ColumnInfo{FieldId: 1, DataType: schemapb.DataType_Double, Nullable: true}
	strCol := &planpb.ColumnInfo{FieldId: 4, DataType: schemapb.DataType_VarChar, Nullable: true}
	other := nullableInt64Col(2)
	third := nullableInt64Col(3)

	cases := []struct {
		name  string
		left  *planpb.Expr
		right *planpb.Expr
	}{
		{
			name:  "int64 literal vs float literal",
			left:  unaryRange(doubleCol, planpb.OpType_Equal, i64Val(7)),
			right: unaryRange(doubleCol, planpb.OpType_Equal, f64Val(7)),
		},
		{
			name:  "string literal vs int literal",
			left:  unaryRange(strCol, planpb.OpType_Equal, strVal("7")),
			right: unaryRange(strCol, planpb.OpType_Equal, i64Val(7)),
		},
		{
			name:  "same field id different data type",
			left:  unaryRange(&planpb.ColumnInfo{FieldId: 5, DataType: schemapb.DataType_Int64}, planpb.OpType_Equal, i64Val(7)),
			right: unaryRange(&planpb.ColumnInfo{FieldId: 5, DataType: schemapb.DataType_Int32}, planpb.OpType_Equal, i64Val(7)),
		},
		{
			name:  "same field id different nullability",
			left:  unaryRange(&planpb.ColumnInfo{FieldId: 5, DataType: schemapb.DataType_Int64}, planpb.OpType_Equal, i64Val(7)),
			right: unaryRange(&planpb.ColumnInfo{FieldId: 5, DataType: schemapb.DataType_Int64, Nullable: true}, planpb.OpType_Equal, i64Val(7)),
		},
		{
			name:  "nested path",
			left:  unaryRange(&planpb.ColumnInfo{FieldId: 6, DataType: schemapb.DataType_JSON, NestedPath: []string{"a"}}, planpb.OpType_Equal, i64Val(7)),
			right: unaryRange(&planpb.ColumnInfo{FieldId: 6, DataType: schemapb.DataType_JSON, NestedPath: []string{"a"}}, planpb.OpType_Equal, i64Val(7)),
		},
		{
			name:  "element level",
			left:  unaryRange(&planpb.ColumnInfo{FieldId: 7, DataType: schemapb.DataType_Int64, IsElementLevel: true}, planpb.OpType_Equal, i64Val(7)),
			right: unaryRange(&planpb.ColumnInfo{FieldId: 7, DataType: schemapb.DataType_Int64, IsElementLevel: true}, planpb.OpType_Equal, i64Val(7)),
		},
		{
			name:  "binary range bound type",
			left:  &planpb.Expr{Expr: &planpb.Expr_BinaryRangeExpr{BinaryRangeExpr: &planpb.BinaryRangeExpr{ColumnInfo: doubleCol, LowerValue: i64Val(1), UpperValue: i64Val(9)}}},
			right: &planpb.Expr{Expr: &planpb.Expr_BinaryRangeExpr{BinaryRangeExpr: &planpb.BinaryRangeExpr{ColumnInfo: doubleCol, LowerValue: f64Val(1), UpperValue: i64Val(9)}}},
		},
		{
			name: "term value type",
			left: termI64(doubleCol, 1, 2),
			right: &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
				ColumnInfo: doubleCol, Values: []*planpb.GenericValue{f64Val(1), f64Val(2)},
			}}},
		},
		{
			name:  "term values differ",
			left:  termI64(nullableInt64Col(8), 1, 2),
			right: termI64(nullableInt64Col(8), 1, 2, 3),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			input := orExpr(andExpr(c.left, gtI64(other, 1)), andExpr(c.right, gtI64(third, 1)))
			got := rewriteClone(input, true)
			require.Equal(t, planpb.BinaryExpr_LogicalOr, got.GetBinaryExpr().GetOp(), "must not factor: %v", got)
		})
	}

	// Control: identical typed atoms are factored.
	input := orExpr(
		andExpr(unaryRange(doubleCol, planpb.OpType_Equal, f64Val(7)), gtI64(other, 1)),
		andExpr(unaryRange(doubleCol, planpb.OpType_Equal, f64Val(7)), gtI64(third, 1)),
	)
	got := rewriteClone(input, true)
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, got.GetBinaryExpr().GetOp(), "%v", got)
	require.Equal(t, 1, countEqual(got, unaryRange(doubleCol, planpb.OpType_Equal, f64Val(7))))
}

func TestRewrite_CommonFactor_DoesNotMutateInput(t *testing.T) {
	a, b, c := nullableInt64Col(1), nullableInt64Col(2), nullableInt64Col(3)
	input := orExpr(
		andExpr(eqI64(a, 7), gtI64(b, 1)),
		andExpr(eqI64(a, 7), termI64(c, 1, 2)),
		andExpr(gtI64(c, 0), eqI64(a, 7)),
	)
	before := proto.Clone(input).(*planpb.Expr)
	got := rewriter.RewriteExprWithConfig(input, true)
	require.True(t, proto.Equal(before, input), "input tree must not be modified")
	require.Equal(t, 1, countEqual(got, eqI64(a, 7)), "%v", got)
}

// ---------------------------------------------------------------------------
// Exhaustive three-valued differential over random trees
// ---------------------------------------------------------------------------

// The atoms are on nullable int64 fields 1..4 with the domain {NULL, 0, 1, 2},
// so every leaf takes each of TRUE/FALSE/UNKNOWN on some row. For the "> 0"
// atoms alone the 4^4 rows cover all 3^4 truth assignments.
var randomFields = []int64{1, 2, 3, 4}

func randomRows() []oracleRow {
	domains := map[int64][]any{}
	for _, f := range randomFields {
		domains[f] = []any{nil, int64(0), int64(1), int64(2)}
	}
	return enumerateRows(randomFields, domains)
}

// treeGen generates random predicates over randomFields from a pool of atom
// shapes.
type treeGen struct {
	r        *rand.Rand
	atoms    []func(col *planpb.ColumnInfo) *planpb.Expr
	allowNot bool
}

var (
	atomGt0   = func(col *planpb.ColumnInfo) *planpb.Expr { return gtI64(col, 0) }
	atomGt1   = func(col *planpb.ColumnInfo) *planpb.Expr { return gtI64(col, 1) }
	atomEq1   = func(col *planpb.ColumnInfo) *planpb.Expr { return eqI64(col, 1) }
	atomNe1   = func(col *planpb.ColumnInfo) *planpb.Expr { return unaryRange(col, planpb.OpType_NotEqual, i64Val(1)) }
	atomIn21  = func(col *planpb.ColumnInfo) *planpb.Expr { return termI64(col, 2, 1) }
	atomRange = func(col *planpb.ColumnInfo) *planpb.Expr { return binaryRangeI64(col, 0, 2, false, true) }
)

func (g *treeGen) atom() *planpb.Expr {
	col := nullableInt64Col(randomFields[g.r.Intn(len(randomFields))])
	return g.atoms[g.r.Intn(len(g.atoms))](col)
}

func (g *treeGen) tree(depth int) *planpb.Expr {
	if depth == 0 || g.r.Intn(4) == 0 {
		return g.atom()
	}
	switch g.r.Intn(5) {
	case 0:
		if g.allowNot {
			return notExpr(g.tree(depth - 1))
		}
		return g.atom()
	case 1, 2:
		parts := make([]*planpb.Expr, 2+g.r.Intn(3))
		for i := range parts {
			parts[i] = g.tree(depth - 1)
		}
		return andExpr(parts...)
	default:
		return g.factorable(depth - 1)
	}
}

// factorable builds an OR of AND branches that share a common set of atoms
// (with optional repeats, absorption branches, nested subtrees and a branch
// that blocks factoring).
func (g *treeGen) factorable(depth int) *planpb.Expr {
	r := g.r
	common := make([]*planpb.Expr, 1+r.Intn(2))
	for i := range common {
		common[i] = g.atom()
	}
	branches := make([]*planpb.Expr, 2+r.Intn(3))
	for i := range branches {
		var conj []*planpb.Expr
		for _, c := range common {
			conj = append(conj, proto.Clone(c).(*planpb.Expr))
			if r.Intn(5) == 0 {
				conj = append(conj, proto.Clone(c).(*planpb.Expr))
			}
		}
		extra := r.Intn(3)
		if r.Intn(6) == 0 {
			extra = 0
		}
		for j := 0; j < extra; j++ {
			if depth > 0 && r.Intn(3) == 0 {
				conj = append(conj, g.tree(depth-1))
			} else {
				conj = append(conj, g.atom())
			}
		}
		r.Shuffle(len(conj), func(a, b int) { conj[a], conj[b] = conj[b], conj[a] })
		branches[i] = andExpr(conj...)
	}
	if r.Intn(5) == 0 {
		branches = append(branches, g.atom())
	}
	return orExpr(branches...)
}

// TestRewrite_CommonFactor_ThreeValuedDifferential compares the rewritten and
// the unmodified tree on every row of the enumerated domain, so each atom is
// exercised as TRUE, FALSE and UNKNOWN in every combination with the others.
func TestRewrite_CommonFactor_ThreeValuedDifferential(t *testing.T) {
	rows := randomRows()
	g := &treeGen{
		r:        rand.New(rand.NewSource(54388)),
		atoms:    []func(*planpb.ColumnInfo) *planpb.Expr{atomGt0, atomGt0, atomEq1, atomNe1, atomIn21, atomRange},
		allowNot: true,
	}
	const trials = 3000
	for i := 0; i < trials; i++ {
		var e *planpb.Expr
		switch i % 3 {
		case 0:
			e = g.factorable(2)
		case 1:
			e = notExpr(g.factorable(2))
		default:
			e = g.tree(4)
		}
		got := rewriteClone(e, true)
		requireSameThreeValued(t, e, got, rows, fmt.Sprintf("trial %d: %v", i, e))
		requireSameThreeValued(t, e, rewriteClone(e, false), rows, fmt.Sprintf("trial %d disabled", i))
	}
}

// TestRewrite_CommonFactor_IdempotentRandom checks rewrite(rewrite(e)) ==
// rewrite(e). The atom pool is limited to shapes for which the rules that
// predate common-factor extraction are already idempotent; mixing ==, != and
// IN on one column can take a second pass for unrelated rules (for example,
// NOT (a IN [1]) becomes a != 1).
func TestRewrite_CommonFactor_IdempotentRandom(t *testing.T) {
	g := &treeGen{
		r:     rand.New(rand.NewSource(20261019)),
		atoms: []func(*planpb.ColumnInfo) *planpb.Expr{atomGt0, atomGt1, atomRange},
	}
	for i := 0; i < 3000; i++ {
		e := g.tree(4)
		if i%2 == 0 {
			e = g.factorable(2)
		}
		got := rewriteClone(e, true)
		again := rewriteClone(got, true)
		require.True(t, proto.Equal(got, again), "trial %d: rewrite must be idempotent:\ninput:  %v\nfirst:  %v\nsecond: %v", i, e, got, again)
	}
}

// TestRewrite_CommonFactor_FactoringHappens checks that the random
// factorable trees actually exercise the rule: the shared atom must be
// represented once in the rewritten tree.
func TestRewrite_CommonFactor_FactoringHappens(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	factored := 0
	const trials = 500
	for i := 0; i < trials; i++ {
		// Distinct fields for the factor and residuals, so no same-column
		// merge can hide the result.
		factor := gtI64(nullableInt64Col(1), 0)
		branches := make([]*planpb.Expr, 2+r.Intn(4))
		for j := range branches {
			residual := gtI64(nullableInt64Col(int64(10+j)), int64(r.Intn(3)))
			if r.Intn(2) == 0 {
				branches[j] = andExpr(proto.Clone(factor).(*planpb.Expr), residual)
			} else {
				branches[j] = andExpr(residual, proto.Clone(factor).(*planpb.Expr))
			}
		}
		got := rewriteClone(orExpr(branches...), true)
		if countEqual(got, factor) == 1 {
			factored++
		}
	}
	require.Equal(t, trials, factored)
}

// ---------------------------------------------------------------------------
// Benchmark
// ---------------------------------------------------------------------------

// commonFactorBenchExpr builds an OR of n branches. Field 1 is the tenant,
// fields 10.. are per-branch residual columns.
//   - common:    (tenant == 7 AND r_i > i) OR ...
//   - nocommon:  (tenant == i AND r_i > i) OR ...
//   - selective: (tenant IN [0..9] AND status == 1 AND r_i == i) OR ...
//   - large:     n branches of 8 conjuncts, 3 of them shared
func commonFactorBenchExpr(kind string, n int) *planpb.Expr {
	tenant := nullableInt64Col(1)
	status := nullableInt64Col(2)
	branches := make([]*planpb.Expr, n)
	for i := 0; i < n; i++ {
		residual := nullableInt64Col(int64(10 + i))
		switch kind {
		case "common":
			branches[i] = andExpr(eqI64(tenant, 7), gtI64(residual, int64(i)))
		case "nocommon":
			branches[i] = andExpr(eqI64(tenant, int64(i)), gtI64(residual, int64(i)))
		case "selective":
			branches[i] = andExpr(termI64(tenant, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), eqI64(status, 1), eqI64(residual, int64(i)))
		case "large":
			conj := []*planpb.Expr{eqI64(tenant, 7), eqI64(status, 1), gtI64(nullableInt64Col(3), 0)}
			for j := 0; j < 5; j++ {
				conj = append(conj, gtI64(nullableInt64Col(int64(100+i*5+j)), int64(j)))
			}
			branches[i] = andExpr(conj...)
		}
	}
	return orExpr(branches...)
}

func commonFactorBenchRows(n int) []oracleRow {
	r := rand.New(rand.NewSource(1))
	rows := make([]oracleRow, n)
	for i := range rows {
		row := oracleRow{}
		for _, f := range []int64{1, 2, 3} {
			if r.Intn(10) != 0 {
				row[f] = int64(r.Intn(10))
			}
		}
		for f := int64(10); f < 500; f++ {
			if r.Intn(10) != 0 {
				row[f] = int64(r.Intn(64))
			}
		}
		rows[i] = row
	}
	return rows
}

// BenchmarkRewriteCommonFactor reports, per shape and branch count, the
// rewrite time (ns/op, allocations), the rewritten plan node count (nodes),
// the leaf predicates evaluated per row (leaf-evals/row) and the filter time
// per row (filter-ns/row) of the rewritten plan over 1024 nullable rows. The
// last two come from evalCounting, a Go model of segcore's ConjunctExpr
// (column predicates first, per-row short circuit), not from segcore itself.
//
//	go test -run '^$' -bench BenchmarkRewriteCommonFactor ./internal/parser/planparserv2/rewriter/
func BenchmarkRewriteCommonFactor(b *testing.B) {
	rows := commonFactorBenchRows(1024)
	for _, kind := range []string{"common", "nocommon", "selective", "large"} {
		sizes := []int{2, 4, 8}
		if kind == "large" {
			sizes = []int{16, 64}
		}
		for _, n := range sizes {
			b.Run(fmt.Sprintf("%s/branches=%d", kind, n), func(b *testing.B) {
				input := commonFactorBenchExpr(kind, n)
				out := rewriteClone(input, true)
				evals := 0
				for _, row := range rows {
					evalCounting(b, out, row, &evals)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					e := proto.Clone(input).(*planpb.Expr)
					b.StartTimer()
					rewriter.RewriteExprWithConfig(e, true)
				}
				b.StopTimer()
				const filterPasses = 20
				start := time.Now()
				for pass := 0; pass < filterPasses; pass++ {
					for _, row := range rows {
						evalCounting(b, out, row, nil)
					}
				}
				b.ReportMetric(float64(countNodes(out)), "nodes")
				b.ReportMetric(float64(evals)/float64(len(rows)), "leaf-evals/row")
				b.ReportMetric(float64(time.Since(start))/float64(filterPasses*len(rows)), "filter-ns/row")
			})
		}
	}
}
