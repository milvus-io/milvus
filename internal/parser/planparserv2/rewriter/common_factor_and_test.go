package rewriter_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Tests for the AND-of-ORs direction of common-factor extraction:
//
//	(F OR R1) AND (F OR R2) AND ...  →  F OR (R1 AND R2 AND ...)
//	F AND (F OR R)                   →  F
//
// They reuse the three-valued oracle, schema and tree helpers of
// common_factor_test.go. An OR clause in the parser cases holds at most one
// == predicate: combineOrEqualsToIn emits == predicates on different columns
// in map order, which would make the expected forms flaky (unrelated to this
// rule).

func TestRewrite_CommonFactorAnd_Cases(t *testing.T) {
	helper := buildSchemaHelperForCommonFactor(t)
	rows := commonFactorRows()

	cases := []struct {
		name      string
		expr      string
		want      string
		atom      string
		atomCount int
	}{
		{
			name: "two clauses",
			expr: `(tenant == 7 or price > 100) and (tenant == 7 or rating > 4)`,
			want: `tenant == 7 or (price > 100 and rating > 4)`,
		},
		{
			name: "issue shape with residuals on several columns",
			expr: `(tenant in [1, 2, 3] or rating == 1) and (tenant in [1, 2, 3] or price == 2) and (tenant in [1, 2, 3] or name == "a") and (tenant in [1, 2, 3] or flag == true)`,
			want: `tenant in [1, 2, 3] or (rating == 1 and price == 2 and name == "a" and flag == true)`,
		},
		{
			name: "absorption by an atom",
			expr: `tenant == 7 and (tenant == 7 or price > 100)`,
			want: `tenant == 7`,
		},
		{
			name: "absorption atom last",
			expr: `(tenant == 7 or price > 100) and (rating > 4 or tenant == 7) and tenant == 7`,
			want: `tenant == 7`,
		},
		{
			name: "absorption by a smaller clause",
			expr: `(tenant == 7 or rating > 4) and (rating > 4 or price > 100 or tenant == 7)`,
			want: `tenant == 7 or rating > 4`,
		},
		{
			name: "identical clauses",
			expr: `(tenant == 7 or price > 100) and (tenant == 7 or price > 100)`,
			want: `tenant == 7 or price > 100`,
		},
		{
			name:      "nonmatching clauses",
			expr:      `(tenant == 7 or price > 100) and (tenant == 8 or rating > 4)`,
			want:      `(tenant == 7 or price > 100) and (tenant == 8 or rating > 4)`,
			atom:      `price > 100`,
			atomCount: 1,
		},
		{
			name: "partial sharing keeps the other clause",
			expr: `(tenant == 7 or price > 100) and (name == "a" or plain > 1) and (tenant == 7 or rating > 4)`,
			want: `(tenant == 7 or (price > 100 and rating > 4)) and (name == "a" or plain > 1)`,
		},
		{
			name: "partial sharing with a plain conjunct",
			expr: `plain == 1 and (tenant == 7 or price > 100) and (tenant == 7 or rating > 4)`,
			want: `plain == 1 and (tenant == 7 or (price > 100 and rating > 4))`,
		},
		{
			name: "two independent groups",
			expr: `(tenant == 7 or price > 100) and (name == "a" or price > 200) and (tenant == 7 or rating > 4) and (name == "a" or rating > 1)`,
			want: `(tenant == 7 or (price > 100 and rating > 4)) and (name == "a" or (price > 200 and rating > 1))`,
		},
		{
			name:      "overlapping groups take the first factor",
			expr:      `(tenant == 7 or price > 100) and (tenant == 7 or rating > 1) and (rating > 1 or plain > 1)`,
			want:      `(tenant == 7 or (price > 100 and rating > 1)) and (rating > 1 or plain > 1)`,
			atom:      `tenant == 7`,
			atomCount: 1,
		},
		{
			name: "larger group wins",
			expr: `(rating > 1 or price > 100) and (tenant == 7 or rating > 1) and (tenant == 7 or name == "a") and (tenant == 7 or flag == true)`,
			want: `(rating > 1 or price > 100) and (tenant == 7 or (rating > 1 and name == "a" and flag == true))`,
		},
		{
			name: "multiple factors in different order",
			expr: `(tenant == 7 or rating > 1 or price > 100) and (rating > 1 or tenant == 7 or plain > 1)`,
			want: `tenant == 7 or rating > 1 or (price > 100 and plain > 1)`,
		},
		{
			name: "repeated factor within a clause",
			expr: `(tenant > 7 or price > 100 or tenant > 7) and (tenant > 7 or rating > 4)`,
			want: `tenant > 7 or (price > 100 and rating > 4)`,
		},
		{
			name: "in factor with differently ordered values",
			expr: `(tenant in [8, 7] or price > 100) and (tenant in [7, 8, 7] or rating > 4)`,
			want: `tenant in [7, 8] or (price > 100 and rating > 4)`,
		},
		{
			name: "binary range factor",
			expr: `(50 < price < 200 or rating > 4) and (50 < price < 200 or tenant == 8)`,
			want: `50 < price < 200 or (rating > 4 and tenant == 8)`,
		},
		{
			name:      "operator differs",
			expr:      `(rating > 4 or price > 100) and (rating >= 4 or tenant == 8)`,
			atom:      `rating > 4`,
			atomCount: 1,
		},
		{
			name: "not equal factor on nullable field",
			expr: `(tenant != 7 or price > 100) and (tenant != 7 or rating > 4)`,
			want: `tenant != 7 or (price > 100 and rating > 4)`,
		},
		{
			name: "bool factor",
			expr: `(flag == true or tenant > 7) and (flag == true or price > 100)`,
			want: `flag == true or (tenant > 7 and price > 100)`,
		},
		{
			name: "string factor",
			expr: `(name == "a" or tenant > 7) and (name == "a" or price > 100)`,
			want: `name == "a" or (tenant > 7 and price > 100)`,
		},
		{
			name: "outer not",
			expr: `not ((tenant == 7 or price > 100) and (tenant == 7 or rating > 4))`,
			want: `not (tenant == 7 or (price > 100 and rating > 4))`,
		},
		{
			name: "nested inner factoring then outer factoring",
			expr: `(plain > 1 or ((tenant == 7 or price > 100) and (tenant == 7 or rating > 4))) and (plain > 1 or flag == true)`,
			want: `plain > 1 or ((tenant == 7 or (price > 100 and rating > 4)) and flag == true)`,
		},
		{
			name: "factor inside a nested residual",
			expr: `(tenant == 7 or (price > 100 and rating > 4)) and (tenant == 7 or plain > 1)`,
			want: `tenant == 7 or (price > 100 and rating > 4 and plain > 1)`,
		},
		{
			name: "residual atoms merge after factoring",
			expr: `(tenant == 7 or rating > 4) and (tenant == 7 or rating > 3)`,
			want: `tenant == 7 or rating > 4`,
		},
		{
			name: "factored result merges with parent or",
			expr: `rating > 5 or ((rating > 4 or tenant == 7) and (rating > 4 or price > 100))`,
			want: `rating > 4 or (tenant == 7 and price > 100)`,
		},
		{
			name: "dnf and cnf directions meet",
			expr: `((tenant == 7 and price > 100) or (tenant == 7 and rating > 4)) and ((name == "a" or rating > 1) and (name == "a" or plain > 1))`,
			want: `tenant == 7 and (price > 100 or rating > 4) and (name == "a" or (rating > 1 and plain > 1))`,
		},
		{
			name:      "single clause without common factor",
			expr:      `(tenant == 7 or price > 100) and rating > 4`,
			want:      `(tenant == 7 or price > 100) and rating > 4`,
			atom:      `tenant == 7`,
			atomCount: 1,
		},
		{
			name:      "null check is not a factor",
			expr:      `(tenant is null or price > 100) and (tenant is null or rating > 4)`,
			atom:      `tenant is null`,
			atomCount: 2,
		},
		{
			name:      "not is not a factor",
			expr:      `(not (price > 100 and rating > 4) or tenant == 7) and (not (price > 100 and rating > 4) or tenant == 8)`,
			atom:      `not (price > 100 and rating > 4)`,
			atomCount: 2,
		},
		{
			name:      "like is not a factor",
			expr:      `(name like "a%" or tenant == 7) and (name like "a%" or rating > 4)`,
			atom:      `name like "a%"`,
			atomCount: 2,
		},
		{
			name:      "non nullable column factor",
			expr:      `(plain == 1 or price > 100) and (plain == 1 or rating > 4)`,
			want:      `plain == 1 or (price > 100 and rating > 4)`,
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

// buildSchemaHelperForIssue54692 mirrors the collection of issue #54692.
func buildSchemaHelperForIssue54692(t testing.TB) *typeutil.SchemaHelper {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "tenant", DataType: schemapb.DataType_Int64},
		{FieldID: 102, Name: "level", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "category", DataType: schemapb.DataType_Int64},
		{FieldID: 104, Name: "category5", DataType: schemapb.DataType_Int64},
		{FieldID: 105, Name: "age", DataType: schemapb.DataType_Int64},
	}
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Name: "issue_54692", Fields: fields})
	require.NoError(t, err)
	return helper
}

// TestRewrite_CommonFactorAnd_Issue54692 checks that the CNF filter of issue
// #54692, (C OR A0) AND ... AND (C OR A7), rewrites to the same plan as the
// original C OR (A0 AND ... AND A7).
func TestRewrite_CommonFactorAnd_Issue54692(t *testing.T) {
	helper := buildSchemaHelperForIssue54692(t)
	const c = `category5 in [1, 2, 3]`
	clauses := make([]string, 8)
	atoms := make([]string, 8)
	for i := range clauses {
		atoms[i] = fmt.Sprintf("age == %d", 1+i%1000)
		clauses[i] = fmt.Sprintf("(%s or %s)", c, atoms[i])
	}
	cnf := strings.Join(clauses, " and ")
	original := fmt.Sprintf("%s or (%s)", c, strings.Join(atoms, " and "))

	cnfRaw := parseRaw(t, helper, cnf)
	cnfPlan := rewriteClone(cnfRaw, true)
	originalPlan := rewriteClone(parseRaw(t, helper, original), true)
	t.Logf("cnf filter:      %s", cnf)
	t.Logf("cnf plan:        %v", cnfPlan)
	t.Logf("original filter: %s", original)
	t.Logf("original plan:   %v", originalPlan)
	require.True(t, proto.Equal(originalPlan, cnfPlan), "original:\n%v\ncnf:\n%v", originalPlan, cnfPlan)

	cAtom := rewriteClone(parseRaw(t, helper, c), true)
	require.Equal(t, 1, countEqual(cnfPlan, cAtom), "C must be evaluated once: %v", cnfPlan)

	domains := map[int64][]any{
		104: {int64(1), int64(4)},
		105: {int64(1), int64(2), int64(9)},
	}
	requireSameThreeValued(t, cnfRaw, cnfPlan, enumerateRows([]int64{104, 105}, domains), cnf)
}

func TestRewrite_CommonFactorAnd_UnsupportedColumnsAndExpressions(t *testing.T) {
	helper := buildSchemaHelperForCommonFactor(t)
	cases := []struct {
		expr string
		atom string
	}{
		{`(JSONField["a"] == 7 or tenant == 7) and (JSONField["a"] == 7 or rating > 4)`, `JSONField["a"] == 7`},
		{`(dyn == 7 or tenant == 7) and (dyn == 7 or rating > 4)`, `dyn == 7`},
		{`(ArrayInt[0] == 7 or tenant == 7) and (ArrayInt[0] == 7 or rating > 4)`, `ArrayInt[0] == 7`},
		{`(array_contains(ArrayInt, 7) or tenant == 7) and (array_contains(ArrayInt, 7) or rating > 4)`, `array_contains(ArrayInt, 7)`},
		{`(text_match(name, "a") or tenant == 7) and (text_match(name, "a") or rating > 4)`, `text_match(name, "a")`},
		{`(tenant + 1 == 8 or price > 100) and (tenant + 1 == 8 or rating > 4)`, `tenant + 1 == 8`},
		{`(tenant == rating or price > 100) and (tenant == rating or name == "a")`, `tenant == rating`},
	}
	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			original := parseRaw(t, helper, c.expr)
			got := rewriteClone(original, true)
			atom := rewriteClone(parseRaw(t, helper, c.atom), true)
			require.Equal(t, 2, countEqual(got, atom), "unsupported predicate must stay in every clause: %v", got)
			require.Equal(t, planpb.BinaryExpr_LogicalAnd, got.GetBinaryExpr().GetOp())
		})
	}
}

func TestRewrite_CommonFactorAnd_OptimizerDisabled(t *testing.T) {
	helper := buildSchemaHelperForCommonFactor(t)
	original := parseRaw(t, helper, `(tenant == 7 or price > 100) and (tenant == 7 or rating > 4)`)
	got := rewriteClone(original, false)
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, got.GetBinaryExpr().GetOp())
	atom := rewriteClone(parseRaw(t, helper, `tenant == 7`), false)
	require.Equal(t, 2, countEqual(got, atom))
}

func TestRewrite_CommonFactorAnd_TypedLiteralDistinctions(t *testing.T) {
	doubleCol := &planpb.ColumnInfo{FieldId: 1, DataType: schemapb.DataType_Double, Nullable: true}
	other := nullableInt64Col(2)
	third := nullableInt64Col(3)
	pairs := [][2]*planpb.Expr{
		{unaryRange(doubleCol, planpb.OpType_Equal, i64Val(7)), unaryRange(doubleCol, planpb.OpType_Equal, f64Val(7))},
		{
			unaryRange(&planpb.ColumnInfo{FieldId: 5, DataType: schemapb.DataType_Int64}, planpb.OpType_Equal, i64Val(7)),
			unaryRange(&planpb.ColumnInfo{FieldId: 5, DataType: schemapb.DataType_Int64, Nullable: true}, planpb.OpType_Equal, i64Val(7)),
		},
		{termI64(nullableInt64Col(8), 1, 2), termI64(nullableInt64Col(8), 1, 2, 3)},
	}
	for i, p := range pairs {
		input := andExpr(orExpr(p[0], gtI64(other, 1)), orExpr(p[1], gtI64(third, 1)))
		got := rewriteClone(input, true)
		require.Equal(t, planpb.BinaryExpr_LogicalAnd, got.GetBinaryExpr().GetOp(), "pair %d must not factor: %v", i, got)
	}

	// Control: identical typed atoms are factored.
	f7 := unaryRange(doubleCol, planpb.OpType_Equal, f64Val(7))
	got := rewriteClone(andExpr(orExpr(f7, gtI64(other, 1)), orExpr(proto.Clone(f7).(*planpb.Expr), gtI64(third, 1))), true)
	require.Equal(t, planpb.BinaryExpr_LogicalOr, got.GetBinaryExpr().GetOp(), "%v", got)
	require.Equal(t, 1, countEqual(got, f7))
}

func TestRewrite_CommonFactorAnd_DoesNotMutateInput(t *testing.T) {
	a, b, c := nullableInt64Col(1), nullableInt64Col(2), nullableInt64Col(3)
	input := andExpr(
		orExpr(eqI64(a, 7), gtI64(b, 1)),
		orExpr(eqI64(a, 7), termI64(c, 1, 2)),
		orExpr(gtI64(c, 0), eqI64(a, 7)),
	)
	before := proto.Clone(input).(*planpb.Expr)
	got := rewriter.RewriteExprWithConfig(input, true)
	require.True(t, proto.Equal(before, input), "input tree must not be modified")
	require.Equal(t, 1, countEqual(got, eqI64(a, 7)), "%v", got)
}

// cnfFactorable builds an AND of OR clauses, most of which share a common set
// of atoms (with repeats, absorbing atoms, nested subtrees and conjuncts that
// share nothing).
func (g *treeGen) cnfFactorable(depth int) *planpb.Expr {
	r := g.r
	common := make([]*planpb.Expr, 1+r.Intn(2))
	for i := range common {
		common[i] = g.atom()
	}
	clauses := make([]*planpb.Expr, 2+r.Intn(3))
	for i := range clauses {
		var disj []*planpb.Expr
		for _, c := range common {
			disj = append(disj, proto.Clone(c).(*planpb.Expr))
			if r.Intn(5) == 0 {
				disj = append(disj, proto.Clone(c).(*planpb.Expr))
			}
		}
		extra := r.Intn(3)
		if r.Intn(6) == 0 {
			extra = 0
		}
		for j := 0; j < extra; j++ {
			if depth > 0 && r.Intn(3) == 0 {
				disj = append(disj, g.tree(depth-1))
			} else {
				disj = append(disj, g.atom())
			}
		}
		r.Shuffle(len(disj), func(a, b int) { disj[a], disj[b] = disj[b], disj[a] })
		clauses[i] = orExpr(disj...)
	}
	for r.Intn(3) == 0 {
		// A conjunct that may share nothing with the group.
		clauses = append(clauses, orExpr(g.atom(), g.atom()))
	}
	if r.Intn(5) == 0 {
		clauses = append(clauses, g.atom())
	}
	r.Shuffle(len(clauses), func(a, b int) { clauses[a], clauses[b] = clauses[b], clauses[a] })
	return andExpr(clauses...)
}

func TestRewrite_CommonFactorAnd_ThreeValuedDifferential(t *testing.T) {
	rows := randomRows()
	g := &treeGen{
		r:        rand.New(rand.NewSource(54692)),
		atoms:    []func(*planpb.ColumnInfo) *planpb.Expr{atomGt0, atomGt0, atomEq1, atomNe1, atomIn21, atomRange},
		allowNot: true,
	}
	const trials = 3000
	for i := 0; i < trials; i++ {
		var e *planpb.Expr
		switch i % 4 {
		case 0:
			e = g.cnfFactorable(2)
		case 1:
			e = notExpr(g.cnfFactorable(2))
		case 2:
			e = orExpr(g.cnfFactorable(1), g.factorable(1))
		default:
			e = andExpr(g.factorable(1), g.cnfFactorable(1))
		}
		got := rewriteClone(e, true)
		requireSameThreeValued(t, e, got, rows, fmt.Sprintf("trial %d: %v", i, e))
		requireSameThreeValued(t, e, rewriteClone(e, false), rows, fmt.Sprintf("trial %d disabled", i))
	}
}

func TestRewrite_CommonFactorAnd_IdempotentRandom(t *testing.T) {
	g := &treeGen{
		r:     rand.New(rand.NewSource(20261027)),
		atoms: []func(*planpb.ColumnInfo) *planpb.Expr{atomGt0, atomGt1, atomRange},
	}
	for i := 0; i < 3000; i++ {
		var e *planpb.Expr
		switch i % 3 {
		case 0:
			e = g.cnfFactorable(2)
		case 1:
			e = andExpr(g.cnfFactorable(1), g.factorable(1))
		default:
			e = orExpr(g.factorable(1), g.cnfFactorable(1))
		}
		got := rewriteClone(e, true)
		again := rewriteClone(got, true)
		require.True(t, proto.Equal(got, again), "trial %d: rewrite must be idempotent:\ninput:  %v\nfirst:  %v\nsecond: %v", i, e, got, again)
	}
}

// TestRewrite_CommonFactorAnd_FactoringHappens checks that the shared atom of
// a CNF chain is represented once, also when unrelated conjuncts are mixed in.
func TestRewrite_CommonFactorAnd_FactoringHappens(t *testing.T) {
	r := rand.New(rand.NewSource(11))
	const trials = 500
	for i := 0; i < trials; i++ {
		factor := gtI64(nullableInt64Col(1), 0)
		var clauses []*planpb.Expr
		for j := 0; j < 2+r.Intn(4); j++ {
			residual := gtI64(nullableInt64Col(int64(10+j)), int64(r.Intn(3)))
			if r.Intn(2) == 0 {
				clauses = append(clauses, orExpr(proto.Clone(factor).(*planpb.Expr), residual))
			} else {
				clauses = append(clauses, orExpr(residual, proto.Clone(factor).(*planpb.Expr)))
			}
		}
		if r.Intn(2) == 0 {
			clauses = append(clauses, gtI64(nullableInt64Col(2), 0))
		}
		r.Shuffle(len(clauses), func(a, b int) { clauses[a], clauses[b] = clauses[b], clauses[a] })
		got := rewriteClone(andExpr(clauses...), true)
		require.Equal(t, 1, countEqual(got, factor), "trial %d: %v", i, got)
	}
}
