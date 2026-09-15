package planparserv2

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// A folded chain must produce exactly the plan the equivalent single-operation
// expression produces today. That is the whole safety argument for this feature:
// the segcore executor is handed a BinaryArithOpEvalRangeExpr it already knows
// how to run, not a new node shape.
func TestExpr_NestedArith_FoldsToSingleOpPlan(t *testing.T) {
	helper := newTestSchemaHelper(t)

	tests := []struct {
		nested     string
		equivalent string
	}{
		// additive chains
		{`((Int64Field + 2) + 3) == 5`, `Int64Field + 5 == 5`},
		{`((Int64Field + 7) - 3) == 5`, `Int64Field + 4 == 5`},
		{`((Int64Field - 2) + 6) == 5`, `Int64Field + 4 == 5`},
		{`((Int64Field - 2) - 3) == 5`, `Int64Field + -5 == 5`},
		// multiplicative chain
		{`((Int64Field * 2) * 3) == 6`, `Int64Field * 6 == 6`},
		// Bitwise chains. Note the parentheses on the right-hand expressions: in
		// the grammar BitAnd/BitOr/BitXor bind *looser* than ==, so `x & 2 == 2`
		// would parse as `x & (2 == 2)`.
		{`((Int64Field & 6) & 3) == 2`, `(Int64Field & 2) == 2`},
		{`((Int64Field | 4) | 1) == 5`, `(Int64Field | 5) == 5`},
		{`((Int64Field ^ 4) ^ 1) == 5`, `(Int64Field ^ 5) == 5`},
		// depth > 2 folds by recursion
		{`(((Int64Field + 1) + 2) + 3) == 6`, `Int64Field + 6 == 6`},
		{`((((Int64Field & 15) & 7) & 6) & 4) == 4`, `(Int64Field & 4) == 4`},
		// narrower integer columns fold the same way
		{`((Int8Field + 1) + 2) < 5`, `Int8Field + 3 < 5`},
		{`((Int32Field * 2) * 5) > 6`, `Int32Field * 10 > 6`},
		// the comparison operator is carried through untouched
		{`((Int64Field + 2) + 3) != 5`, `Int64Field + 5 != 5`},
		{`((Int64Field + 2) + 3) >= 5`, `Int64Field + 5 >= 5`},
	}

	for _, tt := range tests {
		t.Run(tt.nested, func(t *testing.T) {
			got, err := ParseExpr(helper, tt.nested, nil)
			require.NoError(t, err, tt.nested)
			require.NotNil(t, got.GetBinaryArithOpEvalRangeExpr(), "expected a fused arith-range node")

			want, err := ParseExpr(helper, tt.equivalent, nil)
			require.NoError(t, err, tt.equivalent)

			assert.True(t, proto.Equal(got, want),
				"folded plan differs from the equivalent single-op plan\n nested: %s\n  got: %v\n want: %v",
				tt.nested, got, want)
		})
	}
}

// The arithmetic operators are left-associative, so the way a user actually
// writes a chain -- without redundant parentheses -- produces the same nested
// tree and folds identically.
func TestExpr_NestedArith_UnparenthesizedChain(t *testing.T) {
	helper := newTestSchemaHelper(t)

	tests := []struct {
		expr    string
		arithOp planpb.ArithOpType
		operand int64
	}{
		{`Int64Field + 2 + 3 == 5`, planpb.ArithOpType_Add, 5},
		{`Int64Field - 2 - 3 == 5`, planpb.ArithOpType_Add, -5},
		{`Int64Field * 2 * 3 == 6`, planpb.ArithOpType_Mul, 6},
		{`Int64Field + 1 + 2 + 3 == 6`, planpb.ArithOpType_Add, 6},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			expr, err := ParseExpr(helper, tt.expr, nil)
			require.NoError(t, err, tt.expr)
			arith := expr.GetBinaryArithOpEvalRangeExpr()
			require.NotNil(t, arith, tt.expr)
			assert.Equal(t, tt.arithOp, arith.GetArithOp())
			assert.EqualValues(t, tt.operand, arith.GetRightOperand().GetInt64Val())
		})
	}
}

// Everything the fold cannot prove equivalent must keep returning the
// pre-existing error. A silent wrong answer here would be far worse than the
// current "not supported".
func TestExpr_NestedArith_UnsupportedChainsStillRejected(t *testing.T) {
	helper := newTestSchemaHelper(t)

	exprs := []string{
		// Float arithmetic is not associative, so these must not fold.
		`((FloatField + 0.1) + 0.2) == 0.3`,
		`((DoubleField * 2.0) * 3.0) == 6.0`,
		`((FloatField + 1) + 2) == 3`,
		// JSON is dynamically typed: the runtime value may be a float.
		`((JSONField["a"] + 1) + 2) == 5`,
		`((A + 1) + 2) == 5`,
		// Integer division truncates and Mod has no fold identity.
		`((Int64Field / 2) / 3) == 1`,
		`((Int64Field % 10) % 3) == 1`,
		`((Int64Field + 2) / 3) == 1`,
		`((Int64Field / 2) + 3) == 1`,
		// Mixed operator families have no single-operation equivalent.
		`((Int64Field + 2) * 3) == 9`,
		`((Int64Field * 2) + 3) == 9`,
		`((Int64Field & 6) | 1) == 3`,
		`((Int64Field | 6) & 1) == 0`,
		`((Int64Field ^ 6) & 1) == 0`,
		// Reversed operands (constant on the left) remain unsupported.
		`((2 - Int64Field) - 3) == 5`,
		`(2 + (Int64Field + 1)) == 5`,
		// Two fields is a different unsupported case entirely.
		`((Int64Field + Int32Field) + 2) == 5`,
	}

	for _, expr := range exprs {
		t.Run(expr, func(t *testing.T) {
			plan, err := ParseExpr(helper, expr, nil)
			assert.Error(t, err, expr)
			assert.Nil(t, plan, expr)
		})
	}
}

// A template variable *inside the chain* carries no value at plan time, so the
// chain cannot be folded and stays unsupported. A template variable on the
// compared-against value is untouched by folding and keeps working.
func TestExpr_NestedArith_Templates(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("template inside the chain is not folded", func(t *testing.T) {
		_, err := ParseExpr(helper, `((Int64Field + {v}) + 2) == 5`,
			map[string]*schemapb.TemplateValue{"v": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 3}}})
		assert.Error(t, err)
	})

	t.Run("template as the compared value still folds", func(t *testing.T) {
		expr, err := ParseExpr(helper, `((Int64Field + 2) + 3) == {v}`,
			map[string]*schemapb.TemplateValue{"v": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 5}}})
		require.NoError(t, err)
		arith := expr.GetBinaryArithOpEvalRangeExpr()
		require.NotNil(t, arith)
		assert.Equal(t, planpb.ArithOpType_Add, arith.GetArithOp())
		assert.EqualValues(t, 5, arith.GetRightOperand().GetInt64Val())
	})
}

// An integer array element is an integer column for folding purposes: the
// executor evaluates it through the same int64 arithmetic path.
func TestExpr_NestedArith_IntegerArrayElement(t *testing.T) {
	helper := newTestSchemaHelper(t)

	expr, err := ParseExpr(helper, `((ArrayField[0] + 2) + 3) == 5`, nil)
	require.NoError(t, err)
	arith := expr.GetBinaryArithOpEvalRangeExpr()
	require.NotNil(t, arith)
	assert.Equal(t, planpb.ArithOpType_Add, arith.GetArithOp())
	assert.EqualValues(t, 5, arith.GetRightOperand().GetInt64Val())
}

// composeArithOps is the algebraic core; check the identities directly,
// including the two's-complement boundaries where a naive implementation breaks.
func TestComposeArithOps(t *testing.T) {
	tests := []struct {
		name    string
		op1     planpb.ArithOpType
		a       int64
		op2     planpb.ArithOpType
		b       int64
		wantOp  planpb.ArithOpType
		wantVal int64
		wantOK  bool
	}{
		{"add-add", planpb.ArithOpType_Add, 2, planpb.ArithOpType_Add, 3, planpb.ArithOpType_Add, 5, true},
		{"add-sub", planpb.ArithOpType_Add, 7, planpb.ArithOpType_Sub, 3, planpb.ArithOpType_Add, 4, true},
		{"sub-add", planpb.ArithOpType_Sub, 2, planpb.ArithOpType_Add, 6, planpb.ArithOpType_Add, 4, true},
		{"sub-sub", planpb.ArithOpType_Sub, 2, planpb.ArithOpType_Sub, 3, planpb.ArithOpType_Add, -5, true},
		{"mul-mul", planpb.ArithOpType_Mul, 2, planpb.ArithOpType_Mul, 3, planpb.ArithOpType_Mul, 6, true},
		{"and-and", planpb.ArithOpType_BitAnd, 6, planpb.ArithOpType_BitAnd, 3, planpb.ArithOpType_BitAnd, 2, true},
		{"or-or", planpb.ArithOpType_BitOr, 4, planpb.ArithOpType_BitOr, 1, planpb.ArithOpType_BitOr, 5, true},
		{"xor-xor", planpb.ArithOpType_BitXor, 4, planpb.ArithOpType_BitXor, 1, planpb.ArithOpType_BitXor, 5, true},

		// Two's-complement boundaries. Reassociation is exact modulo 2^64, so the
		// wrapped constant is the correct one; negating MinInt64 yields itself,
		// which is precisely its additive inverse mod 2^64.
		{"sub-minint64", planpb.ArithOpType_Sub, math.MinInt64, planpb.ArithOpType_Add, 0, planpb.ArithOpType_Add, math.MinInt64, true},
		{"add-overflow-wraps", planpb.ArithOpType_Add, math.MaxInt64, planpb.ArithOpType_Add, 1, planpb.ArithOpType_Add, math.MinInt64, true},

		// No identity exists for these pairs.
		{"add-mul", planpb.ArithOpType_Add, 2, planpb.ArithOpType_Mul, 3, planpb.ArithOpType_Unknown, 0, false},
		{"and-or", planpb.ArithOpType_BitAnd, 6, planpb.ArithOpType_BitOr, 1, planpb.ArithOpType_Unknown, 0, false},
		{"div-div", planpb.ArithOpType_Div, 2, planpb.ArithOpType_Div, 3, planpb.ArithOpType_Unknown, 0, false},
		{"mod-mod", planpb.ArithOpType_Mod, 10, planpb.ArithOpType_Mod, 3, planpb.ArithOpType_Unknown, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op, val, ok := composeArithOps(tt.op1, NewInt(tt.a), tt.op2, NewInt(tt.b))
			assert.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			assert.Equal(t, tt.wantOp, op)
			assert.Equal(t, tt.wantVal, val.GetInt64Val())
		})
	}
}

// A float constant anywhere in the chain must block the fold, even on an integer
// column: the intermediate result is no longer an integer.
func TestComposeArithOps_RejectsNonInteger(t *testing.T) {
	_, _, ok := composeArithOps(planpb.ArithOpType_Add, NewFloat(1.5), planpb.ArithOpType_Add, NewInt(2))
	assert.False(t, ok)

	_, _, ok = composeArithOps(planpb.ArithOpType_Add, NewInt(1), planpb.ArithOpType_Add, NewFloat(2.5))
	assert.False(t, ok)
}
