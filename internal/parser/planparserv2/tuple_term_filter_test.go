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

package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestExpr_TupleTerm(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("valid 2-column, 2-tuple: lowers to OR of ANDs of equalities", func(t *testing.T) {
		expr, err := ParseExpr(helper, `[Int64Field, VarCharField] in [[1, "a"], [2, "b"]]`, nil)
		require.NoError(t, err)

		or := expr.GetBinaryExpr()
		require.NotNil(t, or, "two tuples must lower to a top-level OR, not a TupleTermExpr or anything else")
		assert.Equal(t, planpb.BinaryExpr_LogicalOr, or.GetOp())

		for _, branch := range []*planpb.Expr{or.GetLeft(), or.GetRight()} {
			and := branch.GetBinaryExpr()
			require.NotNil(t, and, "each OR branch must be an AND of the per-column equalities")
			assert.Equal(t, planpb.BinaryExpr_LogicalAnd, and.GetOp())

			left := and.GetLeft().GetUnaryRangeExpr()
			right := and.GetRight().GetUnaryRangeExpr()
			require.NotNil(t, left)
			require.NotNil(t, right)
			assert.Equal(t, planpb.OpType_Equal, left.GetOp())
			assert.Equal(t, planpb.OpType_Equal, right.GetOp())
		}
	})

	t.Run("valid 2-column, 1-tuple: lowers directly to a single AND, no outer OR", func(t *testing.T) {
		expr, err := ParseExpr(helper, `[Int64Field, VarCharField] in [[1, "a"]]`, nil)
		require.NoError(t, err)

		and := expr.GetBinaryExpr()
		require.NotNil(t, and, "a single tuple must not be wrapped in a 1-way OR")
		assert.Equal(t, planpb.BinaryExpr_LogicalAnd, and.GetOp())
	})

	t.Run("valid 2-column, 3-tuple: OR-folds left-to-right, not just pairwise", func(t *testing.T) {
		expr, err := ParseExpr(helper, `[Int64Field, VarCharField] in [[1, "a"], [2, "b"], [3, "c"]]`, nil)
		require.NoError(t, err)

		// foldOrOperands folds left-to-right over N>2 operands: it is NOT a
		// flat N-way OR (this plan format has no such node) -- the tree is
		// (tuple1 OR tuple2) OR tuple3. Only verified up to 2 tuples
		// elsewhere in this file; this confirms the fold keeps working, and
		// keeps the expected shape, past the pairwise case.
		outerOr := expr.GetBinaryExpr()
		require.NotNil(t, outerOr)
		assert.Equal(t, planpb.BinaryExpr_LogicalOr, outerOr.GetOp())

		innerOr := outerOr.GetLeft().GetBinaryExpr()
		require.NotNil(t, innerOr, "left child of the outer OR must itself be an OR of the first two tuples")
		assert.Equal(t, planpb.BinaryExpr_LogicalOr, innerOr.GetOp())

		for _, leaf := range []*planpb.Expr{innerOr.GetLeft(), innerOr.GetRight(), outerOr.GetRight()} {
			and := leaf.GetBinaryExpr()
			require.NotNil(t, and, "every fold leaf must still be an AND of equalities, not something flattened away")
			assert.Equal(t, planpb.BinaryExpr_LogicalAnd, and.GetOp())
		}
	})

	t.Run("valid 3-column tuple in", func(t *testing.T) {
		assertValidExpr(t, helper, `[Int64Field, VarCharField, BoolField] in [[1, "a", true], [2, "b", false]]`)
	})

	t.Run("int literal widens to a float column, same as single-column IN", func(t *testing.T) {
		// Regression check: the superseded TupleTermExpr-based test suite
		// covered int-to-float widening explicitly; make sure the rewrite to
		// lowering didn't silently drop that coverage.
		expr, err := ParseExpr(helper, `[Int64Field, FloatField] in [[1, 2]]`, nil)
		require.NoError(t, err)

		and := expr.GetBinaryExpr()
		require.NotNil(t, and)
		assert.Equal(t, planpb.BinaryExpr_LogicalAnd, and.GetOp())

		floatEq := and.GetRight().GetUnaryRangeExpr()
		require.NotNil(t, floatEq)
		assert.Equal(t, planpb.OpType_Equal, floatEq.GetOp())
		assert.Equal(t, float64(2), floatEq.GetValue().GetFloatVal(),
			"the literal 2 must be cast to a float value (castValue widening), not left as an int64")
	})

	// NOT must be written `[...] not in [...]` (NOT immediately before IN),
	// matching how single-column `field not in [...]` already works -- the
	// prefix form `not [...] in [...]` is a DIFFERENT, invalid parse under
	// this grammar's operator precedence: the generic unary NOT
	// (`op=(ADD|SUB|BNOT|NOT) expr # Unary`) binds to the bracketed array
	// first as its own primary operand (`(not [...]) in [...]`), not to the
	// whole `in` expression -- the exact same precedence single-column IN
	// already has (`not Int64Field in [1,2]` is likewise invalid; only
	// `Int64Field not in [1,2]` works).
	t.Run("not wraps the lowered expression in UnaryExpr, same as single-column Term", func(t *testing.T) {
		expr, err := ParseExpr(helper, `[Int64Field, VarCharField] not in [[1, "a"]]`, nil)
		require.NoError(t, err)
		unary := expr.GetUnaryExpr()
		require.NotNil(t, unary)
		assert.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
		require.NotNil(t, unary.GetChild().GetBinaryExpr(), "child of NOT must be the lowered AND, not a bespoke node")
	})

	t.Run("prefix NOT before the bracket is invalid, same as single-column IN", func(t *testing.T) {
		assertInvalidExpr(t, helper, `not [Int64Field, VarCharField] in [[1, "a"]]`)
		assertInvalidExpr(t, helper, `not Int64Field in [1, 2]`)
	})

	t.Run("parens let NOT apply to the whole tuple-in, unlike the bare prefix form", func(t *testing.T) {
		// The precedence trap above means bare `not [...] in [...]` is
		// invalid. Confirm the natural workaround actually works: wrapping
		// the tuple-in in explicit parens lets NOT bind to the whole thing,
		// rather than assuming it does without checking.
		expr, err := ParseExpr(helper, `not ([Int64Field, VarCharField] in [[1, "a"]])`, nil)
		require.NoError(t, err)
		unary := expr.GetUnaryExpr()
		require.NotNil(t, unary, "parens must let NOT wrap the whole lowered tuple expression")
		assert.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
		assert.NotNil(t, unary.GetChild().GetBinaryExpr(),
			"child of NOT must be the lowered AND, proving parens fixed the precedence trap")
	})

	t.Run("single-column in is unaffected", func(t *testing.T) {
		expr, err := ParseExpr(helper, `Int64Field in [1, 2, 3]`, nil)
		require.NoError(t, err)
		require.NotNil(t, expr.GetTermExpr(), "single-column IN must keep taking the existing TermExpr path")
	})

	t.Run("a single bracketed column is not a tuple, existing error unchanged", func(t *testing.T) {
		assertInvalidExpr(t, helper, `[Int64Field] in [[1]]`)
	})

	t.Run("arity mismatch is rejected", func(t *testing.T) {
		assertInvalidExpr(t, helper, `[Int64Field, VarCharField] in [[1, "a"], [2]]`)
	})

	t.Run("duplicate left-hand column is rejected", func(t *testing.T) {
		assertInvalidExpr(t, helper, `[Int64Field, Int64Field] in [[1, 2]]`)
	})

	t.Run("value that cannot cast to its column's type is rejected", func(t *testing.T) {
		assertInvalidExpr(t, helper, `[Int64Field, VarCharField] in [["not-an-int", "a"]]`)
	})

	t.Run("non-primitive column (whole array field) is rejected", func(t *testing.T) {
		assertInvalidExpr(t, helper, `[ArrayField, Int64Field] in [[[1], 2]]`)
	})

	t.Run("JSON path column is rejected in v1", func(t *testing.T) {
		assertInvalidExpr(t, helper, `[JSONField["x"], Int64Field] in [[1, 2]]`)
	})

	// Regression for a review finding: typeutil.IsPrimitiveType includes
	// TIMESTAMPTZ, but castValue's TIMESTAMPTZ branch is an unconditional
	// passthrough (real timestamp literals are validated and converted
	// through a completely separate ISO-string grammar path, never through
	// castValue). Without this explicit rejection, a TIMESTAMPTZ tuple
	// column would silently accept any value kind -- bool, array, a plain
	// (non-ISO) string -- parse successfully, and only fail much later, at
	// execution time, with a type assertion error deep in segcore.
	t.Run("TIMESTAMPTZ column is rejected in v1 (castValue cannot validate it)", func(t *testing.T) {
		assertInvalidExpr(t, helper, `[TimestamptzField, Int64Field] in [["2026-10-13T00:00:00Z", 1]]`)
		assertInvalidExpr(t, helper, `[TimestamptzField, Int64Field] in [[true, 1]]`)
		assertInvalidExpr(t, helper, `[TimestamptzField, Int64Field] in [[[1], 1]]`)
	})

	t.Run("empty tuple list lowers to the same constant-false node as `false and expr`", func(t *testing.T) {
		expr, err := ParseExpr(helper, `[Int64Field, VarCharField] in []`, nil)
		require.NoError(t, err)
		unary := expr.GetUnaryExpr()
		require.NotNil(t, unary, "empty list must fold to NOT(AlwaysTrueExpr), matching alwaysFalseExpr()")
		assert.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
		assert.NotNil(t, unary.GetChild().GetAlwaysTrueExpr())
	})

	t.Run("not in with an empty list: double negation collapses to AlwaysTrueExpr", func(t *testing.T) {
		// Chains two individually-tested pieces that were never tested
		// together: the empty-list fold (-> NOT(AlwaysTrueExpr), i.e.
		// alwaysFalseExpr()) and the `not in` wrap (-> another NOT on top).
		// Naively that's NOT(NOT(AlwaysTrueExpr)) -- but the parser's
		// existing post-visit rewriter (rewriter/entry.go) already
		// recognizes exactly this "NOT of an always-false child" shape and
		// collapses it straight to a bare AlwaysTrueExpr, with no leftover
		// UnaryExpr at all. `[a,b] not in []` is semantically "matches
		// everything" (nothing was excluded), and this confirms the plan
		// says so directly and minimally, not via two redundant NOTs this
		// code happened to build.
		expr, err := ParseExpr(helper, `[Int64Field, VarCharField] not in []`, nil)
		require.NoError(t, err)
		assert.NotNil(t, expr.GetAlwaysTrueExpr(),
			"NOT(NOT(AlwaysTrueExpr)) must be optimized down to a bare AlwaysTrueExpr by the existing rewriter pass")
	})

	t.Run("delete-safety: lowered tuple term is ordinary AND/OR, never membership-unsafe", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, `[Int64Field, VarCharField] in [[1, "a"]]`, nil)
		require.NoError(t, err)
		assert.False(t, PlanContainsMembershipFilterUnsafeForDelete(plan),
			"the lowered form is plain AND/OR/equality, not a Bloom-kind membership filter, and must be delete-eligible")
	})
}
