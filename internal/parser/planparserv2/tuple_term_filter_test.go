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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestExpr_TupleTerm(t *testing.T) {
	helper := newTestSchemaHelper(t)

	t.Run("valid 2-column tuple in", func(t *testing.T) {
		expr, err := ParseExpr(helper, `[Int64Field, VarCharField] in [[1, "a"], [2, "b"]]`, nil)
		require.NoError(t, err)
		tte := expr.GetTupleTermExpr()
		require.NotNil(t, tte, "must materialize as TupleTermExpr, not TermExpr/OR-of-equalities")
		require.Len(t, tte.GetColumns(), 2)
		assert.Equal(t, schemapb.DataType_Int64, tte.GetColumns()[0].GetDataType())
		assert.Equal(t, schemapb.DataType_VarChar, tte.GetColumns()[1].GetDataType())
		require.Len(t, tte.GetTuples(), 2)
		require.Len(t, tte.GetTuples()[0].GetArray(), 2)
		assert.EqualValues(t, 1, tte.GetTuples()[0].GetArray()[0].GetInt64Val())
		assert.Equal(t, "a", tte.GetTuples()[0].GetArray()[1].GetStringVal())
	})

	t.Run("valid 3-column tuple in", func(t *testing.T) {
		assertValidExpr(t, helper, `[Int64Field, VarCharField, BoolField] in [[1, "a", true], [2, "b", false]]`)
	})

	// NOT must be written `[...] not in [...]` (NOT immediately before IN),
	// matching how single-column `field not in [...]` already works — NOT a
	// bespoke polarity field on TupleTermExpr itself, just the existing
	// generic UnaryExpr wrap this grammar's Term rule already applies
	// (parser_visitor.go ctx.GetOp() != nil). The prefix form `not [...] in
	// [...]` is a DIFFERENT, invalid parse under this grammar's operator
	// precedence: the generic unary NOT (`op=(ADD|SUB|BNOT|NOT) expr #
	// Unary`) binds to the bracketed array first as its own primary operand
	// (`(not [...]) in [...]`), not to the whole `in` expression — the exact
	// same precedence single-column IN already has (`not Int64Field in
	// [1,2]` is likewise invalid; only `Int64Field not in [1,2]` works).
	t.Run("not wraps in UnaryExpr, not a bespoke polarity field", func(t *testing.T) {
		expr, err := ParseExpr(helper, `[Int64Field, VarCharField] not in [[1, "a"]]`, nil)
		require.NoError(t, err)
		unary := expr.GetUnaryExpr()
		require.NotNil(t, unary)
		assert.Equal(t, planpb.UnaryExpr_Not, unary.GetOp())
		require.NotNil(t, unary.GetChild().GetTupleTermExpr())
	})

	t.Run("prefix NOT before the bracket is invalid, same as single-column IN", func(t *testing.T) {
		assertInvalidExpr(t, helper, `not [Int64Field, VarCharField] in [[1, "a"]]`)
		assertInvalidExpr(t, helper, `not Int64Field in [1, 2]`)
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

	t.Run("empty tuple list parses", func(t *testing.T) {
		assertValidExpr(t, helper, `[Int64Field, VarCharField] in []`)
	})

	t.Run("delete-safety: tuple term is exact, not delete-unsafe", func(t *testing.T) {
		plan, err := CreateRetrievePlan(helper, `[Int64Field, VarCharField] in [[1, "a"]]`, nil)
		require.NoError(t, err)
		assert.False(t, PlanContainsMembershipFilterUnsafeForDelete(plan),
			"TupleTermExpr is exact (no false positives) and must not be classified as a delete-unsafe membership filter")
	})
}
