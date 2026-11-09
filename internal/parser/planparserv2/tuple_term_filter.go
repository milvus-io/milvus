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

// Correlated multi-column membership: `[a, b] in [[v1,w1], [v2,w2]]`. See
// docs/design-docs/design_docs/20260901-tuple-term-membership-expression.md
// and the v1-scope discussion on https://github.com/milvus-io/milvus/issues/52832.
//
// v1 is syntactic sugar, not a new plan node or execution operator:
//
//	[a, b] in [[v1,w1], [v2,w2]]
//
// lowers, at parse time, into exactly the expression a user would otherwise
// have to write by hand:
//
//	(a == v1 AND b == w1) OR (a == v2 AND b == w2)
//
// This reuses the existing UnaryRangeExpr (column == literal), AND/OR
// (foldAndOperands/foldOrOperands), and their three-valued NULL handling
// unchanged -- no new segcore execution code, and no new plan-proto message.
// That also means no scalar-index coarse filter and no single hash-set probe:
// each AND-branch's equality can use a column's existing index individually,
// but there is no sharing of work across tuples the way a dedicated
// tuple-membership operator (hash-set lookup, O(1) per row regardless of
// tuple count) would have. For a tuple count where that gap actually matters,
// a dedicated operator is real, scoped follow-up work, not a v1 requirement.
//
// VisitTerm dispatches here when the left-hand side of `in` is a bracketed
// list of two or more plain field identifiers (isTupleTermLHS). This is
// intercepted before the left-hand side is generically visited: VisitArray
// unconditionally rejects non-constant elements ("array element type must be
// generic value"), so `[a, b]` would otherwise fail inside VisitArray itself,
// before VisitTerm's own single-column check ever runs.

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// isTupleTermLHS reports whether arrayCtx is a bracketed list of two or more
// plain top-level field identifiers, e.g. `[a, b]`. `$meta` (the dynamic
// field) is excluded: dynamic/JSON-path columns are a v1 non-goal.
//
// Requiring at least two elements keeps this new syntax scoped to genuine
// multi-column tuples; a single-element `[a]` on the left of `in` is left to
// fall through to the existing (erroring) VisitArray path, unchanged from
// today's behavior.
func isTupleTermLHS(arrayCtx *parser.ArrayContext) bool {
	elems := arrayCtx.AllExpr()
	if len(elems) < 2 {
		return false
	}
	for _, e := range elems {
		idCtx, ok := e.(*parser.IdentifierContext)
		if !ok {
			return false
		}
		if idCtx.GetText() == "$meta" {
			return false
		}
	}
	return true
}

// buildTupleTermExpr builds the lowered OR-of-ANDs expression for ctx
// (`[cols] [not] in [tuples]`), given that arrayCtx (== ctx.Expr(0)) already
// satisfied isTupleTermLHS.
func (v *ParserVisitor) buildTupleTermExpr(ctx *parser.TermContext, arrayCtx *parser.ArrayContext) interface{} {
	columnExprs := arrayCtx.AllExpr()
	columns := make([]*planpb.ColumnInfo, len(columnExprs))
	dataTypes := make([]schemapb.DataType, len(columnExprs))
	seenFieldIDs := make(map[int64]string, len(columnExprs))

	for i, ce := range columnExprs {
		res := ce.Accept(v)
		if err := getError(res); err != nil {
			return err
		}
		exprWithType := getExpr(res)
		columnInfo := toColumnInfo(exprWithType)
		if columnInfo == nil {
			return merr.WrapErrParameterInvalidMsg(
				"tuple 'in' left-hand side element must be a plain field, but got: %s", ce.GetText())
		}
		if !typeutil.IsPrimitiveType(columnInfo.GetDataType()) {
			return merr.WrapErrParameterInvalidMsg(
				"tuple 'in' only supports top-level scalar fields, but field '%s' has type %s",
				ce.GetText(), columnInfo.GetDataType().String())
		}
		// typeutil.IsPrimitiveType includes TIMESTAMPTZ, but castValue's
		// TIMESTAMPTZ branch is an unconditional passthrough: real timestamp
		// literals never reach castValue at all (they go through a dedicated
		// ISO-string grammar path that calls
		// timestamptz.ValidateAndReturnUnixMicroTz with the query's timezone
		// -- see the TimestamptzCompareForward/Reverse rules in Plan.g4).
		// Routed through castValue instead, as every tuple value is here, a
		// TIMESTAMPTZ column would silently accept a bool/string/array
		// literal with no validation, producing a plan whose equality node
		// carries the wrong GenericValue kind -- segcore's TIMESTAMPTZ
		// execution path requires kInt64Val and would fail a type assertion
		// at evaluation time, not at parse time. Reject it explicitly here,
		// the same way JSON/ARRAY columns are already excluded; proper
		// support needs the same ISO-string validation/conversion ordinary
		// timestamp comparisons use, which is out of scope for this v1.
		if typeutil.IsTimestamptzType(columnInfo.GetDataType()) {
			return merr.WrapErrParameterInvalidMsg(
				"tuple 'in' does not yet support TIMESTAMPTZ fields, but field '%s' has that type",
				ce.GetText())
		}
		if prev, dup := seenFieldIDs[columnInfo.GetFieldId()]; dup {
			return merr.WrapErrParameterInvalidMsg(
				"tuple 'in' left-hand side columns must be distinct, but '%s' and '%s' refer to the same field",
				prev, ce.GetText())
		}
		seenFieldIDs[columnInfo.GetFieldId()] = ce.GetText()
		columns[i] = columnInfo
		dataTypes[i] = columnInfo.GetDataType()
	}

	term := ctx.Expr(1).Accept(v)
	if err := getError(term); err != nil {
		return err
	}
	valueExpr := getValueExpr(term)
	if valueExpr.GetValue() == nil && valueExpr.GetTemplateVariableName() != "" {
		// Templated RHS (`[a,b] in {tuples}`) is structurally representable
		// (nested TemplateArrayValue already lowers into nested GenericValue,
		// see convert_field_data_to_generic_value.go), but is an explicit v1
		// non-goal: it is not wired here yet.
		return merr.WrapErrParameterInvalidMsg(
			"tuple 'in' does not yet support a template right-hand side, use a literal list of tuples")
	}

	rhsValue := valueExpr.GetValue()
	if rhsValue == nil {
		return merr.WrapErrParameterInvalidMsg(
			"value '%s' in tuple list cannot be a non-const expression", ctx.Expr(1).GetText())
	}
	if !IsArray(rhsValue) {
		return merr.WrapErrParameterInvalidMsg(
			"the right-hand side of tuple 'in' must be a list of tuples, but got: %s", ctx.Expr(1).GetText())
	}

	rawTuples := rhsValue.GetArrayVal().GetArray()

	// Empty list: `[a,b] in []` matches nothing, same as single-column
	// `field in []` today. An OR with zero disjuncts is not a buildable
	// shape, so fold directly to the same constant-false node the existing
	// boolean-literal folding already produces for e.g. `false and expr`
	// (see alwaysFalseExpr in utils.go) -- not a bespoke false node.
	if len(rawTuples) == 0 {
		expr := &ExprWithType{
			expr:     alwaysFalseExpr(),
			dataType: schemapb.DataType_Bool,
		}
		return wrapTupleTermNot(ctx, expr)
	}

	tupleOperands := make([]interface{}, len(rawTuples))
	for i, rawTuple := range rawTuples {
		if !IsArray(rawTuple) {
			return merr.WrapErrParameterInvalidMsg(
				"each element of tuple 'in' right-hand side must itself be a list of %d values, but got: %s",
				len(columns), rawTuple.String())
		}
		rawElems := rawTuple.GetArrayVal().GetArray()
		if len(rawElems) != len(columns) {
			return merr.WrapErrParameterInvalidMsg(
				"tuple 'in' right-hand side element %d has %d values, but %d columns were given on the left-hand side",
				i, len(rawElems), len(columns))
		}

		// One tuple -> one AND-branch: (col[0] == val[0]) AND (col[1] ==
		// val[1]) AND ... Each equality is exactly the UnaryRangeExpr
		// (column == literal) the ordinary `field == literal` grammar path
		// builds (see handleCompareRightValue in utils.go) -- built directly
		// here since the column/value pair is already resolved and cast,
		// rather than re-deriving it through that generic path.
		eqOperands := make([]interface{}, len(columns))
		for j, rawElem := range rawElems {
			casted, err := castValue(dataTypes[j], rawElem)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg(
					"tuple 'in' value '%s' at position %d of tuple %d cannot be cast to %s",
					rawElem.String(), j, i, dataTypes[j].String())
			}
			eqOperands[j] = &ExprWithType{
				expr: &planpb.Expr{
					Expr: &planpb.Expr_UnaryRangeExpr{
						UnaryRangeExpr: &planpb.UnaryRangeExpr{
							ColumnInfo: columns[j],
							Op:         planpb.OpType_Equal,
							Value:      casted,
						},
					},
				},
				dataType: schemapb.DataType_Bool,
			}
		}

		tupleResult := v.foldAndOperands(eqOperands)
		if err := getError(tupleResult); err != nil {
			return err
		}
		tupleOperands[i] = tupleResult
	}

	result := v.foldOrOperands(tupleOperands)
	if err := getError(result); err != nil {
		return err
	}

	return wrapTupleTermNot(ctx, getExpr(result))
}

// wrapTupleTermNot applies the Term rule's own `op=NOT?` capture (the NOT in
// `[...] NOT IN [...]`) to the already-built lowered expression, exactly the
// same way VisitTerm's single-column path wraps a TermExpr -- a plain,
// generic UnaryExpr{Op: Not}, not a bespoke polarity on the lowered tree.
func wrapTupleTermNot(ctx *parser.TermContext, expr *ExprWithType) interface{} {
	if ctx.GetOp() == nil {
		return expr
	}
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_UnaryExpr{
				UnaryExpr: &planpb.UnaryExpr{
					Op:    planpb.UnaryExpr_Not,
					Child: expr.expr,
				},
			},
		},
		dataType: schemapb.DataType_Bool,
	}
}
