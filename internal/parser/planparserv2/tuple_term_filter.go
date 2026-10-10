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
// docs/design-docs/design_docs/20260901-tuple-term-membership-expression.md.
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

// buildTupleTermExpr builds a TupleTermExpr from ctx (`[cols] [not] in
// [tuples]`), given that arrayCtx (== ctx.Expr(0)) already satisfied
// isTupleTermLHS.
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
	tuples := make([]*planpb.Array, len(rawTuples))
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
		castedElems := make([]*planpb.GenericValue, len(rawElems))
		for j, rawElem := range rawElems {
			casted, err := castValue(dataTypes[j], rawElem)
			if err != nil {
				return merr.WrapErrParameterInvalidMsg(
					"tuple 'in' value '%s' at position %d of tuple %d cannot be cast to %s",
					rawElem.String(), j, i, dataTypes[j].String())
			}
			castedElems[j] = casted
		}
		tuples[i] = &planpb.Array{Array: castedElems}
	}

	expr := &planpb.Expr{
		Expr: &planpb.Expr_TupleTermExpr{
			TupleTermExpr: &planpb.TupleTermExpr{
				Columns: columns,
				Tuples:  tuples,
			},
		},
	}
	if ctx.GetOp() != nil {
		expr = &planpb.Expr{
			Expr: &planpb.Expr_UnaryExpr{
				UnaryExpr: &planpb.UnaryExpr{
					Op:    planpb.UnaryExpr_Not,
					Child: expr,
				},
			},
		}
	}

	return &ExprWithType{
		expr:     expr,
		dataType: schemapb.DataType_Bool,
	}
}
