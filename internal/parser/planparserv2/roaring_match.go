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
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// roaring_match(field, {bitmap}) — exact membership filter. The client builds
// the MRB1 bitmap blob (client/v3/roaringfilter, portable Roaring64 so the C++
// prober reads the same bytes) and passes it as a bytes template parameter; the
// proxy validates it and embeds it into the plan without rebuilding. Unlike
// bloom_match this is exact — no false positives — but its size follows the
// value distribution rather than the member count. See
// docs/design-docs/design_docs/20260714-roaring-exact-membership-expression.md.
const (
	// RoaringMatchFunctionName is the CallExpr function name of the exact
	// membership expression.
	RoaringMatchFunctionName = "roaring_match"

	// mrb1HeaderSize is the fixed MRB1 envelope header length, allowed on top of
	// the body budget in the same way bloom_match allows the MBF1 header.
	mrb1HeaderSize = roaringfilter.HeaderSize
)

// checkRoaringMatchField validates that the probe column is a plain top-level
// signed-integer field. Roaring indexes integers, so unlike bloom_match there is
// no VARCHAR or JSON path: a string would have to be hashed into the integer key
// space first, which would reintroduce the false positives roaring_match exists
// to avoid.
func checkRoaringMatchField(columnInfo *planpb.ColumnInfo, argText string) error {
	if columnInfo == nil {
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of roaring_match must be a scalar field name, got: %s", argText)
	}
	dataType := columnInfo.GetDataType()
	if typeutil.IsJSONType(dataType) || len(columnInfo.GetNestedPath()) != 0 {
		return merr.WrapErrParameterInvalidMsg(
			"roaring_match is not supported on JSON or dynamic fields, got: %s", argText)
	}
	if !typeutil.IsIntegerType(dataType) {
		return merr.WrapErrParameterInvalidMsg(
			"roaring_match only supports INT8/INT16/INT32/INT64 fields, but field (%s) is of type %s",
			argText, dataType.String())
	}
	return nil
}

// validateRoaringBitmapBlob gates and structurally validates a client pre-built
// MRB1 blob before it is embedded into the plan.
//
// The body is fully validated here, not merely bounded. A Roaring body is a
// nested structure of container descriptors, offsets and run intervals, so
// unlike an SBBF body — which is an opaque bit array — a malformed one can drive
// out-of-range reads or quadratic work in the decoder. roaringfilter.Validate
// walks it without materializing a bitmap, in time linear in the supplied bytes,
// and rejects it here, at the proxy,
// rather than letting every QueryNode discover the problem after fan-out.
func validateRoaringBitmapBlob(blob []byte) error {
	// Per-blob gate, shared with bloom_match: proxy.maxMembershipFilterSize budgets
	// the body and the fixed MRB1 header is allowed on top. Checked before Validate
	// so an oversized blob is rejected without decoding it.
	//
	// The proxy separately budgets the whole assembled request's membership-filter-bearing
	// plans before proto.Marshal; this gate bounds one bitmap at its input
	// boundary, which that aggregate check cannot do on its own.
	if maxSize := paramtable.Get().ProxyCfg.MaxMembershipFilterSize.GetAsInt(); len(blob) > maxSize+mrb1HeaderSize {
		bodySize := len(blob) - mrb1HeaderSize
		if bodySize < 0 {
			bodySize = 0
		}
		return merr.WrapErrParameterInvalidMsg(
			"roaring_match bitmap blob body is %d bytes, exceeding proxy.maxMembershipFilterSize (%d); "+
				"a Roaring bitmap's size follows the value distribution, so a sparser member set costs "+
				"more per member than a dense one", bodySize, maxSize)
	}
	_, err := roaringfilter.Validate(blob)
	if err != nil {
		return merr.Wrap(err, "roaring_match bitmap blob is invalid")
	}
	return nil
}

func (v *ParserVisitor) visitRoaringMatch(ctx *parser.CallContext) interface{} {
	allArgs := ctx.AllExpr()
	if len(allArgs) != 2 {
		return merr.WrapErrParameterInvalidMsg(
			"roaring_match requires exactly 2 arguments: roaring_match(field, {bitmap}), got %d", len(allArgs))
	}

	field := allArgs[0].Accept(v)
	if err := getError(field); err != nil {
		return err
	}
	fieldExpr := getExpr(field)
	if fieldExpr == nil {
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of roaring_match must be a scalar field name, got: %s", allArgs[0].GetText())
	}
	columnInfo := toColumnInfo(fieldExpr)
	if err := checkRoaringMatchField(columnInfo, allArgs[0].GetText()); err != nil {
		return err
	}

	values := allArgs[1].Accept(v)
	if err := getError(values); err != nil {
		return merr.WrapErrParameterInvalidMsg(
			"the second argument of roaring_match must be a {template} placeholder carrying a client pre-built roaring bitmap blob")
	}
	valueExpr := getValueExpr(values)
	if valueExpr == nil || !isTemplateExpr(valueExpr) {
		return merr.WrapErrParameterInvalidMsg(
			"the second argument of roaring_match must be a {template} placeholder carrying a client pre-built roaring bitmap blob")
	}

	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_CallExpr{
				CallExpr: &planpb.CallExpr{
					FunctionName: RoaringMatchFunctionName,
					FunctionParameters: []*planpb.Expr{
						fieldExpr.expr,
						{
							Expr:       &planpb.Expr_ValueExpr{ValueExpr: valueExpr},
							IsTemplate: true,
						},
					},
				},
			},
			IsTemplate: true,
		},
		dataType: schemapb.DataType_Bool,
	}
}

// FillRoaringMatchExpressionValue validates a resolved MRB1 bytes template and
// materializes the deferred call into a dedicated exact-membership plan node.
func FillRoaringMatchExpressionValue(
	expr *planpb.Expr,
	call *planpb.CallExpr,
	templateValues map[string]*planpb.GenericValue,
) error {
	params := call.GetFunctionParameters()
	if len(params) != 2 {
		return merr.WrapErrQueryPlanMsg(
			"malformed roaring_match call: expected 2 parameters, got %d", len(params))
	}
	columnParam, ok := params[0].GetExpr().(*planpb.Expr_ColumnExpr)
	if !ok || columnParam.ColumnExpr == nil || columnParam.ColumnExpr.GetInfo() == nil {
		return merr.WrapErrQueryPlanMsg(
			"malformed roaring_match call: first parameter must be a populated column expression")
	}
	columnInfo := columnParam.ColumnExpr.GetInfo()
	if err := checkRoaringMatchField(columnInfo, "deferred column parameter"); err != nil {
		return err
	}

	templateParam, ok := params[1].GetExpr().(*planpb.Expr_ValueExpr)
	if !ok || templateParam.ValueExpr == nil || !params[1].GetIsTemplate() || !isTemplateExpr(templateParam.ValueExpr) {
		return merr.WrapErrQueryPlanMsg(
			"malformed roaring_match call: second parameter must be a template value expression with a non-empty name")
	}
	templateName := templateParam.ValueExpr.GetTemplateVariableName()
	value, ok := templateValues[templateName]
	if !ok {
		return merr.WrapErrQueryPlanMsg(
			"the value of expression template variable name {%s} is not found", templateName)
	}
	blobValue, ok := value.GetVal().(*planpb.GenericValue_BytesVal)
	if !ok {
		return merr.WrapErrQueryPlanMsg(
			"the value of roaring_match template variable {%s} must be a client pre-built roaring bitmap blob (bytes)",
			templateName)
	}
	if err := validateRoaringBitmapBlob(blobValue.BytesVal); err != nil {
		return err
	}
	expr.Expr = &planpb.Expr_RoaringFilterExpr{
		RoaringFilterExpr: &planpb.RoaringFilterExpr{
			ColumnInfo: columnInfo,
			BitmapBlob: blobValue.BytesVal,
		},
	}
	expr.IsTemplate = false
	return nil
}

// hasRoaringFilterExpr reports whether the expression tree contains a roaring
// membership node — either a materialized RoaringFilterExpr or a still-deferred
// roaring_match call. Kept separate from hasBloomFilterExpr rather than merged
// into one "has a filter blob" walk because the two answer different questions:
// the bloom walk also gates the delete path, which roaring_match is allowed to
// use (it is exact, so it cannot remove rows outside the caller's set).
func hasRoaringFilterExpr(expr *planpb.Expr) bool {
	return walkExpr(expr, func(node *planpb.Expr) bool {
		switch e := node.GetExpr().(type) {
		case *planpb.Expr_RoaringFilterExpr:
			return true
		case *planpb.Expr_CallExpr:
			return e.CallExpr.GetFunctionName() == RoaringMatchFunctionName
		default:
			return false
		}
	})
}

// collectRoaringFilterExprs appends every materialized RoaringFilterExpr node
// in the tree. It deliberately uses the same walk as the plan-size and
// element-level guards so log redaction cannot miss a newly nested predicate.
func collectRoaringFilterExprs(expr *planpb.Expr, out *[]*planpb.RoaringFilterExpr) {
	walkExpr(expr, func(node *planpb.Expr) bool {
		if e, ok := node.GetExpr().(*planpb.Expr_RoaringFilterExpr); ok {
			*out = append(*out, e.RoaringFilterExpr)
		}
		return false
	})
}

// PlanContainsRoaringFilter reports whether the plan's main predicate or a
// scorer filter contains a roaring_match expression.
func PlanContainsRoaringFilter(plan *planpb.PlanNode) bool {
	if plan == nil {
		return false
	}
	for _, scorer := range plan.GetScorers() {
		if hasRoaringFilterExpr(scorer.GetFilter()) {
			return true
		}
	}
	switch realPlan := plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		return hasRoaringFilterExpr(realPlan.VectorAnns.GetPredicates())
	case *planpb.PlanNode_Predicates:
		return hasRoaringFilterExpr(realPlan.Predicates)
	case *planpb.PlanNode_Query:
		return hasRoaringFilterExpr(realPlan.Query.GetPredicates())
	}
	return false
}
