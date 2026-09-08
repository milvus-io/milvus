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

// Unified control flow for the membership-filter expression family. The unified
// surface name shares one visitor skeleton and one deferred-call fill path,
// and one set of tree walkers; everything that genuinely differs between them
// — envelope validation, field-type domains, delete safety — lives in the
// per-kind validators (bloom_match.go / roaring_match.go) and is selected
// through membershipKind by sniffing the blob's magic header. See
// docs/design-docs/design_docs/20260822-membership-match-expression.md.

import (
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	parser "github.com/milvus-io/milvus/internal/parser/planparserv2/generated"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/roaringfilter"
)

// MembershipMatchFunctionName is the unified membership-filter surface syntax,
// membership_match(field, {blob}, type=bloom|roaring). The type is optional;
// when omitted the filter kind is derived from the blob's magic header.
const MembershipMatchFunctionName = "membership_match"

// membershipKind identifies which concrete membership structure a blob (and
// therefore a materialized plan node) carries.
type membershipKind int

const (
	membershipUnknown membershipKind = iota
	membershipBloom
	membershipRoaring
)

// isMembershipFunctionName reports whether name is the membership-filter
// surface function handled by the unified skeleton.
func isMembershipFunctionName(name string) bool {
	return name == MembershipMatchFunctionName
}

// sniffMembershipKind identifies the blob format from its magic header. Both
// envelopes are self-describing (4-byte magic at offset 0), so the optional
// type argument is only a consistency pin; an unrecognized header is a request
// error, never a guess.
func sniffMembershipKind(blob []byte) (membershipKind, error) {
	if len(blob) < 4 {
		return membershipUnknown, merr.WrapErrParameterInvalidMsg(
			"membership filter blob is %d bytes, too short to carry a format header", len(blob))
	}
	switch string(blob[0:4]) {
	case mbf1Magic:
		return membershipBloom, nil
	case roaringfilter.Magic:
		return membershipRoaring, nil
	default:
		// The blob is supplied out of band and this error is logged by Proxy.
		// Report the protocol expectation without echoing caller-controlled bytes.
		return membershipUnknown, merr.WrapErrParameterInvalidMsg(
			"membership filter blob has unknown format magic; supported formats are %q (bloom) and %q (roaring)",
			mbf1Magic, roaringfilter.Magic)
	}
}

// checkMembershipField dispatches the per-kind probe-column validation.
func checkMembershipField(kind membershipKind, columnInfo *planpb.ColumnInfo, argText, functionName string) error {
	switch kind {
	case membershipBloom:
		return checkBloomMatchField(columnInfo, argText, functionName)
	case membershipRoaring:
		return checkRoaringMatchField(columnInfo, argText, functionName)
	default:
		return merr.WrapErrParameterInvalidMsg("unknown membership filter kind: %d", kind)
	}
}

// visitMembershipCall emits a deferred CallExpr for membership_match. Field
// domain validation happens at fill time because the kind is unknown until the
// blob's magic header is read.
func (v *ParserVisitor) visitMembershipCall(ctx *parser.CallContext, functionName string) interface{} {
	return v.visitMembershipArgs(ctx.AllExpr(), functionName)
}

func (v *ParserVisitor) visitMembershipArgs(allArgs []parser.IExprContext, functionName string) interface{} {
	if len(allArgs) != 2 {
		return merr.WrapErrParameterInvalidMsg(
			"%s requires exactly 2 positional arguments; use type=bloom|roaring for the optional kind", functionName)
	}

	field := allArgs[0].Accept(v)
	if err := getError(field); err != nil {
		return err
	}
	fieldExpr := getExpr(field)
	if fieldExpr == nil {
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of %s must be a scalar field name, got: %s", functionName, allArgs[0].GetText())
	}
	columnInfo := toColumnInfo(fieldExpr)
	if columnInfo == nil {
		// Kind-agnostic structural floor; the per-kind validator re-checks the
		// full field domain once the blob kind is resolved at fill time.
		return merr.WrapErrParameterInvalidMsg(
			"the first argument of %s must be a scalar field name, got: %s", functionName, allArgs[0].GetText())
	}

	values := allArgs[1].Accept(v)
	if err := getError(values); err != nil {
		return err
	}
	valueExpr := getValueExpr(values)
	if valueExpr == nil || !isTemplateExpr(valueExpr) {
		// Deliberately does not echo the argument: a literal list would put every
		// member value into a client-facing error and the server log.
		return merr.WrapErrParameterInvalidMsg(
			"the second argument of %s must be a {template} placeholder carrying a client pre-built membership filter blob", functionName)
	}

	// Deferred: FillExpressionValue validates the blob and materializes the
	// dedicated plan node once the template value is resolved.
	params := []*planpb.Expr{fieldExpr.expr, {
		Expr:       &planpb.Expr_ValueExpr{ValueExpr: valueExpr},
		IsTemplate: true,
	}}
	return &ExprWithType{
		expr: &planpb.Expr{
			Expr: &planpb.Expr_CallExpr{
				CallExpr: &planpb.CallExpr{
					FunctionName:       functionName,
					FunctionParameters: params,
				},
			},
			IsTemplate: true,
		},
		dataType: schemapb.DataType_Bool,
	}
}

// fillMembershipMatchExpressionValue resolves the template placeholder of a
// deferred membership-filter call, determines the filter kind from the blob
// magic, checks an optional explicit type pin, validates the client pre-built
// blob with the per-kind validator, and rewrites the node into its dedicated
// plan node in place.
//
// Every parameter access is guarded so a malformed call produces an input
// error instead of a nil dereference.
func fillMembershipMatchExpressionValue(
	expr *planpb.Expr,
	call *planpb.CallExpr,
	templateValues map[string]*planpb.GenericValue,
	ctx *fillExpressionContext,
) error {
	functionName := call.GetFunctionName()
	params := call.GetFunctionParameters()
	if len(params) != 2 && len(params) != 3 {
		return merr.WrapErrQueryPlanMsg(
			"malformed %s call: expected 2 parameters plus optional type, got %d", functionName, len(params))
	}
	columnParam, ok := params[0].GetExpr().(*planpb.Expr_ColumnExpr)
	if !ok || columnParam.ColumnExpr == nil || columnParam.ColumnExpr.GetInfo() == nil {
		return merr.WrapErrQueryPlanMsg(
			"malformed %s call: first parameter must be a populated column expression", functionName)
	}
	columnInfo := columnParam.ColumnExpr.GetInfo()

	templateParam, ok := params[1].GetExpr().(*planpb.Expr_ValueExpr)
	if !ok || templateParam.ValueExpr == nil || !params[1].GetIsTemplate() || !isTemplateExpr(templateParam.ValueExpr) {
		return merr.WrapErrQueryPlanMsg(
			"malformed %s call: second parameter must be a template value expression with a non-empty name", functionName)
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
			"the value of %s template variable {%s} must be a client pre-built membership filter blob (bytes)",
			functionName, templateName)
	}

	kind, err := sniffMembershipKind(blobValue.BytesVal)
	if err != nil {
		return err
	}
	if len(params) == 3 {
		option, ok := params[2].GetExpr().(*planpb.Expr_ValueExpr)
		if !ok || option.ValueExpr == nil || option.ValueExpr.GetValue() == nil {
			return merr.WrapErrQueryPlanMsg("malformed %s call: type must be bloom or roaring", functionName)
		}
		typeName := strings.ToLower(option.ValueExpr.GetValue().GetStringVal())
		var requested membershipKind
		switch typeName {
		case "bloom":
			requested = membershipBloom
		case "roaring":
			requested = membershipRoaring
		default:
			return merr.WrapErrQueryPlanMsg("malformed %s call: type must be bloom or roaring", functionName)
		}
		if requested != kind {
			return merr.WrapErrQueryPlanMsg("%s type %q does not match filter blob format", functionName, typeName)
		}
	}
	// Re-validate the probe column against the resolved kind even when the
	// visitor already checked it: fills also serve hand-assembled plans (the
	// c-shared parser boundary), and the unified name resolves its kind only
	// here. Fail closed rather than fan out an out-of-domain probe.
	if err := checkMembershipField(kind, columnInfo, ctx.membershipFieldName(columnInfo), functionName); err != nil {
		return err
	}

	if err := materializeMembershipBlob(expr, kind, columnInfo, templateName, blobValue.BytesVal, ctx); err != nil {
		return err
	}
	expr.IsTemplate = false
	return nil
}

// walkExpr visits every expression node until visit returns true. Keeping the
// recursion in one place prevents blob accounting, delete safety, element-level
// guards, plan-size accounting, and log redaction from drifting as new
// container nodes are added.
func walkExpr(expr *planpb.Expr, visit func(*planpb.Expr) bool) bool {
	if expr == nil {
		return false
	}
	if visit(expr) {
		return true
	}
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_CallExpr:
		for _, param := range e.CallExpr.GetFunctionParameters() {
			if walkExpr(param, visit) {
				return true
			}
		}
	case *planpb.Expr_UnaryExpr:
		return walkExpr(e.UnaryExpr.GetChild(), visit)
	case *planpb.Expr_BinaryExpr:
		return walkExpr(e.BinaryExpr.GetLeft(), visit) || walkExpr(e.BinaryExpr.GetRight(), visit)
	case *planpb.Expr_BinaryArithExpr:
		return walkExpr(e.BinaryArithExpr.GetLeft(), visit) || walkExpr(e.BinaryArithExpr.GetRight(), visit)
	case *planpb.Expr_RandomSampleExpr:
		return walkExpr(e.RandomSampleExpr.GetPredicate(), visit)
	case *planpb.Expr_ElementFilterExpr:
		return walkExpr(e.ElementFilterExpr.GetElementExpr(), visit) ||
			walkExpr(e.ElementFilterExpr.GetPredicate(), visit)
	case *planpb.Expr_MatchExpr:
		return walkExpr(e.MatchExpr.GetPredicate(), visit)
	}
	return false
}

// materializeMembershipBlob runs the per-kind admission gate and writes the
// matching materialized plan node into expr. The roaring path reuses a
// request-cached structural validation when the same bytes were already
// validated for another occurrence of the same template name.
func materializeMembershipBlob(
	expr *planpb.Expr,
	kind membershipKind,
	columnInfo *planpb.ColumnInfo,
	templateName string,
	blob []byte,
	ctx *fillExpressionContext,
) error {
	switch kind {
	case membershipBloom:
		validatedBlob, err := validateBloomFilterBlob(blob)
		if err != nil {
			return err
		}
		// Envelope is structurally valid here, so the domain byte is safe to read.
		if err := checkBloomFilterValueDomain(columnInfo, validatedBlob); err != nil {
			return err
		}
		expr.Expr = &planpb.Expr_BloomFilterExpr{
			BloomFilterExpr: &planpb.BloomFilterExpr{
				ColumnInfo: columnInfo,
				FilterBlob: validatedBlob,
			},
		}
		return nil
	case membershipRoaring:
		validated, err := ctx.validatedRoaringBlob(templateName, blob)
		if err != nil {
			return err
		}
		expr.Expr = &planpb.Expr_RoaringFilterExpr{
			RoaringFilterExpr: &planpb.RoaringFilterExpr{
				ColumnInfo: columnInfo,
				BitmapBlob: validated.blob,
			},
		}
		return nil
	default:
		return merr.WrapErrQueryPlanMsg("unknown membership filter kind: %d", kind)
	}
}

// --- Tree walkers -----------------------------------------------------------
//
// One walker serves blob accounting, delete safety, element-level guards, plan
// size accounting, and log redaction, so they cannot drift as new container
// nodes are added.

// hasMembershipFilterExpr reports whether the expression tree contains any
// membership-filter node — a materialized Bloom/RoaringFilterExpr or a
// still-deferred call of any surface name.
func hasMembershipFilterExpr(expr *planpb.Expr) bool {
	return walkExpr(expr, func(node *planpb.Expr) bool {
		switch e := node.GetExpr().(type) {
		case *planpb.Expr_BloomFilterExpr, *planpb.Expr_RoaringFilterExpr:
			return true
		case *planpb.Expr_CallExpr:
			return isMembershipFunctionName(e.CallExpr.GetFunctionName())
		default:
			return false
		}
	})
}

// hasDeleteUnsafeMembershipFilterExpr reports whether the tree contains a
// membership node whose kind must not drive deletes: a materialized
// BloomFilterExpr, or a deferred call whose kind is (or cannot yet be proven
// not to be) approximate. Deferred calls do not survive a filled plan in
// practice; treating them as unsafe keeps the guard correct even if it is ever
// reached before filling.
func hasDeleteUnsafeMembershipFilterExpr(expr *planpb.Expr) bool {
	return walkExpr(expr, func(node *planpb.Expr) bool {
		switch e := node.GetExpr().(type) {
		case *planpb.Expr_BloomFilterExpr:
			return true
		case *planpb.Expr_RoaringFilterExpr:
			return false
		case *planpb.Expr_CallExpr:
			// Deferred unified call: kind unknown here, fail closed.
			return isMembershipFunctionName(e.CallExpr.GetFunctionName())
		default:
			return false
		}
	})
}

// collectMembershipFilterExprs appends every materialized membership node in
// the tree, regardless of kind, as blob slots redaction can elide.
type membershipBlobSlot struct {
	get func() []byte
	set func([]byte)
}

func collectMembershipFilterExprs(expr *planpb.Expr, out *[]membershipBlobSlot) {
	walkExpr(expr, func(node *planpb.Expr) bool {
		switch e := node.GetExpr().(type) {
		case *planpb.Expr_BloomFilterExpr:
			bf := e.BloomFilterExpr
			*out = append(*out, membershipBlobSlot{
				get: func() []byte { return bf.FilterBlob },
				set: func(b []byte) { bf.FilterBlob = b },
			})
		case *planpb.Expr_RoaringFilterExpr:
			rf := e.RoaringFilterExpr
			*out = append(*out, membershipBlobSlot{
				get: func() []byte { return rf.BitmapBlob },
				set: func(b []byte) { rf.BitmapBlob = b },
			})
		}
		return false
	})
}

func planPredicates(plan *planpb.PlanNode) *planpb.Expr {
	switch realPlan := plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		return realPlan.VectorAnns.GetPredicates()
	case *planpb.PlanNode_Predicates:
		return realPlan.Predicates
	case *planpb.PlanNode_Query:
		return realPlan.Query.GetPredicates()
	}
	return nil
}

func planHasMembershipExpr(plan *planpb.PlanNode, pred func(*planpb.Expr) bool) bool {
	if plan == nil {
		return false
	}
	for _, scorer := range plan.GetScorers() {
		if pred(scorer.GetFilter()) {
			return true
		}
	}
	return pred(planPredicates(plan))
}

// PlanContainsMembershipFilter reports whether the plan's main predicate or any
// scorer filter contains a membership filter of any kind. Membership blobs are
// embedded in the plan and fanned out to every QueryNode, so proxy plan-size
// accounting charges every occurrence against the shared budget.
func PlanContainsMembershipFilter(plan *planpb.PlanNode) bool {
	return planHasMembershipExpr(plan, hasMembershipFilterExpr)
}

// PlanContainsMembershipFilterUnsafeForDelete reports whether the plan carries
// a membership filter whose kind must not drive deletes (approximate kinds, and
// anything whose kind cannot be proven exact). Used by the proxy delete path.
func PlanContainsMembershipFilterUnsafeForDelete(plan *planpb.PlanNode) bool {
	return planHasMembershipExpr(plan, hasDeleteUnsafeMembershipFilterExpr)
}
