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

// Log redaction for plans carrying values supplied through expression
// templates or large membership filter blobs. RedactPlanForLog makes sure none
// of those caller-supplied values lands verbatim in a proxy log.

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// redactedPlan wraps a plan so its String() elides template-substituted values
// and membership blobs. As a fmt.Stringer, mlog.Stringer defers the work: when
// the log level is disabled, String() is never called and there is zero cost.
type redactedPlan struct{ plan *planpb.PlanNode }

// redactTemplateValues swaps every value that arrived through an expression
// template for a marker, returning a restore func.
//
// Template substitution keeps template_variable_name on the filled node, which
// is what lets this tell a caller-supplied out-of-band value from a literal
// written into the expression text. Only the former is elided: a literal is
// already in the caller's hands and is what makes a logged plan readable.
func redactTemplateValues(root *planpb.Expr) func() {
	var restores []func()
	elide := func(name string, slot **planpb.GenericValue) {
		if name == "" || *slot == nil {
			return
		}
		saved := *slot
		*slot = &planpb.GenericValue{
			Val: &planpb.GenericValue_StringVal{
				StringVal: fmt.Sprintf("<template %s elided>", name),
			},
		}
		restores = append(restores, func() { *slot = saved })
	}
	elideList := func(name string, slot *[]*planpb.GenericValue) {
		if name == "" || len(*slot) == 0 {
			return
		}
		saved := *slot
		*slot = []*planpb.GenericValue{{
			Val: &planpb.GenericValue_StringVal{
				StringVal: fmt.Sprintf("<template %s elided, %d values>", name, len(saved)),
			},
		}}
		restores = append(restores, func() { *slot = saved })
	}

	walkExpr(root, func(node *planpb.Expr) bool {
		switch e := node.GetExpr().(type) {
		case *planpb.Expr_ValueExpr:
			elide(e.ValueExpr.GetTemplateVariableName(), &e.ValueExpr.Value)
		case *planpb.Expr_UnaryRangeExpr:
			elide(e.UnaryRangeExpr.GetTemplateVariableName(), &e.UnaryRangeExpr.Value)
		case *planpb.Expr_BinaryRangeExpr:
			elide(e.BinaryRangeExpr.GetLowerTemplateVariableName(), &e.BinaryRangeExpr.LowerValue)
			elide(e.BinaryRangeExpr.GetUpperTemplateVariableName(), &e.BinaryRangeExpr.UpperValue)
		case *planpb.Expr_TermExpr:
			elideList(e.TermExpr.GetTemplateVariableName(), &e.TermExpr.Values)
		case *planpb.Expr_JsonContainsExpr:
			elideList(e.JsonContainsExpr.GetTemplateVariableName(), &e.JsonContainsExpr.Elements)
		case *planpb.Expr_BinaryArithOpEvalRangeExpr:
			elide(e.BinaryArithOpEvalRangeExpr.GetOperandTemplateVariableName(),
				&e.BinaryArithOpEvalRangeExpr.RightOperand)
			elide(e.BinaryArithOpEvalRangeExpr.GetValueTemplateVariableName(),
				&e.BinaryArithOpEvalRangeExpr.Value)
		}
		return false
	})

	return func() {
		for i := len(restores) - 1; i >= 0; i-- {
			restores[i]()
		}
	}
}

func (p redactedPlan) String() string {
	if p.plan == nil {
		return "<nil>"
	}

	// Values supplied out of band never reach the log, membership blob or not.
	restore := redactTemplateValues(planPredicates(p.plan))
	defer restore()
	for _, sc := range p.plan.GetScorers() {
		defer redactTemplateValues(sc.GetFilter())()
	}
	var slots []membershipBlobSlot
	collectMembershipFilterExprs(planPredicates(p.plan), &slots)
	// Scorer filters carry their own predicate tree and can embed membership
	// blobs too (function-score / rerank filters); redact those as well.
	for _, sc := range p.plan.GetScorers() {
		collectMembershipFilterExprs(sc.GetFilter(), &slots)
	}
	if len(slots) == 0 {
		return p.plan.String()
	}
	// Swap each blob for a short {N bytes} marker (a byte-slice pointer
	// assignment — no copy, unlike proto.Clone which would duplicate the
	// up-to-tens-of-MiB body), stringify, then restore the originals via defer.
	// Safe because zap evaluates a Stringer field synchronously in this
	// goroutine at the log call, and these call sites own the task-local plan, so
	// no other reader observes the temporary state.
	saved := make([][]byte, len(slots))
	for i, slot := range slots {
		saved[i] = slot.get()
		slot.set(fmt.Appendf(nil, "<%d bytes elided>", len(saved[i])))
	}
	defer func() {
		for i, slot := range slots {
			slot.set(saved[i])
		}
	}()
	return p.plan.String()
}

// RedactPlanForLog returns a fmt.Stringer that renders the plan with every
// template-substituted value and membership_match blob replaced by a marker, so
// caller-supplied values and up-to-tens-of-MiB membership payloads never land
// verbatim in a proxy debug log. Cheap when logging is disabled (lazy) and when
// the plan has no membership blob (no clone).
func RedactPlanForLog(plan *planpb.PlanNode) fmt.Stringer {
	return redactedPlan{plan: plan}
}
