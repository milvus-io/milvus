package rewriter

import (
	rewriterules "github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter/rules"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

const defaultMaxRuleApplicationsPerInputNode = 64

// rewriteTask is either a pre-visit that schedules children or a post-visit
// that applies rules after those children have finished.
type rewriteTask struct {
	slot      **planpb.Expr
	postVisit bool
}

type rewriter struct {
	rules               []rewriterules.Rule
	maxRuleApplications int
	hitApplicationLimit bool
}

func newRewriter(optimizeEnabled bool) *rewriter {
	return newRewriterWithRules(rewriterules.NewDefaultRules(optimizeEnabled), 0)
}

func newRewriterWithRules(rules []rewriterules.Rule, maxRuleApplications int) *rewriter {
	return &rewriter{
		rules:               rules,
		maxRuleApplications: maxRuleApplications,
	}
}

func (r *rewriter) rewrite(expr *planpb.Expr) *planpb.Expr {
	if expr == nil {
		return nil
	}

	r.hitApplicationLimit = false
	maxRuleApplications := r.maxRuleApplications
	if maxRuleApplications <= 0 {
		maxRuleApplications = defaultMaxRuleApplicationsPerInputNode * countRewriteNodes(expr)
	}

	result := expr
	stack := []rewriteTask{{slot: &result}}
	ruleApplications := 0

	for len(stack) > 0 {
		last := len(stack) - 1
		task := stack[last]
		stack = stack[:last]

		current := *task.slot
		if current == nil {
			continue
		}

		if !task.postVisit {
			// Keep the current-node task below its children on the stack. If a
			// child rewrites itself, it is traversed again before this task runs.
			stack = append(stack, rewriteTask{slot: task.slot, postVisit: true})
			if binary := current.GetBinaryExpr(); binary != nil {
				stack = append(stack,
					rewriteTask{slot: &binary.Right},
					rewriteTask{slot: &binary.Left},
				)
			} else if unary := current.GetUnaryExpr(); unary != nil {
				stack = append(stack, rewriteTask{slot: &unary.Child})
			}
			continue
		}

		for _, rule := range r.rules {
			if !rule.Match(current) {
				continue
			}
			next, changed := rule.Apply(current)
			if !changed || next == nil {
				continue
			}
			*task.slot = next

			ruleApplications++
			if ruleApplications >= maxRuleApplications {
				r.hitApplicationLimit = true
				return result
			}

			// The replacement may have a different shape and may expose new rule
			// inputs in its children, so process the replacement post-order again.
			stack = append(stack, rewriteTask{slot: task.slot})
			break
		}
	}

	return result
}

func countRewriteNodes(expr *planpb.Expr) int {
	count := 0
	stack := []*planpb.Expr{expr}
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if current == nil {
			continue
		}
		count++
		if binary := current.GetBinaryExpr(); binary != nil {
			stack = append(stack, binary.GetLeft(), binary.GetRight())
		} else if unary := current.GetUnaryExpr(); unary != nil {
			stack = append(stack, unary.GetChild())
		}
	}
	if count == 0 {
		return 1
	}
	return count
}
