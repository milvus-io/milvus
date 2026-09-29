package rewriter

import (
	"testing"

	"github.com/stretchr/testify/require"

	rewriterules "github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter/rules"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

type testRewriteRule struct {
	matchExpr func(expr *planpb.Expr) bool
	rewrite   func(expr *planpb.Expr) (*planpb.Expr, bool)
}

func (r testRewriteRule) Match(expr *planpb.Expr) bool {
	return r.matchExpr == nil || r.matchExpr(expr)
}

func (r testRewriteRule) Apply(expr *planpb.Expr) (*planpb.Expr, bool) {
	return r.rewrite(expr)
}

func TestRewriterAppliesOnlyMatchingRules(t *testing.T) {
	applyCount := 0
	rule := testRewriteRule{
		matchExpr: func(*planpb.Expr) bool { return false },
		rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
			applyCount++
			return expr, false
		},
	}

	result := newRewriterWithRules([]rewriterules.Rule{rule}, 1).rewrite(newTestBoolValueExpr(false))

	require.False(t, result.GetValueExpr().GetValue().GetBoolVal())
	require.Zero(t, applyCount)
}

func TestRewriterIgnoresNilReplacement(t *testing.T) {
	rules := []rewriterules.Rule{
		testRewriteRule{
			rewrite: func(*planpb.Expr) (*planpb.Expr, bool) {
				return nil, true
			},
		},
		testRewriteRule{
			rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
				value := expr.GetValueExpr().GetValue()
				if _, ok := value.GetVal().(*planpb.GenericValue_BoolVal); !ok {
					return expr, false
				}
				return newTestStringValueExpr("rewritten"), true
			},
		},
	}
	rewriter := newRewriterWithRules(rules, defaultMaxRuleApplicationsPerInputNode)

	result := rewriter.rewrite(newTestBoolValueExpr(false))

	require.Equal(t, "rewritten", result.GetValueExpr().GetValue().GetStringVal())
	require.False(t, rewriter.hitApplicationLimit)
}

func TestRewriterRestartsOrderedRulesAfterChange(t *testing.T) {
	rules := []rewriterules.Rule{
		testRewriteRule{
			rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
				value := expr.GetValueExpr().GetValue()
				if _, ok := value.GetVal().(*planpb.GenericValue_BoolVal); !ok || !value.GetBoolVal() {
					return expr, false
				}
				return newTestStringValueExpr("done"), true
			},
		},
		testRewriteRule{
			rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
				value := expr.GetValueExpr().GetValue()
				if _, ok := value.GetVal().(*planpb.GenericValue_BoolVal); !ok || value.GetBoolVal() {
					return expr, false
				}
				return newTestBoolValueExpr(true), true
			},
		},
	}
	rewriter := newRewriterWithRules(rules, defaultMaxRuleApplicationsPerInputNode)

	result := rewriter.rewrite(newTestBoolValueExpr(false))

	require.Equal(t, "done", result.GetValueExpr().GetValue().GetStringVal())
	require.False(t, rewriter.hitApplicationLimit)
}

func TestRewriterRevisitsChildrenAfterParentChange(t *testing.T) {
	left := newTestBoolValueExpr(false)
	right := newTestBoolValueExpr(true)
	visits := map[*planpb.Expr]int{}
	rule := testRewriteRule{
		rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
			if binary := expr.GetBinaryExpr(); binary != nil && binary.GetOp() == planpb.BinaryExpr_LogicalAnd {
				return newTestLogicalExpr(planpb.BinaryExpr_LogicalOr, binary.GetLeft(), binary.GetRight()), true
			}
			visits[expr]++
			return expr, false
		},
	}
	rewriter := newRewriterWithRules([]rewriterules.Rule{rule}, defaultMaxRuleApplicationsPerInputNode)

	result := rewriter.rewrite(newTestLogicalExpr(planpb.BinaryExpr_LogicalAnd, left, right))

	require.Equal(t, planpb.BinaryExpr_LogicalOr, result.GetBinaryExpr().GetOp())
	require.Equal(t, 2, visits[left])
	require.Equal(t, 2, visits[right])
}

func TestRewriterStabilizesChildBeforeParent(t *testing.T) {
	parentSawIntermediateChild := false
	parentApplications := 0
	rules := []rewriterules.Rule{
		testRewriteRule{
			rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
				unary := expr.GetUnaryExpr()
				if unary == nil {
					return expr, false
				}
				parentApplications++
				childValue := unary.GetChild().GetValueExpr().GetValue()
				if _, ok := childValue.GetVal().(*planpb.GenericValue_StringVal); !ok || childValue.GetStringVal() != "child" {
					parentSawIntermediateChild = true
					return expr, false
				}
				return newTestStringValueExpr("parent"), true
			},
		},
		testRewriteRule{
			rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
				value := expr.GetValueExpr().GetValue()
				if _, ok := value.GetVal().(*planpb.GenericValue_BoolVal); !ok || value.GetBoolVal() {
					return expr, false
				}
				return newTestBoolValueExpr(true), true
			},
		},
		testRewriteRule{
			rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
				value := expr.GetValueExpr().GetValue()
				if _, ok := value.GetVal().(*planpb.GenericValue_BoolVal); !ok || !value.GetBoolVal() {
					return expr, false
				}
				return newTestStringValueExpr("child"), true
			},
		},
	}
	rewriter := newRewriterWithRules(rules, defaultMaxRuleApplicationsPerInputNode)

	result := rewriter.rewrite(newTestNotExpr(newTestBoolValueExpr(false)))

	require.Equal(t, "parent", result.GetValueExpr().GetValue().GetStringVal())
	require.False(t, parentSawIntermediateChild)
	require.Equal(t, 1, parentApplications)
}

func TestRewriterStopsNonConvergingRule(t *testing.T) {
	const maxSteps = 3
	applyCount := 0
	rule := testRewriteRule{
		rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
			valueExpr := expr.GetValueExpr()
			if valueExpr == nil {
				return expr, false
			}
			applyCount++
			return newTestBoolValueExpr(!valueExpr.GetValue().GetBoolVal()), true
		},
	}
	rewriter := newRewriterWithRules([]rewriterules.Rule{rule}, maxSteps)

	result := rewriter.rewrite(newTestBoolValueExpr(false))

	require.True(t, rewriter.hitApplicationLimit)
	require.Equal(t, maxSteps, applyCount)
	require.True(t, result.GetValueExpr().GetValue().GetBoolVal())
}

func TestRewriterStopsExpandingRule(t *testing.T) {
	const maxSteps = 3
	const safetyLimit = 100
	applyCount := 0
	rule := testRewriteRule{
		rewrite: func(expr *planpb.Expr) (*planpb.Expr, bool) {
			if expr.GetValueExpr() == nil || applyCount >= safetyLimit {
				return expr, false
			}
			applyCount++
			return newTestNotExpr(expr), true
		},
	}
	rewriter := newRewriterWithRules([]rewriterules.Rule{rule}, maxSteps)

	result := rewriter.rewrite(newTestBoolValueExpr(false))

	require.True(t, rewriter.hitApplicationLimit)
	require.Equal(t, maxSteps, applyCount)
	require.NotNil(t, result)
}

func newTestBoolValueExpr(value bool) *planpb.Expr {
	return newTestValueExpr(newTestBoolValue(value))
}

func newTestBoolValue(value bool) *planpb.GenericValue {
	return &planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: value}}
}

func newTestStringValueExpr(value string) *planpb.Expr {
	return newTestValueExpr(&planpb.GenericValue{
		Val: &planpb.GenericValue_StringVal{StringVal: value},
	})
}

func newTestLogicalExpr(op planpb.BinaryExpr_BinaryOp, left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op:    op,
				Left:  left,
				Right: right,
			},
		},
	}
}

func newTestNotExpr(child *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Op:    planpb.UnaryExpr_Not,
				Child: child,
			},
		},
	}
}

func newTestValueExpr(value *planpb.GenericValue) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_ValueExpr{
			ValueExpr: &planpb.ValueExpr{
				Value: value,
			},
		},
	}
}
