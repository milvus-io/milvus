package rewriter

import (
	rewriterules "github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter/rules"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func RewriteExpr(e *planpb.Expr) *planpb.Expr {
	optimizeEnabled := paramtable.Get().CommonCfg.EnabledOptimizeExpr.GetAsBool()
	return RewriteExprWithConfig(e, optimizeEnabled)
}

func RewriteExprWithConfig(e *planpb.Expr, optimizeEnabled bool) *planpb.Expr {
	if e == nil {
		return nil
	}
	e = rewriterules.NormalizeTermExprs(e)
	e = rewriterules.NormalizeEmptyArrayComparisons(e)
	return newRewriter(optimizeEnabled).rewrite(e)
}
