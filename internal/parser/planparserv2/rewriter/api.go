package rewriter

import (
	rewriterules "github.com/milvus-io/milvus/internal/parser/planparserv2/rewriter/rules"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// CompareRangeValues compares two supported range literals exactly.
func CompareRangeValues(a, b *planpb.GenericValue) (int, bool) {
	return rewriterules.CompareRangeValues(a, b)
}

// IsAlwaysTrueExpr reports whether expr is the canonical true expression.
func IsAlwaysTrueExpr(expr *planpb.Expr) bool {
	return rewriterules.IsAlwaysTrueExpr(expr)
}

// IsAlwaysFalseExpr reports whether expr is the canonical false expression.
func IsAlwaysFalseExpr(expr *planpb.Expr) bool {
	return rewriterules.IsAlwaysFalseExpr(expr)
}
