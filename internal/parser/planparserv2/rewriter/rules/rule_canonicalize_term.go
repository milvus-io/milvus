package rules

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

type canonicalizeTermRule struct{}

func (canonicalizeTermRule) Match(expr *planpb.Expr) bool {
	term := expr.GetTermExpr()
	if term == nil {
		return false
	}
	if len(term.GetValues()) == 1 {
		return true
	}
	return effectiveDataType(term.GetColumnInfo()) == schemapb.DataType_Bool &&
		allBoolVals(term.GetValues()) &&
		boolValuesCoverDomain(term.GetValues()) &&
		canFoldPredicateToBoolConstant(term.GetColumnInfo())
}

func (canonicalizeTermRule) Apply(expr *planpb.Expr) (*planpb.Expr, bool) {
	term := expr.GetTermExpr()

	if effectiveDataType(term.GetColumnInfo()) == schemapb.DataType_Bool {
		values := term.GetValues()
		if allBoolVals(values) {
			if boolValuesCoverDomain(values) {
				if !canFoldPredicateToBoolConstant(term.GetColumnInfo()) {
					return expr, false
				}
				return newAlwaysTrueExpr(), true
			}
			if len(values) == 1 {
				return newUnaryRangeExpr(term.GetColumnInfo(), planpb.OpType_Equal, values[0]), true
			}
		}
	}

	if len(term.GetValues()) == 1 {
		return newUnaryRangeExpr(term.GetColumnInfo(), planpb.OpType_Equal, term.GetValues()[0]), true
	}

	return expr, false
}

func boolValuesCoverDomain(values []*planpb.GenericValue) bool {
	hasFalse, hasTrue := false, false
	for _, value := range values {
		if value.GetBoolVal() {
			hasTrue = true
		} else {
			hasFalse = true
		}
	}
	return hasTrue && hasFalse
}

func allBoolVals(values []*planpb.GenericValue) bool {
	if len(values) == 0 {
		return false
	}
	for _, value := range values {
		if _, ok := value.GetVal().(*planpb.GenericValue_BoolVal); !ok {
			return false
		}
	}
	return true
}
