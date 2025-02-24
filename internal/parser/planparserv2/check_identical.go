package planparserv2

import (
	"reflect"

	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

// CheckPredicatesIdentical check if two exprs are identical.
func CheckPredicatesIdentical(expr, other *planpb.Expr) bool {
	if expr == nil || other == nil {
		return expr == other
	}

	v := NewShowExprVisitor()
	js1 := v.VisitExpr(expr)
	js2 := v.VisitExpr(other)
	return reflect.DeepEqual(js1, js2)
}

func CheckQueryInfoIdentical(info1, info2 *planpb.QueryInfo) bool {
	if info1.GetTopk() != info2.GetTopk() {
		return false
	}
	if info1.GetMetricType() != info2.GetMetricType() {
		return false
	}
	if info1.GetSearchParams() != info2.GetSearchParams() {
		// TODO: check json identical.
		return false
	}
	if info1.GetRoundDecimal() != info2.GetRoundDecimal() {
		return false
	}
	return true
}

func CheckVectorANNSIdentical(node1, node2 *planpb.VectorANNS) bool {
	if node1.GetVectorType() != node2.GetVectorType() {
		return false
	}
	if node1.GetFieldId() != node2.GetFieldId() {
		return false
	}
	if node1.GetPlaceholderTag() != node2.GetPlaceholderTag() {
		return false
	}
	if !CheckQueryInfoIdentical(node1.GetQueryInfo(), node2.GetQueryInfo()) {
		return false
	}
	return CheckPredicatesIdentical(node1.GetPredicates(), node2.GetPredicates())
}

func CheckPlanNodeIdentical(node1, node2 *planpb.PlanNode) bool {
	switch node1.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		switch node2.GetNode().(type) {
		case *planpb.PlanNode_VectorAnns:
			if !funcutil.SliceSetEqual(node1.GetOutputFieldIds(), node2.GetOutputFieldIds()) {
				return false
			}
			return CheckVectorANNSIdentical(node1.GetVectorAnns(), node2.GetVectorAnns())
		default:
			return false
		}

	case *planpb.PlanNode_Predicates:
		switch node2.GetNode().(type) {
		case *planpb.PlanNode_Predicates:
			if !funcutil.SliceSetEqual(node1.GetOutputFieldIds(), node2.GetOutputFieldIds()) {
				return false
			}
			return CheckPredicatesIdentical(node1.GetPredicates(), node2.GetPredicates())
		default:
			return false
		}
	}
	return false
}
