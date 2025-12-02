package exprutil

import (
	"math"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type KeyType int64

const (
	PartitionKey  KeyType = iota
	ClusteringKey KeyType = PartitionKey + 1
)

func ParseExprFromPlan(plan *planpb.PlanNode) (*planpb.Expr, error) {
	node := plan.GetNode()

	if node == nil {
		return nil, errors.New("can't get expr from empty plan node")
	}

	var expr *planpb.Expr
	switch node := node.(type) {
	case *planpb.PlanNode_VectorAnns:
		expr = node.VectorAnns.GetPredicates()
	case *planpb.PlanNode_Query:
		expr = node.Query.GetPredicates()
	default:
		return nil, errors.New("unsupported plan node type")
	}

	return expr, nil
}

// ParsePartitionKeysFromBinaryExpr parses BinaryExpr is prunble
// if true, returns candidate key values base on the Logical op type.
func ParsePartitionKeysFromBinaryExpr(expr *planpb.BinaryExpr, keyType KeyType) ([]*planpb.GenericValue, bool) {
	lCandidates, lPrunable := ParseKeysFromExpr(expr.Left, keyType)
	rCandidate, rPrunable := ParseKeysFromExpr(expr.Right, keyType)

	if expr.Op == planpb.BinaryExpr_LogicalAnd {
		switch {
		case lPrunable && rPrunable:
			// case: partition_key in [7, 8] && partition_key in [8, 9]
			// return [7, 8] intersect [8, 9] = [8]
			return IntersectKeys(lCandidates, rCandidate), true
		case lPrunable && !rPrunable:
			return lCandidates, true
		case !lPrunable && rPrunable:
			return rCandidate, true
		case !lPrunable && !rPrunable:
			return nil, false
		}
	}

	if expr.Op == planpb.BinaryExpr_LogicalOr {
		if lPrunable && rPrunable {
			// case: partition_key in [7, 8] || partition_key in [8, 9]
			// return [7, 8] union [8, 9] = [7, 8, 9]
			return append(lCandidates, rCandidate...), true
		}
		return nil, false
	}

	return nil, false
}

// ParsePartitionKeysFromUnaryExpr parses UnaryExpr is prunble.
// currently, only "Not" is supported, which means unary expression is always not prunable.
func ParsePartitionKeysFromUnaryExpr(expr *planpb.UnaryExpr, keyType KeyType) ([]*planpb.GenericValue, bool) {
	return nil, false
}

// ParsePartitionKeysFromTermExpr parses TermExpr is prunble.
// it checks if the term expression is a partition key or clustering key.
func ParsePartitionKeysFromTermExpr(expr *planpb.TermExpr, keyType KeyType) ([]*planpb.GenericValue, bool) {
	if keyType == PartitionKey && expr.GetColumnInfo().GetIsPartitionKey() {
		return expr.GetValues(), true
	} else if keyType == ClusteringKey && expr.GetColumnInfo().GetIsClusteringKey() {
		return expr.GetValues(), true
	}
	return nil, false
}

// ParsePartitionKeysFromUnaryRangeExpr parses UnaryRangeExpr is prunble.
func ParsePartitionKeysFromUnaryRangeExpr(expr *planpb.UnaryRangeExpr, keyType KeyType) (candidate []*planpb.GenericValue, prunable bool) {
	if expr.GetOp() == planpb.OpType_Equal {
		if expr.GetColumnInfo().GetIsPartitionKey() && keyType == PartitionKey ||
			expr.GetColumnInfo().GetIsClusteringKey() && keyType == ClusteringKey {
			return []*planpb.GenericValue{expr.Value}, true
		}
	}
	return nil, false
}

// ParseKeysFromExpr parses keys from the given expression based on the key type.
// If the expression can limit the search scope to specified partitions, return the corresponding key values and a flag indicating whether pruning is possible.
// otherwise, return nil and false indicating that pruning is not possible base on this expression.
func ParseKeysFromExpr(expr *planpb.Expr, keyType KeyType) (candidates []*planpb.GenericValue, prunable bool) {
	switch expr := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		candidates, prunable = ParsePartitionKeysFromBinaryExpr(expr.BinaryExpr, keyType)
	case *planpb.Expr_UnaryExpr:
		candidates, prunable = ParsePartitionKeysFromUnaryExpr(expr.UnaryExpr, keyType)
	case *planpb.Expr_TermExpr:
		candidates, prunable = ParsePartitionKeysFromTermExpr(expr.TermExpr, keyType)
	case *planpb.Expr_UnaryRangeExpr:
		candidates, prunable = ParsePartitionKeysFromUnaryRangeExpr(expr.UnaryRangeExpr, keyType)
	}

	return candidates, prunable
}

func IntersectKeys(l []*planpb.GenericValue, r []*planpb.GenericValue) []*planpb.GenericValue {
	if len(l) == 0 || len(r) == 0 {
		return nil
	}
	// all elements shall be in same type
	switch l[0].Val.(type) {
	case *planpb.GenericValue_Int64Val:
		lSet := typeutil.NewSet(lo.Map(l, func(e *planpb.GenericValue, _ int) int64 { return e.GetInt64Val() })...)
		rSet := typeutil.NewSet(lo.Map(r, func(e *planpb.GenericValue, _ int) int64 { return e.GetInt64Val() })...)
		return lo.Map(lSet.Intersection(rSet).Collect(), func(e int64, _ int) *planpb.GenericValue {
			return &planpb.GenericValue{
				Val: &planpb.GenericValue_Int64Val{
					Int64Val: e,
				},
			}
		})
	case *planpb.GenericValue_StringVal:
		lSet := typeutil.NewSet(lo.Map(l, func(e *planpb.GenericValue, _ int) string { return e.GetStringVal() })...)
		rSet := typeutil.NewSet(lo.Map(r, func(e *planpb.GenericValue, _ int) string { return e.GetStringVal() })...)
		return lo.Map(lSet.Intersection(rSet).Collect(), func(e string, _ int) *planpb.GenericValue {
			return &planpb.GenericValue{
				Val: &planpb.GenericValue_StringVal{
					StringVal: e,
				},
			}
		})
	}
	return nil
}

func ParseKeys(expr *planpb.Expr, kType KeyType) []*planpb.GenericValue {
	res, prunable := ParseKeysFromExpr(expr, kType)
	if !prunable {
		res = nil
	}
	// TODO return empty result if prunable and candidates lens is 0

	return res
}

type PlanRange struct {
	lower        *planpb.GenericValue
	upper        *planpb.GenericValue
	includeLower bool
	includeUpper bool
}

func (planRange *PlanRange) ToIntRange() *IntRange {
	iRange := &IntRange{}
	if planRange.lower == nil {
		iRange.lower = math.MinInt64
		iRange.includeLower = false
	} else {
		iRange.lower = planRange.lower.GetInt64Val()
		iRange.includeLower = planRange.includeLower
	}

	if planRange.upper == nil {
		iRange.upper = math.MaxInt64
		iRange.includeUpper = false
	} else {
		iRange.upper = planRange.upper.GetInt64Val()
		iRange.includeUpper = planRange.includeUpper
	}
	return iRange
}

func (planRange *PlanRange) ToStrRange() *StrRange {
	sRange := &StrRange{}
	if planRange.lower == nil {
		sRange.lower = ""
		sRange.includeLower = false
	} else {
		sRange.lower = planRange.lower.GetStringVal()
		sRange.includeLower = planRange.includeLower
	}

	if planRange.upper == nil {
		sRange.upper = ""
		sRange.includeUpper = false
	} else {
		sRange.upper = planRange.upper.GetStringVal()
		sRange.includeUpper = planRange.includeUpper
	}
	return sRange
}

type IntRange struct {
	lower        int64
	upper        int64
	includeLower bool
	includeUpper bool
}

func NewIntRange(l int64, r int64, includeL bool, includeR bool) *IntRange {
	return &IntRange{
		lower:        l,
		upper:        r,
		includeLower: includeL,
		includeUpper: includeR,
	}
}

func IntRangeOverlap(range1 *IntRange, range2 *IntRange) bool {
	var leftBound int64
	if range1.lower < range2.lower {
		leftBound = range2.lower
	} else {
		leftBound = range1.lower
	}
	var rightBound int64
	if range1.upper < range2.upper {
		rightBound = range1.upper
	} else {
		rightBound = range2.upper
	}
	return leftBound <= rightBound
}

type StrRange struct {
	lower        string
	upper        string
	includeLower bool
	includeUpper bool
}

func NewStrRange(l string, r string, includeL bool, includeR bool) *StrRange {
	return &StrRange{
		lower:        l,
		upper:        r,
		includeLower: includeL,
		includeUpper: includeR,
	}
}

func StrRangeOverlap(range1 *StrRange, range2 *StrRange) bool {
	var leftBound string
	if range1.lower < range2.lower {
		leftBound = range2.lower
	} else {
		leftBound = range1.lower
	}
	var rightBound string
	if range1.upper < range2.upper || range2.upper == "" {
		rightBound = range1.upper
	} else {
		rightBound = range2.upper
	}
	return leftBound <= rightBound
}

func GetCommonDataType(a *PlanRange, b *PlanRange) schemapb.DataType {
	var bound *planpb.GenericValue
	if a.lower != nil {
		bound = a.lower
	} else if a.upper != nil {
		bound = a.upper
	}
	if bound == nil {
		if b.lower != nil {
			bound = b.lower
		} else if b.upper != nil {
			bound = b.upper
		}
	}
	if bound == nil {
		return schemapb.DataType_None
	}
	switch bound.Val.(type) {
	case *planpb.GenericValue_Int64Val:
		{
			return schemapb.DataType_Int64
		}
	case *planpb.GenericValue_StringVal:
		{
			return schemapb.DataType_VarChar
		}
	}
	return schemapb.DataType_None
}

func ValidatePartitionKeyIsolation(expr *planpb.Expr) error {
	foundPartitionKey, err := validatePartitionKeyIsolationFromExpr(expr)
	if err != nil {
		return err
	}
	if !foundPartitionKey {
		return errors.New("partition key not found in expr or the expr is invalid when validating partition key isolation")
	}
	return nil
}

func validatePartitionKeyIsolationFromExpr(expr *planpb.Expr) (bool, error) {
	switch expr := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		return validatePartitionKeyIsolationFromBinaryExpr(expr.BinaryExpr)
	case *planpb.Expr_UnaryExpr:
		return validatePartitionKeyIsolationFromUnaryExpr(expr.UnaryExpr)
	case *planpb.Expr_TermExpr:
		return validatePartitionKeyIsolationFromTermExpr(expr.TermExpr)
	case *planpb.Expr_UnaryRangeExpr:
		return validatePartitionKeyIsolationFromRangeExpr(expr.UnaryRangeExpr)
	case *planpb.Expr_BinaryRangeExpr:
		return validatePartitionKeyIsolationFromBinaryRangeExpr(expr.BinaryRangeExpr)
	}
	return false, nil
}

func validatePartitionKeyIsolationFromBinaryExpr(expr *planpb.BinaryExpr) (bool, error) {
	// return directly if has errors on either or both sides
	leftRes, leftErr := validatePartitionKeyIsolationFromExpr(expr.Left)
	if leftErr != nil {
		return leftRes, leftErr
	}
	rightRes, rightErr := validatePartitionKeyIsolationFromExpr(expr.Right)
	if rightErr != nil {
		return rightRes, rightErr
	}

	// the following deals with no error on either side
	if expr.Op == planpb.BinaryExpr_LogicalAnd {
		// if one of them is partition key
		// e.g. partition_key_field == 1 && other_field > 10
		if leftRes || rightRes {
			return true, nil
		}
		// if none of them is partition key
		return false, nil
	}

	if expr.Op == planpb.BinaryExpr_LogicalOr {
		// if either side has partition key, but OR them
		// e.g. partition_key_field == 1 || other_field > 10
		if leftRes || rightRes {
			return true, errors.New("partition key isolation does not support OR")
		}
		// if none of them has partition key
		return false, nil
	}
	return false, nil
}

func validatePartitionKeyIsolationFromUnaryExpr(expr *planpb.UnaryExpr) (bool, error) {
	res, err := validatePartitionKeyIsolationFromExpr(expr.GetChild())
	if err != nil {
		return res, err
	}
	if expr.Op == planpb.UnaryExpr_Not {
		if res {
			return true, errors.New("partition key isolation does not support NOT")
		}
		return false, nil
	}
	return res, err
}

func validatePartitionKeyIsolationFromTermExpr(expr *planpb.TermExpr) (bool, error) {
	if expr.GetColumnInfo().GetIsPartitionKey() {
		// e.g. partition_key_field in [1, 2, 3]
		return true, errors.New("partition key isolation does not support IN")
	}
	return false, nil
}

func validatePartitionKeyIsolationFromRangeExpr(expr *planpb.UnaryRangeExpr) (bool, error) {
	if expr.GetColumnInfo().GetIsPartitionKey() {
		if expr.GetOp() == planpb.OpType_Equal {
			// e.g. partition_key_field == 1
			return true, nil
		}
		return true, errors.Newf("partition key isolation does not support %s", expr.GetOp().String())
	}
	return false, nil
}

func validatePartitionKeyIsolationFromBinaryRangeExpr(expr *planpb.BinaryRangeExpr) (bool, error) {
	if expr.GetColumnInfo().GetIsPartitionKey() {
		return true, errors.New("partition key isolation does not support BinaryRange")
	}
	return false, nil
}
