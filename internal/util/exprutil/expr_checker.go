package exprutil

import (
	"math"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
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

func ParsePartitionKeysFromBinaryExpr(expr *planpb.BinaryExpr, keyType KeyType) ([]*planpb.GenericValue, bool) {
	leftRes, leftInRange := ParseKeysFromExpr(expr.Left, keyType)
	rightRes, rightInRange := ParseKeysFromExpr(expr.Right, keyType)

	if expr.Op == planpb.BinaryExpr_LogicalAnd {
		// case: partition_key_field in [7, 8] && partition_key > 8
		if len(leftRes)+len(rightRes) > 0 {
			leftRes = append(leftRes, rightRes...)
			return leftRes, false
		}

		// case: other_field > 10 && partition_key_field > 8
		return nil, leftInRange || rightInRange
	}

	if expr.Op == planpb.BinaryExpr_LogicalOr {
		// case: partition_key_field in [7, 8] or partition_key > 8
		if leftInRange || rightInRange {
			return nil, true
		}

		// case: partition_key_field in [7, 8] or other_field > 10
		leftRes = append(leftRes, rightRes...)
		return leftRes, false
	}

	return nil, false
}

func ParsePartitionKeysFromUnaryExpr(expr *planpb.UnaryExpr, keyType KeyType) ([]*planpb.GenericValue, bool) {
	res, partitionInRange := ParseKeysFromExpr(expr.GetChild(), keyType)
	if expr.Op == planpb.UnaryExpr_Not {
		// case: partition_key_field not in [7, 8]
		if len(res) != 0 {
			return nil, true
		}

		// case: other_field not in [10]
		return nil, partitionInRange
	}

	// UnaryOp only includes "Not" for now
	return res, partitionInRange
}

func ParsePartitionKeysFromTermExpr(expr *planpb.TermExpr, keyType KeyType) ([]*planpb.GenericValue, bool) {
	if keyType == PartitionKey && expr.GetColumnInfo().GetIsPartitionKey() {
		return expr.GetValues(), false
	} else if keyType == ClusteringKey && expr.GetColumnInfo().GetIsClusteringKey() {
		return expr.GetValues(), false
	}
	return nil, false
}

func ParsePartitionKeysFromUnaryRangeExpr(expr *planpb.UnaryRangeExpr, keyType KeyType) ([]*planpb.GenericValue, bool) {
	if expr.GetOp() == planpb.OpType_Equal {
		if expr.GetColumnInfo().GetIsPartitionKey() && keyType == PartitionKey ||
			expr.GetColumnInfo().GetIsClusteringKey() && keyType == ClusteringKey {
			return []*planpb.GenericValue{expr.Value}, false
		}
	}
	return nil, true
}

func ParseKeysFromExpr(expr *planpb.Expr, keyType KeyType) ([]*planpb.GenericValue, bool) {
	var res []*planpb.GenericValue
	keyInRange := false
	switch expr := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		res, keyInRange = ParsePartitionKeysFromBinaryExpr(expr.BinaryExpr, keyType)
	case *planpb.Expr_UnaryExpr:
		res, keyInRange = ParsePartitionKeysFromUnaryExpr(expr.UnaryExpr, keyType)
	case *planpb.Expr_TermExpr:
		res, keyInRange = ParsePartitionKeysFromTermExpr(expr.TermExpr, keyType)
	case *planpb.Expr_UnaryRangeExpr:
		res, keyInRange = ParsePartitionKeysFromUnaryRangeExpr(expr.UnaryRangeExpr, keyType)
	}

	return res, keyInRange
}

func ParseKeys(expr *planpb.Expr, kType KeyType) []*planpb.GenericValue {
	res, keyInRange := ParseKeysFromExpr(expr, kType)
	if keyInRange {
		res = nil
	}

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

/*
principles for range parsing
1. no handling unary expr like 'NOT'
2. no handling 'or' expr, no matter on clusteringKey or not, just terminate all possible prune
3. for any unlogical 'and' expr, we check and terminate upper away
4. no handling Term and Range at the same time
*/

func ParseRanges(expr *planpb.Expr, kType KeyType) ([]*PlanRange, bool) {
	var res []*PlanRange
	matchALL := true
	switch expr := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		res, matchALL = ParseRangesFromBinaryExpr(expr.BinaryExpr, kType)
	case *planpb.Expr_UnaryRangeExpr:
		res, matchALL = ParseRangesFromUnaryRangeExpr(expr.UnaryRangeExpr, kType)
	case *planpb.Expr_TermExpr:
		res, matchALL = ParseRangesFromTermExpr(expr.TermExpr, kType)
	case *planpb.Expr_UnaryExpr:
		res, matchALL = nil, true
		// we don't handle NOT operation, just consider as unable_to_parse_range
	}
	return res, matchALL
}

func ParseRangesFromBinaryExpr(expr *planpb.BinaryExpr, kType KeyType) ([]*PlanRange, bool) {
	if expr.Op == planpb.BinaryExpr_LogicalOr {
		return nil, true
	}
	_, leftIsTerm := expr.GetLeft().GetExpr().(*planpb.Expr_TermExpr)
	_, rightIsTerm := expr.GetRight().GetExpr().(*planpb.Expr_TermExpr)
	if leftIsTerm || rightIsTerm {
		// either of lower or upper is term query like x IN [1,2,3]
		// we will terminate the prune process
		return nil, true
	}
	leftRanges, leftALL := ParseRanges(expr.Left, kType)
	rightRanges, rightALL := ParseRanges(expr.Right, kType)
	if leftALL && rightALL {
		return nil, true
	} else if leftALL && !rightALL {
		return rightRanges, rightALL
	} else if rightALL && !leftALL {
		return leftRanges, leftALL
	}
	// only unary ranges or further binary ranges are lower
	// calculate the intersection and return the resulting ranges
	// it's expected that only single range can be returned from lower and upper child
	if len(leftRanges) != 1 || len(rightRanges) != 1 {
		return nil, true
	}
	intersected := Intersect(leftRanges[0], rightRanges[0])
	matchALL := intersected == nil
	return []*PlanRange{intersected}, matchALL
}

func ParseRangesFromUnaryRangeExpr(expr *planpb.UnaryRangeExpr, kType KeyType) ([]*PlanRange, bool) {
	if expr.GetColumnInfo().GetIsPartitionKey() && kType == PartitionKey ||
		expr.GetColumnInfo().GetIsClusteringKey() && kType == ClusteringKey {
		switch expr.GetOp() {
		case planpb.OpType_Equal:
			{
				return []*PlanRange{
					{
						lower:        expr.Value,
						upper:        expr.Value,
						includeLower: true,
						includeUpper: true,
					},
				}, false
			}
		case planpb.OpType_GreaterThan:
			{
				return []*PlanRange{
					{
						lower:        expr.Value,
						upper:        nil,
						includeLower: false,
						includeUpper: false,
					},
				}, false
			}
		case planpb.OpType_GreaterEqual:
			{
				return []*PlanRange{
					{
						lower:        expr.Value,
						upper:        nil,
						includeLower: true,
						includeUpper: false,
					},
				}, false
			}
		case planpb.OpType_LessThan:
			{
				return []*PlanRange{
					{
						lower:        nil,
						upper:        expr.Value,
						includeLower: false,
						includeUpper: false,
					},
				}, false
			}
		case planpb.OpType_LessEqual:
			{
				return []*PlanRange{
					{
						lower:        nil,
						upper:        expr.Value,
						includeLower: false,
						includeUpper: true,
					},
				}, false
			}
		}
	}
	return nil, true
}

func ParseRangesFromTermExpr(expr *planpb.TermExpr, kType KeyType) ([]*PlanRange, bool) {
	if expr.GetColumnInfo().GetIsPartitionKey() && kType == PartitionKey ||
		expr.GetColumnInfo().GetIsClusteringKey() && kType == ClusteringKey {
		res := make([]*PlanRange, 0)
		for _, value := range expr.GetValues() {
			res = append(res, &PlanRange{
				lower:        value,
				upper:        value,
				includeLower: true,
				includeUpper: true,
			})
		}
		return res, false
	}
	return nil, true
}

var minusInfiniteInt = &planpb.GenericValue{
	Val: &planpb.GenericValue_Int64Val{
		Int64Val: math.MinInt64,
	},
}

var positiveInfiniteInt = &planpb.GenericValue{
	Val: &planpb.GenericValue_Int64Val{
		Int64Val: math.MaxInt64,
	},
}

var minStrVal = &planpb.GenericValue{
	Val: &planpb.GenericValue_StringVal{
		StringVal: "",
	},
}

var maxStrVal = &planpb.GenericValue{}

func complementPlanRange(pr *PlanRange, dataType schemapb.DataType) *PlanRange {
	if dataType == schemapb.DataType_Int64 {
		if pr.lower == nil {
			pr.lower = minusInfiniteInt
		}
		if pr.upper == nil {
			pr.upper = positiveInfiniteInt
		}
	} else {
		if pr.lower == nil {
			pr.lower = minStrVal
		}
		if pr.upper == nil {
			pr.upper = maxStrVal
		}
	}

	return pr
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

func Intersect(a *PlanRange, b *PlanRange) *PlanRange {
	dataType := GetCommonDataType(a, b)
	complementPlanRange(a, dataType)
	complementPlanRange(b, dataType)

	// Check if 'a' and 'b' non-overlapping at all
	rightBound := minGenericValue(a.upper, b.upper)
	leftBound := maxGenericValue(a.lower, b.lower)
	if compareGenericValue(leftBound, rightBound) > 0 {
		return nil
	}

	// Check if 'a' range ends exactly where 'b' range starts
	if !a.includeUpper && !b.includeLower && (compareGenericValue(a.upper, b.lower) == 0) {
		return nil
	}
	// Check if 'b' range ends exactly where 'a' range starts
	if !b.includeUpper && !a.includeLower && (compareGenericValue(b.upper, a.lower) == 0) {
		return nil
	}

	return &PlanRange{
		lower:        leftBound,
		upper:        rightBound,
		includeLower: a.includeLower || b.includeLower,
		includeUpper: a.includeUpper || b.includeUpper,
	}
}

func compareGenericValue(left *planpb.GenericValue, right *planpb.GenericValue) int64 {
	if right == nil || left == nil {
		return -1
	}
	switch left.Val.(type) {
	case *planpb.GenericValue_Int64Val:
		if left.GetInt64Val() == right.GetInt64Val() {
			return 0
		} else if left.GetInt64Val() < right.GetInt64Val() {
			return -1
		} else {
			return 1
		}
	case *planpb.GenericValue_StringVal:
		if right.Val == nil {
			return -1
		}
		return int64(strings.Compare(left.GetStringVal(), right.GetStringVal()))
	}
	return 0
}

func minGenericValue(left *planpb.GenericValue, right *planpb.GenericValue) *planpb.GenericValue {
	if compareGenericValue(left, right) < 0 {
		return left
	}
	return right
}

func maxGenericValue(left *planpb.GenericValue, right *planpb.GenericValue) *planpb.GenericValue {
	if compareGenericValue(left, right) >= 0 {
		return left
	}
	return right
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
