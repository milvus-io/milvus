package delegator

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/storage"
)

type Expr interface {
	Inputs() []Expr
	Eval(segmentStats []*storage.SegmentStats, bitset **bitset.BitSet)
	Size() uint
}

func PruneByScalarField(expr Expr, segmentStats []*storage.SegmentStats, segmentIDs []UniqueID, filteredSegments map[UniqueID]struct{}) {
	var bst **bitset.BitSet
	expr.Eval(segmentStats, bst)
	(*bst).FlipRange(0, (*bst).Len())
	for i, e := (*bst).NextSet(0); e; i, e = (*bst).NextSet(i + 1) {
		filteredSegments[segmentIDs[i]] = struct{}{}
	}
}

type ExprImpl struct {
	Expr
	size uint
}

func (exprImpl *ExprImpl) Size() uint {
	return exprImpl.size
}

func NewExprImpl(size uint) *ExprImpl {
	return &ExprImpl{size: size}
}

type LogicalBinaryExpr struct {
	ExprImpl
	left  Expr
	right Expr
	op    planpb.BinaryExpr_BinaryOp
}

func NewLogicalBinaryExpr(sz uint, l Expr, r Expr, op planpb.BinaryExpr_BinaryOp) *LogicalBinaryExpr {
	return &LogicalBinaryExpr{ExprImpl: ExprImpl{size: sz}, left: l, right: r, op: op}
}

func (lbe *LogicalBinaryExpr) Eval(segmentStats []*storage.SegmentStats, bst **bitset.BitSet) {
	leftExpr := lbe.Inputs()[0]
	var leftBitSet **bitset.BitSet
	leftExpr.Eval(segmentStats, leftBitSet)

	rightExpr := lbe.Inputs()[1]
	var rightBitSet **bitset.BitSet
	rightExpr.Eval(segmentStats, rightBitSet)

	if lbe.op == planpb.BinaryExpr_LogicalAnd {
		(*leftBitSet).InPlaceIntersection(*rightBitSet)
	} else if lbe.op == planpb.BinaryExpr_LogicalOr {
		(*leftBitSet).InPlaceUnion(*rightBitSet)
	}
	*bst = *leftBitSet
}

func (lbe *LogicalBinaryExpr) Inputs() []Expr {
	return []Expr{lbe.left, lbe.right}
}

type LogicalUnaryExpr struct {
	ExprImpl
	inner Expr
}

func NewLogicalUnaryExpr(sz uint, in Expr) *LogicalUnaryExpr {
	return &LogicalUnaryExpr{ExprImpl: ExprImpl{size: sz}, inner: in}
}

func (lue *LogicalUnaryExpr) Eval(segmentStats []*storage.SegmentStats, bst **bitset.BitSet) {
	(lue.Inputs()[0]).Eval(segmentStats, bst)
	(*bst).FlipRange(0, lue.Size())
}

func (lue *LogicalUnaryExpr) Inputs() []Expr {
	return []Expr{lue.inner}
}

type BinaryRangeExpr struct {
	ExprImpl
	lowerVal     *storage.ScalarFieldValue
	upperVal     *storage.ScalarFieldValue
	includeLower bool
	includeUpper bool
}

func NewBinaryRangeExpr(sz uint, lower *storage.ScalarFieldValue,
	upper *storage.ScalarFieldValue, inLower bool, inUpper bool) *BinaryRangeExpr {
	return &BinaryRangeExpr{ExprImpl: ExprImpl{size: sz}, lowerVal: lower, upperVal: upper,
		includeLower: inLower, includeUpper: inUpper}
}

func (bre *BinaryRangeExpr) Eval(segmentStats []*storage.SegmentStats,
	bst **bitset.BitSet) {
	localBst := bitset.New(bre.Size())
	for i, segStat := range segmentStats {
		fieldStat := &(segStat.FieldStats[0])
		idx := uint(i)
		commonMin := storage.MaxScalar(&fieldStat.Min, bre.lowerVal)
		commonMax := storage.MinScalar(&fieldStat.Max, bre.upperVal)
		if !((*commonMin).GT(*commonMax)) {
			localBst.Set(idx)
		}
	}
	*bst = localBst
}

func (bre *BinaryRangeExpr) Inputs() []Expr {
	return nil
}

type UnaryRangeExpr struct {
	ExprImpl
	op  planpb.OpType
	val *storage.ScalarFieldValue
}

func NewUnaryRangeExpr(sz uint, value *storage.ScalarFieldValue, op planpb.OpType) *UnaryRangeExpr {
	return &UnaryRangeExpr{ExprImpl: ExprImpl{size: sz}, op: op, val: value}
}

func (ure *UnaryRangeExpr) Eval(
	segmentStats []*storage.SegmentStats,
	bst **bitset.BitSet) {
	localBst := bitset.New(ure.Size())
	for i, segStat := range segmentStats {
		fieldStat := &(segStat.FieldStats[0])
		idx := uint(i)
		val := ure.val
		switch ure.op {
		case planpb.OpType_Equal:
			if (*val).GE(fieldStat.Min) && (*val).LE(fieldStat.Max) {
				localBst.Set(idx)
			}
		case planpb.OpType_LessEqual:
			if !((*val).LT(fieldStat.Min)) {
				localBst.Set(idx)
			}
		case planpb.OpType_LessThan:
			if !((*val).LE(fieldStat.Min)) {
				localBst.Set(idx)
			}
		case planpb.OpType_GreaterEqual:
			if !((*val).GT(fieldStat.Max)) {
				localBst.Set(idx)
			}
		case planpb.OpType_GreaterThan:
			if !((*val).GE(fieldStat.Max)) {
				localBst.Set(idx)
			}
		case planpb.OpType_NotEqual:
			if (*val).LT(fieldStat.Min) || (*val).GT(fieldStat.Max) {
				localBst.Set(idx)
			}
		}
	}
	*bst = localBst
}

func (ure *UnaryRangeExpr) Inputs() []Expr {
	return nil
}

type TermExpr struct {
	ExprImpl
	vals []*storage.ScalarFieldValue
}

func NewTermExpr(sz uint, values []*storage.ScalarFieldValue) *TermExpr {
	return &TermExpr{ExprImpl: ExprImpl{size: sz}, vals: values}
}

func (te *TermExpr) Eval(segmentStats []*storage.SegmentStats,
	bst **bitset.BitSet) {
	localBst := bitset.New(te.Size())
	for i, segStat := range segmentStats {
		fieldStat := &(segStat.FieldStats[0])
		for _, val := range te.vals {
			if (*val).GT(fieldStat.Max) {
				//as the vals inside expr has been sorted before executed, if current val has exceeded the max, then
				//no need to iterate over other values
				break
			}
			if fieldStat.Min.LE(*val) && (*val).LE(fieldStat.Max) {
				localBst.Set(uint(i))
				break
			}
		}
	}
	*bst = localBst
}

func (te *TermExpr) Inputs() []Expr {
	return nil
}

func ParseExpr(exprPb *planpb.Expr, size uint) Expr {
	var res Expr
	switch exp := exprPb.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		res = ParseLogicalBinaryExpr(exp.BinaryExpr)
	case *planpb.Expr_UnaryExpr:
		res = ParseLogicalUnaryExpr(exp.UnaryExpr)
	case *planpb.Expr_BinaryRangeExpr:
		res = ParseBinaryRangeExpr(exp.BinaryRangeExpr)
	case *planpb.Expr_UnaryRangeExpr:
		res = ParseUnaryRangeExpr(exp.UnaryRangeExpr)
	case *planpb.Expr_TermExpr:
		res = ParseTermExpr(exp.TermExpr)
	}
	return res
}

func ParseLogicalBinaryExpr(exprPb *planpb.BinaryExpr) *LogicalBinaryExpr {
	leftExpr := ParseExpr(exprPb.Left)
	rightExpr := ParseExpr(exprPb.Right)
	NewLogicalBinaryExpr()
	return nil
}

func ParseLogicalUnaryExpr(exprPb *planpb.UnaryExpr) *LogicalUnaryExpr {
	return nil
}

func ParseBinaryRangeExpr(exprPb *planpb.BinaryRangeExpr) *BinaryRangeExpr {
	return nil
}

func ParseUnaryRangeExpr(exprPb *planpb.UnaryRangeExpr) *UnaryRangeExpr {
	return nil
}

func ParseTermExpr(exprPb *planpb.TermExpr) *TermExpr {
	return nil
}
