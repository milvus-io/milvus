package delegator

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/storage"
)

type EvalCtx struct {
	segmentStats  []*storage.SegmentStats
	size          uint
	allTrueBitSet *bitset.BitSet
}

func NewEvalCtx(segStats []*storage.SegmentStats, size uint, allTrueBst *bitset.BitSet) *EvalCtx {
	return &EvalCtx{segStats, size, allTrueBst}
}

type Expr interface {
	Inputs() []Expr
	Eval(evalCtx *EvalCtx, bitset **bitset.BitSet)
}

func PruneByScalarField(expr Expr, segmentStats []*storage.SegmentStats, segmentIDs []UniqueID, filteredSegments map[UniqueID]struct{}) {
	var bst **bitset.BitSet
	size := uint(len(segmentIDs))
	allTrueBst := bitset.New(size)
	allTrueBst.FlipRange(0, size)
	expr.Eval(NewEvalCtx(segmentStats, size, allTrueBst), bst)
	(*bst).FlipRange(0, (*bst).Len())
	for i, e := (*bst).NextSet(0); e; i, e = (*bst).NextSet(i + 1) {
		filteredSegments[segmentIDs[i]] = struct{}{}
	}
}

type LogicalBinaryExpr struct {
	left  Expr
	right Expr
	op    planpb.BinaryExpr_BinaryOp
}

func NewLogicalBinaryExpr(l Expr, r Expr, op planpb.BinaryExpr_BinaryOp) *LogicalBinaryExpr {
	return &LogicalBinaryExpr{left: l, right: r, op: op}
}

func (lbe *LogicalBinaryExpr) Eval(evalCtx *EvalCtx, bst **bitset.BitSet) {
	//1. eval left
	leftExpr := lbe.Inputs()[0]
	var leftBitSet **bitset.BitSet
	if leftExpr != nil {
		leftExpr.Eval(evalCtx, leftBitSet)
	}

	//2. eval right
	rightExpr := lbe.Inputs()[1]
	var rightBitSet **bitset.BitSet
	if rightExpr != nil {
		rightExpr.Eval(evalCtx, leftBitSet)
	}

	//3. set true for possible nil expr
	if leftBitSet == nil {
		*leftBitSet = evalCtx.allTrueBitSet
	}
	if rightBitSet == nil {
		*rightBitSet = evalCtx.allTrueBitSet
	}

	//4. and/or left/right results
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
	inner Expr
}

func NewLogicalUnaryExpr(in Expr) *LogicalUnaryExpr {
	return &LogicalUnaryExpr{inner: in}
}

func (lue *LogicalUnaryExpr) Eval(evalCtx *EvalCtx, bst **bitset.BitSet) {
	inner := lue.Inputs()[0]
	if inner != nil {
		(inner).Eval(evalCtx, bst)
		(*bst).FlipRange(0, evalCtx.size)
	} else {
		*bst = evalCtx.allTrueBitSet
	}
}

func (lue *LogicalUnaryExpr) Inputs() []Expr {
	return []Expr{lue.inner}
}

type BinaryRangeExpr struct {
	lowerVal     storage.ScalarFieldValue
	upperVal     storage.ScalarFieldValue
	includeLower bool
	includeUpper bool
}

func NewBinaryRangeExpr(lower storage.ScalarFieldValue,
	upper storage.ScalarFieldValue, inLower bool, inUpper bool) *BinaryRangeExpr {
	return &BinaryRangeExpr{lowerVal: lower, upperVal: upper, includeLower: inLower, includeUpper: inUpper}
}

func (bre *BinaryRangeExpr) Eval(evalCtx *EvalCtx, bst **bitset.BitSet) {
	localBst := bitset.New(evalCtx.size)
	for i, segStat := range evalCtx.segmentStats {
		fieldStat := &(segStat.FieldStats[0])
		idx := uint(i)
		commonMin := storage.MaxScalar(fieldStat.Min, bre.lowerVal)
		commonMax := storage.MinScalar(fieldStat.Max, bre.upperVal)
		if !((commonMin).GT(commonMax)) {
			localBst.Set(idx)
		}
	}
	*bst = localBst
}

func (bre *BinaryRangeExpr) Inputs() []Expr {
	return nil
}

type UnaryRangeExpr struct {
	op  planpb.OpType
	val storage.ScalarFieldValue
}

func NewUnaryRangeExpr(value storage.ScalarFieldValue, op planpb.OpType) *UnaryRangeExpr {
	return &UnaryRangeExpr{op: op, val: value}
}

func (ure *UnaryRangeExpr) Eval(
	evalCtx *EvalCtx,
	bst **bitset.BitSet) {
	localBst := bitset.New(evalCtx.size)
	for i, segStat := range evalCtx.segmentStats {
		fieldStat := &(segStat.FieldStats[0])
		idx := uint(i)
		val := ure.val
		switch ure.op {
		case planpb.OpType_Equal:
			if val.GE(fieldStat.Min) && val.LE(fieldStat.Max) {
				localBst.Set(idx)
			}
		case planpb.OpType_LessEqual:
			if !(val.LT(fieldStat.Min)) {
				localBst.Set(idx)
			}
		case planpb.OpType_LessThan:
			if !(val.LE(fieldStat.Min)) {
				localBst.Set(idx)
			}
		case planpb.OpType_GreaterEqual:
			if !(val.GT(fieldStat.Max)) {
				localBst.Set(idx)
			}
		case planpb.OpType_GreaterThan:
			if !(val.GE(fieldStat.Max)) {
				localBst.Set(idx)
			}
		case planpb.OpType_NotEqual:
			if val.LT(fieldStat.Min) || val.GT(fieldStat.Max) {
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
	vals []storage.ScalarFieldValue
}

func NewTermExpr(values []storage.ScalarFieldValue) *TermExpr {
	return &TermExpr{vals: values}
}

func (te *TermExpr) Eval(evalCtx *EvalCtx,
	bst **bitset.BitSet) {
	localBst := bitset.New(evalCtx.size)
	for i, segStat := range evalCtx.segmentStats {
		fieldStat := &(segStat.FieldStats[0])
		for _, val := range te.vals {
			if val.GT(fieldStat.Max) {
				//as the vals inside expr has been sorted before executed, if current val has exceeded the max, then
				//no need to iterate over other values
				break
			}
			if fieldStat.Min.LE(val) && (val).LE(fieldStat.Max) {
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

type ParseContext struct {
	keyFieldIDToPrune FieldID
	dataType          schemapb.DataType
}

func NewParseContext(keyField FieldID, dType schemapb.DataType) *ParseContext {
	return &ParseContext{keyField, dType}
}

func ParseExpr(exprPb *planpb.Expr, parseCtx *ParseContext) Expr {
	var res Expr
	switch exp := exprPb.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		res = ParseLogicalBinaryExpr(exp.BinaryExpr, parseCtx)
	case *planpb.Expr_UnaryExpr:
		res = ParseLogicalUnaryExpr(exp.UnaryExpr, parseCtx)
	case *planpb.Expr_BinaryRangeExpr:
		res = ParseBinaryRangeExpr(exp.BinaryRangeExpr, parseCtx)
	case *planpb.Expr_UnaryRangeExpr:
		res = ParseUnaryRangeExpr(exp.UnaryRangeExpr, parseCtx)
	case *planpb.Expr_TermExpr:
		res = ParseTermExpr(exp.TermExpr, parseCtx)
	}
	return res
}

func ParseLogicalBinaryExpr(exprPb *planpb.BinaryExpr, parseCtx *ParseContext) *LogicalBinaryExpr {
	leftExpr := ParseExpr(exprPb.Left, parseCtx)
	rightExpr := ParseExpr(exprPb.Right, parseCtx)
	return NewLogicalBinaryExpr(leftExpr, rightExpr, exprPb.GetOp())
}

func ParseLogicalUnaryExpr(exprPb *planpb.UnaryExpr, parseCtx *ParseContext) *LogicalUnaryExpr {
	innerExpr := ParseExpr(exprPb.Child, parseCtx)
	return NewLogicalUnaryExpr(innerExpr)
}

func ParseBinaryRangeExpr(exprPb *planpb.BinaryRangeExpr, parseCtx *ParseContext) *BinaryRangeExpr {
	if exprPb.GetColumnInfo().GetFieldId() != parseCtx.keyFieldIDToPrune {
		return nil
	}
	lower := storage.NewScalarFieldValue(parseCtx.dataType, exprPb.GetLowerValue())
	upper := storage.NewScalarFieldValue(parseCtx.dataType, exprPb.GetUpperInclusive())
	return NewBinaryRangeExpr(lower, upper, exprPb.LowerInclusive, exprPb.UpperInclusive)
}

func ParseUnaryRangeExpr(exprPb *planpb.UnaryRangeExpr, parseCtx *ParseContext) *UnaryRangeExpr {
	if exprPb.GetColumnInfo().GetFieldId() != parseCtx.keyFieldIDToPrune {
		return nil
	}
	innerVal := storage.NewScalarFieldValue(parseCtx.dataType, exprPb.GetValue())
	return NewUnaryRangeExpr(innerVal, exprPb.GetOp())
}

func ParseTermExpr(exprPb *planpb.TermExpr, parseCtx *ParseContext) *TermExpr {
	if exprPb.GetColumnInfo().GetFieldId() != parseCtx.keyFieldIDToPrune {
		return nil
	}
	scalarVals := make([]storage.ScalarFieldValue, 0)
	for _, val := range exprPb.GetValues() {
		scalarVals = append(scalarVals, storage.NewScalarFieldValue(parseCtx.dataType, val))
	}
	return NewTermExpr(scalarVals)
}
