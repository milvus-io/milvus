package delegator

import (
	"github.com/bits-and-blooms/bitset"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/storage"
)

func PruneByScalarField(expr *planpb.Expr, partStats []*storage.PartitionStatsSnapshot) {
	/*switch exprType := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		res, matchALL = ParseRangesFromBinaryExpr(expr.BinaryExpr, kType)
	case *planpb.Expr_UnaryRangeExpr:
		res, matchALL = ParseRangesFromUnaryRangeExpr(expr.UnaryRangeExpr, kType)
	case *planpb.Expr_TermExpr:
		res, matchALL = ParseRangesFromTermExpr(expr.TermExpr, kType)
	case *planpb.Expr_UnaryExpr:
		res, matchALL = nil, true
	}*/
}

type Expr struct {
	dataType schemapb.DataType
	inputs   []*Expr
	name     string
}

type TermExpr struct {
	Expr
	vals []*storage.ScalarFieldValue
}

type UnaryRangeExpr struct {
	Expr
	op  planpb.OpType
	val *storage.ScalarFieldValue
}

func ParseExprForScalarPrune(expr *planpb.Expr) {

}

func PruneUnaryRangeExpr(expr *UnaryRangeExpr,
	segmentStats []*storage.SegmentStats,
	bitset *bitset.BitSet) {
	for i, segStat := range segmentStats {
		fieldStat := &(segStat.FieldStats[0])
		switch expr.op {
		case planpb.OpType_Equal:

		}
	}
}

func PruneTermExpr(expr *TermExpr,
	segmentStats []*storage.SegmentStats,
	bitset *bitset.BitSet) {
	for i, segStat := range segmentStats {
		fieldStat := &(segStat.FieldStats[0])
		for _, val := range expr.vals {
			if (*val).GT(fieldStat.Max) {
				//as the vals inside expr has been sorted before executed, if current val has exceeded the max, then
				//no need to iterate over other values
				break
			}
			if fieldStat.Min.LE(*val) && (*val).LE(fieldStat.Max) {
				bitset.Set(uint(i))
				break
			}
		}
	}
}
