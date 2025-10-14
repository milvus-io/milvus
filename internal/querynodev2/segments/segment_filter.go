// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segments

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// segmentCriterion is the segment filter criterion obj.
type segmentCriterion struct {
	segmentIDs  typeutil.Set[int64]
	channel     metautil.Channel
	segmentType SegmentType
	others      []SegmentFilter
}

func (c *segmentCriterion) Match(segment Segment) bool {
	for _, filter := range c.others {
		if !filter.Match(segment) {
			return false
		}
	}
	return true
}

// SegmentFilter is the interface for segment selection criteria.
type SegmentFilter interface {
	Match(segment Segment) bool
	AddFilter(*segmentCriterion)
}

// SegmentFilterFunc is a type wrapper for `func(Segment) bool` to SegmentFilter.
type SegmentFilterFunc func(segment Segment) bool

func (f SegmentFilterFunc) Match(segment Segment) bool {
	return f(segment)
}

func (f SegmentFilterFunc) AddFilter(c *segmentCriterion) {
	c.others = append(c.others, f)
}

// SegmentIDFilter is the specific segment filter for SegmentID only.
type SegmentIDFilter int64

func (f SegmentIDFilter) Match(segment Segment) bool {
	return segment.ID() == int64(f)
}

func (f SegmentIDFilter) AddFilter(c *segmentCriterion) {
	if c.segmentIDs == nil {
		c.segmentIDs = typeutil.NewSet(int64(f))
		return
	}
	c.segmentIDs = c.segmentIDs.Intersection(typeutil.NewSet(int64(f)))
}

type SegmentIDsFilter struct {
	segmentIDs typeutil.Set[int64]
}

func (f SegmentIDsFilter) Match(segment Segment) bool {
	return f.segmentIDs.Contain(segment.ID())
}

func (f SegmentIDsFilter) AddFilter(c *segmentCriterion) {
	if c.segmentIDs == nil {
		c.segmentIDs = f.segmentIDs
		return
	}
	c.segmentIDs = c.segmentIDs.Intersection(f.segmentIDs)
}

type SegmentTypeFilter SegmentType

func (f SegmentTypeFilter) Match(segment Segment) bool {
	return segment.Type() == SegmentType(f)
}

func (f SegmentTypeFilter) AddFilter(c *segmentCriterion) {
	c.segmentType = SegmentType(f)
}

func WithSkipEmpty() SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.InsertCount() > 0
	})
}

func WithPartition(partitionID typeutil.UniqueID) SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Partition() == partitionID
	})
}

func WithChannel(channel string) SegmentFilter {
	ac, err := metautil.ParseChannel(channel, channelMapper)
	if err != nil {
		return SegmentFilterFunc(func(segment Segment) bool {
			return false
		})
	}
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Shard().Equal(ac)
	})
}

func WithType(typ SegmentType) SegmentFilter {
	return SegmentTypeFilter(typ)
}

func WithID(id int64) SegmentFilter {
	return SegmentIDFilter(id)
}

func WithIDs(ids ...int64) SegmentFilter {
	return SegmentIDsFilter{
		segmentIDs: typeutil.NewSet(ids...),
	}
}

func WithLevel(level datapb.SegmentLevel) SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Level() == level
	})
}

// WithoutLevel is the segment filter for without segment level.
func WithoutLevel(level datapb.SegmentLevel) SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Level() != level
	})
}

// Bloom filter

type filterFunc func(pk storage.PrimaryKey, op planpb.OpType) bool

func binaryExprWalker(expr *planpb.BinaryExpr, filter filterFunc) bool {
	switch expr.Op {
	case planpb.BinaryExpr_LogicalAnd:
		// one of expression return false
		return exprWalker(expr.Left, filter) &&
			exprWalker(expr.Right, filter)

	case planpb.BinaryExpr_LogicalOr:
		// both left and right return false
		return exprWalker(expr.Left, filter) ||
			exprWalker(expr.Right, filter)
	}

	// unknown operator return no filter
	return true
}

func unaryRangeExprWalker(expr *planpb.UnaryRangeExpr, filter filterFunc) bool {
	if expr.GetColumnInfo() == nil ||
		!expr.GetColumnInfo().GetIsPrimaryKey() ||
		expr.GetValue() == nil {
		// not the primary key
		return true
	}

	var pk storage.PrimaryKey
	dt := expr.GetColumnInfo().GetDataType()

	switch dt {
	case schemapb.DataType_Int64:
		pk = storage.NewInt64PrimaryKey(expr.GetValue().GetInt64Val())
	case schemapb.DataType_VarChar:
		pk = storage.NewVarCharPrimaryKey(expr.GetValue().GetStringVal())
	default:
		log.Warn("unknown pk type",
			zap.Int("type", int(dt)),
			zap.String("expr", expr.String()))
		return true
	}

	return filter(pk, expr.Op)
}

func termExprWalker(expr *planpb.TermExpr, filter filterFunc) bool {
	noFilter := true
	if expr.GetColumnInfo() == nil ||
		!expr.GetColumnInfo().GetIsPrimaryKey() {
		return noFilter
	}

	// In empty array, direct return
	if expr.GetValues() == nil {
		return false
	}

	var pk storage.PrimaryKey
	dt := expr.GetColumnInfo().GetDataType()
	invals := expr.GetValues()

	for _, pkval := range invals {
		switch dt {
		case schemapb.DataType_Int64:
			pk = storage.NewInt64PrimaryKey(pkval.GetInt64Val())
		case schemapb.DataType_VarChar:
			pk = storage.NewVarCharPrimaryKey(pkval.GetStringVal())
		default:
			log.Warn("unknown pk type",
				zap.Int("type", int(dt)),
				zap.String("expr", expr.String()))
			return noFilter
		}

		noFilter = filter(pk, planpb.OpType_Equal)
		if noFilter {
			break
		}
	}

	return noFilter
}

// return true if current segment can be filtered
func exprWalker(expr *planpb.Expr, filter filterFunc) bool {
	switch expr := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		return binaryExprWalker(expr.BinaryExpr, filter)
	case *planpb.Expr_UnaryRangeExpr:
		return unaryRangeExprWalker(expr.UnaryRangeExpr, filter)
	case *planpb.Expr_TermExpr:
		return termExprWalker(expr.TermExpr, filter)
	}

	return true
}

func doSparseFilter(seg Segment, plan *planpb.PlanNode) bool {
	queryPlan := plan.GetQuery()
	if queryPlan == nil {
		// do nothing if current plan not the query plan
		return true
	}

	pexpr := queryPlan.GetPredicates()
	if pexpr == nil {
		return true
	}

	return exprWalker(pexpr, func(pk storage.PrimaryKey, op planpb.OpType) bool {
		noFilter := true
		existMinMax := seg.GetMinPk() != nil && seg.GetMaxPk() != nil
		var minPk, maxPk storage.PrimaryKey
		if existMinMax {
			minPk = *seg.GetMinPk()
			maxPk = *seg.GetMaxPk()
		}

		switch op {
		case planpb.OpType_Equal:

			// bloom filter
			existBF := seg.BloomFilterExist()
			if existBF {
				lc := storage.NewLocationsCache(pk)
				// BloomFilter contains this key, no filter here
				noFilter = seg.MayPkExist(lc)
			}

			// no need check min/max again
			if !noFilter {
				break
			}

			// min/max filter
			noFilter = !(existMinMax && (minPk.GT(pk) || maxPk.LT(pk)))
		case planpb.OpType_GreaterThan:
			noFilter = !(existMinMax && maxPk.LE(pk))
		case planpb.OpType_GreaterEqual:
			noFilter = !(existMinMax && maxPk.LT(pk))
		case planpb.OpType_LessThan:
			noFilter = !(existMinMax && minPk.GE(pk))
		case planpb.OpType_LessEqual:
			noFilter = !(existMinMax && minPk.GT(pk))
		}

		return noFilter
	})
}

type SegmentSparseFilter SegmentType

func WithSparseFilter(plan *planpb.PlanNode) SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		if plan == nil {
			log.Debug("SparseFilter with nil plan")
			return true
		}

		rc := doSparseFilter(segment, plan)

		log.Debug("SparseFilter",
			zap.Int64("Segment ID", segment.ID()),
			zap.Bool("No Filter", rc),
			zap.Bool("Exist BF", segment.BloomFilterExist()))
		return rc
	})
}
