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

package delegator

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

const (
	// HintIterativeFilter is the hint value that enables iterative filter execution.
	HintIterativeFilter = "iterative_filter"
	// defaultSelectivity is used when stats are unavailable or expression type is unsupported.
	defaultSelectivity = 0.5
)

// EstimateSelectivity estimates the fraction of rows that pass the given expression
// for a single segment, using its FieldStats (min/max/bloom-filter).
// Returns a value in [0.0, 1.0]. Falls back to defaultSelectivity when stats are
// unavailable or the expression type is not supported.
func EstimateSelectivity(expr *planpb.Expr, segStats storage.SegmentStats) float64 {
	if expr == nil {
		return defaultSelectivity
	}
	return estimateExpr(expr, segStats)
}

func estimateExpr(expr *planpb.Expr, segStats storage.SegmentStats) float64 {
	switch e := expr.GetExpr().(type) {
	case *planpb.Expr_BinaryExpr:
		return estimateBinaryExpr(e.BinaryExpr, segStats)
	case *planpb.Expr_UnaryExpr:
		return estimateUnaryExpr(e.UnaryExpr, segStats)
	case *planpb.Expr_UnaryRangeExpr:
		return estimateUnaryRangeExpr(e.UnaryRangeExpr, segStats)
	case *planpb.Expr_BinaryRangeExpr:
		return estimateBinaryRangeExpr(e.BinaryRangeExpr, segStats)
	case *planpb.Expr_TermExpr:
		return estimateTermExpr(e.TermExpr, segStats)
	default:
		return defaultSelectivity
	}
}

func estimateBinaryExpr(expr *planpb.BinaryExpr, segStats storage.SegmentStats) float64 {
	left := estimateExpr(expr.GetLeft(), segStats)
	right := estimateExpr(expr.GetRight(), segStats)
	switch expr.GetOp() {
	case planpb.BinaryExpr_LogicalAnd:
		// independence assumption
		return left * right
	case planpb.BinaryExpr_LogicalOr:
		return left + right - left*right
	default:
		return defaultSelectivity
	}
}

func estimateUnaryExpr(expr *planpb.UnaryExpr, segStats storage.SegmentStats) float64 {
	if expr.GetOp() == planpb.UnaryExpr_Not {
		inner := estimateExpr(expr.GetChild(), segStats)
		return 1.0 - inner
	}
	return defaultSelectivity
}

func estimateUnaryRangeExpr(expr *planpb.UnaryRangeExpr, segStats storage.SegmentStats) float64 {
	fieldStat := findFieldStat(expr.GetColumnInfo().GetFieldId(), segStats)
	if fieldStat == nil {
		return defaultSelectivity
	}
	min := fieldStat.Min
	max := fieldStat.Max
	if min == nil || max == nil {
		return defaultSelectivity
	}
	// min == max: single-value segment
	if !min.LT(max) {
		switch expr.GetOp() {
		case planpb.OpType_Equal:
			val, err := storage.NewScalarFieldValueFromGenericValue(fieldStat.Type, expr.GetValue())
			if err != nil {
				return defaultSelectivity
			}
			if val.EQ(min) {
				return 1.0
			}
			return 0.0
		case planpb.OpType_NotEqual:
			val, err := storage.NewScalarFieldValueFromGenericValue(fieldStat.Type, expr.GetValue())
			if err != nil {
				return defaultSelectivity
			}
			if val.EQ(min) {
				return 0.0
			}
			return 1.0
		default:
			return defaultSelectivity
		}
	}

	val, err := storage.NewScalarFieldValueFromGenericValue(fieldStat.Type, expr.GetValue())
	if err != nil {
		return defaultSelectivity
	}

	switch expr.GetOp() {
	case planpb.OpType_Equal:
		// use bloom filter if available
		if fieldStat.BF != nil {
			sel := estimateEqualSelectivity(fieldStat, val)
			return sel
		}
		return defaultSelectivity
	case planpb.OpType_NotEqual:
		if fieldStat.BF != nil {
			return 1.0 - estimateEqualSelectivity(fieldStat, val)
		}
		return defaultSelectivity
	case planpb.OpType_GreaterThan:
		// fraction of [val+ε, max] in [min, max]
		return rangeSelectivity(min, max, val, max, false, true)
	case planpb.OpType_GreaterEqual:
		return rangeSelectivity(min, max, val, max, true, true)
	case planpb.OpType_LessThan:
		return rangeSelectivity(min, max, min, val, true, false)
	case planpb.OpType_LessEqual:
		return rangeSelectivity(min, max, min, val, true, true)
	default:
		return defaultSelectivity
	}
}

func estimateBinaryRangeExpr(expr *planpb.BinaryRangeExpr, segStats storage.SegmentStats) float64 {
	fieldStat := findFieldStat(expr.GetColumnInfo().GetFieldId(), segStats)
	if fieldStat == nil {
		return defaultSelectivity
	}
	min := fieldStat.Min
	max := fieldStat.Max
	if min == nil || max == nil {
		return defaultSelectivity
	}
	lower, err := storage.NewScalarFieldValueFromGenericValue(fieldStat.Type, expr.GetLowerValue())
	if err != nil {
		return defaultSelectivity
	}
	upper, err := storage.NewScalarFieldValueFromGenericValue(fieldStat.Type, expr.GetUpperValue())
	if err != nil {
		return defaultSelectivity
	}
	return rangeSelectivity(min, max, lower, upper, expr.GetLowerInclusive(), expr.GetUpperInclusive())
}

func estimateTermExpr(expr *planpb.TermExpr, segStats storage.SegmentStats) float64 {
	fieldStat := findFieldStat(expr.GetColumnInfo().GetFieldId(), segStats)
	if fieldStat == nil {
		return defaultSelectivity
	}
	if len(expr.GetValues()) == 0 {
		return 0.0
	}
	total := 0.0
	for _, v := range expr.GetValues() {
		val, err := storage.NewScalarFieldValueFromGenericValue(fieldStat.Type, v)
		if err != nil {
			total += defaultSelectivity
			continue
		}
		total += estimateEqualSelectivity(fieldStat, val)
	}
	return math.Min(total, 1.0)
}

// rangeSelectivity estimates the fraction of [lo, hi] within [min, max].
// For numeric types this is a linear interpolation; for strings it falls back to defaultSelectivity.
func rangeSelectivity(min, max, lo, hi storage.ScalarFieldValue, loInclusive, hiInclusive bool) float64 {
	if min == nil || max == nil || lo == nil || hi == nil {
		return defaultSelectivity
	}
	// clamp lo/hi to [min, max]
	if lo.LT(min) {
		lo = min
	}
	if hi.GT(max) {
		hi = max
	}
	// empty range
	if hi.LT(lo) || (!loInclusive && !hiInclusive && hi.EQ(lo)) {
		return 0.0
	}

	switch min.(type) {
	case *storage.Int8FieldValue:
		minV := float64(min.GetValue().(int8))
		maxV := float64(max.GetValue().(int8))
		loV := float64(lo.GetValue().(int8))
		hiV := float64(hi.GetValue().(int8))
		return linearSelectivity(minV, maxV, loV, hiV)
	case *storage.Int16FieldValue:
		minV := float64(min.GetValue().(int16))
		maxV := float64(max.GetValue().(int16))
		loV := float64(lo.GetValue().(int16))
		hiV := float64(hi.GetValue().(int16))
		return linearSelectivity(minV, maxV, loV, hiV)
	case *storage.Int32FieldValue:
		minV := float64(min.GetValue().(int32))
		maxV := float64(max.GetValue().(int32))
		loV := float64(lo.GetValue().(int32))
		hiV := float64(hi.GetValue().(int32))
		return linearSelectivity(minV, maxV, loV, hiV)
	case *storage.Int64FieldValue:
		minV := float64(min.GetValue().(int64))
		maxV := float64(max.GetValue().(int64))
		loV := float64(lo.GetValue().(int64))
		hiV := float64(hi.GetValue().(int64))
		return linearSelectivity(minV, maxV, loV, hiV)
	case *storage.FloatFieldValue:
		minV := float64(min.GetValue().(float32))
		maxV := float64(max.GetValue().(float32))
		loV := float64(lo.GetValue().(float32))
		hiV := float64(hi.GetValue().(float32))
		return linearSelectivity(minV, maxV, loV, hiV)
	case *storage.DoubleFieldValue:
		minV := min.GetValue().(float64)
		maxV := max.GetValue().(float64)
		loV := lo.GetValue().(float64)
		hiV := hi.GetValue().(float64)
		return linearSelectivity(minV, maxV, loV, hiV)
	default:
		// strings and other types: fall back
		return defaultSelectivity
	}
}

func linearSelectivity(minV, maxV, loV, hiV float64) float64 {
	span := maxV - minV
	if span <= 0 {
		return 1.0
	}
	sel := (hiV - loV) / span
	if sel < 0 {
		return 0.0
	}
	if sel > 1 {
		return 1.0
	}
	return sel
}

// estimateEqualSelectivity estimates selectivity for a single equality predicate.
// Uses bloom filter to detect definite misses; otherwise falls back to 1/NDV heuristic.
func estimateEqualSelectivity(fieldStat *storage.FieldStats, val storage.ScalarFieldValue) float64 {
	if fieldStat.BF != nil {
		exists := bloomFilterCheck(fieldStat, val)
		if !exists {
			return 0.0
		}
	}
	// value is possibly present; estimate 1/NDV
	// use a simple heuristic: NDV ≈ (max - min) for integers, else 100
	ndv := estimateNDV(fieldStat)
	if ndv <= 0 {
		return defaultSelectivity
	}
	return math.Min(1.0/float64(ndv), 1.0)
}

func estimateNDV(fieldStat *storage.FieldStats) float64 {
	if fieldStat.Min == nil || fieldStat.Max == nil {
		return 100
	}
	switch fieldStat.Type {
	case schemapb.DataType_Int8:
		span := float64(fieldStat.Max.GetValue().(int8)) - float64(fieldStat.Min.GetValue().(int8))
		return math.Max(span+1, 1)
	case schemapb.DataType_Int16:
		span := float64(fieldStat.Max.GetValue().(int16)) - float64(fieldStat.Min.GetValue().(int16))
		return math.Max(span+1, 1)
	case schemapb.DataType_Int32:
		span := float64(fieldStat.Max.GetValue().(int32)) - float64(fieldStat.Min.GetValue().(int32))
		return math.Max(span+1, 1)
	case schemapb.DataType_Int64:
		span := float64(fieldStat.Max.GetValue().(int64)) - float64(fieldStat.Min.GetValue().(int64))
		return math.Max(span+1, 1)
	case schemapb.DataType_Float:
		span := float64(fieldStat.Max.GetValue().(float32)) - float64(fieldStat.Min.GetValue().(float32))
		return math.Max(span, 1)
	case schemapb.DataType_Double:
		span := fieldStat.Max.GetValue().(float64) - fieldStat.Min.GetValue().(float64)
		return math.Max(span, 1)
	default:
		return 100
	}
}

func bloomFilterCheck(fieldStat *storage.FieldStats, val storage.ScalarFieldValue) bool {
	if fieldStat.BF == nil {
		return true
	}
	switch fieldStat.Type {
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
		schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double:
		// encode as 8-byte little-endian
		b := encodeNumericToBytes(fieldStat.Type, val)
		if b == nil {
			return true
		}
		return fieldStat.BF.Test(b)
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		s, ok := val.GetValue().(string)
		if !ok {
			return true
		}
		return fieldStat.BF.TestString(s)
	default:
		return true
	}
}

func encodeNumericToBytes(dt schemapb.DataType, val storage.ScalarFieldValue) []byte {
	b := make([]byte, 8)
	switch dt {
	case schemapb.DataType_Int8:
		v, ok := val.GetValue().(int8)
		if !ok {
			return nil
		}
		common.Endian.PutUint64(b, uint64(v))
	case schemapb.DataType_Int16:
		v, ok := val.GetValue().(int16)
		if !ok {
			return nil
		}
		common.Endian.PutUint64(b, uint64(v))
	case schemapb.DataType_Int32:
		v, ok := val.GetValue().(int32)
		if !ok {
			return nil
		}
		common.Endian.PutUint64(b, uint64(v))
	case schemapb.DataType_Int64:
		v, ok := val.GetValue().(int64)
		if !ok {
			return nil
		}
		common.Endian.PutUint64(b, uint64(v))
	case schemapb.DataType_Float:
		v, ok := val.GetValue().(float32)
		if !ok {
			return nil
		}
		common.Endian.PutUint64(b, uint64(v))
	case schemapb.DataType_Double:
		v, ok := val.GetValue().(float64)
		if !ok {
			return nil
		}
		common.Endian.PutUint64(b, uint64(v))
	default:
		return nil
	}
	return b
}

// findFieldStat returns the FieldStats for the given fieldID, or nil if not found.
func findFieldStat(fieldID int64, segStats storage.SegmentStats) *storage.FieldStats {
	for i := range segStats.FieldStats {
		if segStats.FieldStats[i].FieldID == fieldID {
			return &segStats.FieldStats[i]
		}
	}
	return nil
}

// AdvisedHints maps segmentID → hints string to inject into the search plan.
// An empty map means no override (use whatever hints the user supplied).
type AdvisedHints map[int64]string

// AdviseFilterStrategy examines each sealed segment's FieldStats and returns
// a per-segment hints override when the adaptive strategy is enabled.
//
// Segments whose stats are unavailable are left out of the map (fall back to
// the user-supplied hints / pre-filter default).
func AdviseFilterStrategy(
	expr *planpb.Expr,
	partitionStats map[UniqueID]*storage.PartitionStatsSnapshot,
	sealed []SnapshotItem,
	threshold float64,
) AdvisedHints {
	result := make(AdvisedHints)
	if expr == nil || partitionStats == nil {
		return result
	}

	// build a flat segmentID → SegmentStats lookup from all partitions
	segStatsMap := make(map[int64]storage.SegmentStats)
	for _, partStats := range partitionStats {
		if partStats == nil {
			continue
		}
		for segID, segStat := range partStats.SegmentStats {
			segStatsMap[segID] = segStat
		}
	}

	for _, item := range sealed {
		for _, entry := range item.Segments {
			segStat, ok := segStatsMap[entry.SegmentID]
			if !ok {
				// no stats available → skip (keep existing hints / pre-filter default)
				continue
			}
			sel := EstimateSelectivity(expr, segStat)
			if sel > threshold {
				result[entry.SegmentID] = HintIterativeFilter
			}
		}
	}
	return result
}
