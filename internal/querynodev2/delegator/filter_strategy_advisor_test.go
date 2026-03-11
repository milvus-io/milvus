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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

// makeInt64SegStats builds a SegmentStats with a single Int64 field.
func makeInt64SegStats(fieldID int64, minVal, maxVal int64) storage.SegmentStats {
	return storage.SegmentStats{
		FieldStats: []storage.FieldStats{
			{
				FieldID: fieldID,
				Type:    schemapb.DataType_Int64,
				Min:     storage.NewInt64FieldValue(minVal),
				Max:     storage.NewInt64FieldValue(maxVal),
			},
		},
	}
}

// makeUnaryRangeExpr builds a planpb.Expr for "field op val".
func makeUnaryRangeExpr(fieldID int64, op planpb.OpType, val int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: fieldID},
				Op:         op,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: val}},
			},
		},
	}
}

// makeBinaryRangeExpr builds a planpb.Expr for "lower op field op upper".
func makeBinaryRangeExpr(fieldID int64, lower, upper int64, inclLower, inclUpper bool) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryRangeExpr{
			BinaryRangeExpr: &planpb.BinaryRangeExpr{
				ColumnInfo:     &planpb.ColumnInfo{FieldId: fieldID},
				LowerValue:     &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: lower}},
				UpperValue:     &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: upper}},
				LowerInclusive: inclLower,
				UpperInclusive: inclUpper,
			},
		},
	}
}

// makeTermExpr builds a planpb.Expr for "field IN [vals...]".
func makeTermExpr(fieldID int64, vals ...int64) *planpb.Expr {
	gVals := make([]*planpb.GenericValue, len(vals))
	for i, v := range vals {
		gVals[i] = &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: v}}
	}
	return &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: fieldID},
				Values:     gVals,
			},
		},
	}
}

func makeAndExpr(left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  left,
				Right: right,
				Op:    planpb.BinaryExpr_LogicalAnd,
			},
		},
	}
}

func makeOrExpr(left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Left:  left,
				Right: right,
				Op:    planpb.BinaryExpr_LogicalOr,
			},
		},
	}
}

func makeNotExpr(child *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{
				Child: child,
				Op:    planpb.UnaryExpr_Not,
			},
		},
	}
}

// ---- EstimateSelectivity tests ----

func TestEstimateSelectivity_NilExpr(t *testing.T) {
	seg := makeInt64SegStats(1, 0, 100)
	sel := EstimateSelectivity(nil, seg)
	assert.Equal(t, defaultSelectivity, sel)
}

func TestEstimateSelectivity_GreaterThan_HighSelectivity(t *testing.T) {
	// field > 1 on [0, 100]: ~99% pass
	seg := makeInt64SegStats(1, 0, 100)
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 1)
	sel := EstimateSelectivity(expr, seg)
	assert.InDelta(t, 0.99, sel, 0.01)
}

func TestEstimateSelectivity_GreaterThan_LowSelectivity(t *testing.T) {
	// field > 99 on [0, 100]: ~1% pass
	seg := makeInt64SegStats(1, 0, 100)
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 99)
	sel := EstimateSelectivity(expr, seg)
	assert.InDelta(t, 0.01, sel, 0.01)
}

func TestEstimateSelectivity_LessThan(t *testing.T) {
	// field < 50 on [0, 100]: ~50% pass
	seg := makeInt64SegStats(1, 0, 100)
	expr := makeUnaryRangeExpr(1, planpb.OpType_LessThan, 50)
	sel := EstimateSelectivity(expr, seg)
	assert.InDelta(t, 0.50, sel, 0.01)
}

func TestEstimateSelectivity_BinaryRange(t *testing.T) {
	// 10 <= field <= 90 on [0, 100]: ~80% pass
	seg := makeInt64SegStats(1, 0, 100)
	expr := makeBinaryRangeExpr(1, 10, 90, true, true)
	sel := EstimateSelectivity(expr, seg)
	assert.InDelta(t, 0.80, sel, 0.01)
}

func TestEstimateSelectivity_BinaryRange_EmptyRange(t *testing.T) {
	// 90 <= field <= 10 on [0, 100]: 0% pass
	seg := makeInt64SegStats(1, 0, 100)
	expr := makeBinaryRangeExpr(1, 90, 10, true, true)
	sel := EstimateSelectivity(expr, seg)
	assert.Equal(t, 0.0, sel)
}

func TestEstimateSelectivity_MinEqualsMax(t *testing.T) {
	// segment has only one value: 50
	seg := makeInt64SegStats(1, 50, 50)
	// field > 50: 0% pass (nothing above 50)
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 50)
	sel := EstimateSelectivity(expr, seg)
	assert.Equal(t, defaultSelectivity, sel)
}

func TestEstimateSelectivity_And(t *testing.T) {
	// (field > 10) AND (field < 90) on [0, 100]
	// ~90% AND ~90% ≈ 81%
	seg := makeInt64SegStats(1, 0, 100)
	left := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 10)
	right := makeUnaryRangeExpr(1, planpb.OpType_LessThan, 90)
	expr := makeAndExpr(left, right)
	sel := EstimateSelectivity(expr, seg)
	assert.InDelta(t, 0.81, sel, 0.02)
}

func TestEstimateSelectivity_Or(t *testing.T) {
	// (field < 10) OR (field > 90) on [0, 100]
	// ~10% OR ~10% ≈ 19%
	seg := makeInt64SegStats(1, 0, 100)
	left := makeUnaryRangeExpr(1, planpb.OpType_LessThan, 10)
	right := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 90)
	expr := makeOrExpr(left, right)
	sel := EstimateSelectivity(expr, seg)
	assert.InDelta(t, 0.19, sel, 0.02)
}

func TestEstimateSelectivity_Not(t *testing.T) {
	// NOT (field < 10) on [0, 100]: ~90% pass
	seg := makeInt64SegStats(1, 0, 100)
	inner := makeUnaryRangeExpr(1, planpb.OpType_LessThan, 10)
	expr := makeNotExpr(inner)
	sel := EstimateSelectivity(expr, seg)
	assert.InDelta(t, 0.90, sel, 0.01)
}

func TestEstimateSelectivity_Term(t *testing.T) {
	// field IN [50] on [0, 100]: low selectivity
	seg := makeInt64SegStats(1, 0, 100)
	expr := makeTermExpr(1, 50)
	sel := EstimateSelectivity(expr, seg)
	assert.Less(t, sel, 0.1)
}

func TestEstimateSelectivity_Term_Empty(t *testing.T) {
	seg := makeInt64SegStats(1, 0, 100)
	expr := makeTermExpr(1) // no values
	sel := EstimateSelectivity(expr, seg)
	assert.Equal(t, 0.0, sel)
}

func TestEstimateSelectivity_MissingStats(t *testing.T) {
	// field 99 not in SegmentStats → fall back to defaultSelectivity
	seg := makeInt64SegStats(1, 0, 100)
	expr := makeUnaryRangeExpr(99, planpb.OpType_GreaterThan, 50)
	sel := EstimateSelectivity(expr, seg)
	assert.Equal(t, defaultSelectivity, sel)
}

func TestEstimateSelectivity_UnsupportedExpr(t *testing.T) {
	seg := makeInt64SegStats(1, 0, 100)
	// unknown expr type → defaultSelectivity
	expr := &planpb.Expr{}
	sel := EstimateSelectivity(expr, seg)
	assert.Equal(t, defaultSelectivity, sel)
}

// ---- AdviseFilterStrategy tests ----

func makePartitionStats(segID int64, minVal, maxVal int64) map[UniqueID]*storage.PartitionStatsSnapshot {
	return map[UniqueID]*storage.PartitionStatsSnapshot{
		1: {
			SegmentStats: map[UniqueID]storage.SegmentStats{
				segID: makeInt64SegStats(1, minVal, maxVal),
			},
		},
	}
}

func makeSealed(segID int64) []SnapshotItem {
	return []SnapshotItem{
		{
			NodeID: 1,
			Segments: []SegmentEntry{
				{SegmentID: segID},
			},
		},
	}
}

func TestAdviseFilterStrategy_HighSelectivity(t *testing.T) {
	// field > 1 on [0, 100]: ~99% → above threshold 0.5 → iterative filter
	partStats := makePartitionStats(100, 0, 100)
	sealed := makeSealed(100)
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 1)
	hints := AdviseFilterStrategy(expr, partStats, sealed, 0.5)
	assert.Equal(t, HintIterativeFilter, hints[100])
}

func TestAdviseFilterStrategy_LowSelectivity(t *testing.T) {
	// field > 99 on [0, 100]: ~1% → below threshold 0.5 → no hint
	partStats := makePartitionStats(100, 0, 100)
	sealed := makeSealed(100)
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 99)
	hints := AdviseFilterStrategy(expr, partStats, sealed, 0.5)
	_, ok := hints[100]
	assert.False(t, ok)
}

func TestAdviseFilterStrategy_NoStats(t *testing.T) {
	// segment 200 has no stats → not in result map
	partStats := makePartitionStats(100, 0, 100)
	sealed := makeSealed(200) // different segment ID
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 1)
	hints := AdviseFilterStrategy(expr, partStats, sealed, 0.5)
	_, ok := hints[200]
	assert.False(t, ok)
}

func TestAdviseFilterStrategy_NilExpr(t *testing.T) {
	partStats := makePartitionStats(100, 0, 100)
	sealed := makeSealed(100)
	hints := AdviseFilterStrategy(nil, partStats, sealed, 0.5)
	assert.Empty(t, hints)
}

func TestAdviseFilterStrategy_NilPartitionStats(t *testing.T) {
	sealed := makeSealed(100)
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 1)
	hints := AdviseFilterStrategy(expr, nil, sealed, 0.5)
	assert.Empty(t, hints)
}

func TestAdviseFilterStrategy_ThresholdBoundary(t *testing.T) {
	// selectivity exactly at threshold → should NOT trigger iterative filter (must be strictly greater)
	partStats := makePartitionStats(100, 0, 100)
	sealed := makeSealed(100)
	// field > 50 on [0, 100]: ~50% selectivity
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 50)
	sel := EstimateSelectivity(expr, makeInt64SegStats(1, 0, 100))
	// threshold == sel → no iterative filter
	hints := AdviseFilterStrategy(expr, partStats, sealed, sel)
	_, ok := hints[100]
	assert.False(t, ok)
}

func TestAdviseFilterStrategy_MultipleSegments(t *testing.T) {
	// two segments: one high selectivity, one low
	partStats := map[UniqueID]*storage.PartitionStatsSnapshot{
		1: {
			SegmentStats: map[UniqueID]storage.SegmentStats{
				100: makeInt64SegStats(1, 0, 100),  // field > 1 → ~99% → iterative
				200: makeInt64SegStats(1, 0, 1000), // field > 1 → ~0.1% → pre-filter
			},
		},
	}
	sealed := []SnapshotItem{
		{
			NodeID: 1,
			Segments: []SegmentEntry{
				{SegmentID: 100},
				{SegmentID: 200},
			},
		},
	}
	expr := makeUnaryRangeExpr(1, planpb.OpType_GreaterThan, 1)
	hints := AdviseFilterStrategy(expr, partStats, sealed, 0.5)
	assert.Equal(t, HintIterativeFilter, hints[100])
	_, ok := hints[200]
	assert.False(t, ok)
}
