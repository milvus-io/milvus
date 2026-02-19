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
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// buildTestSealedSegments creates sealed SnapshotItems with bloom filter candidates for testing.
func buildTestSealedSegments(bfsBySegment map[int64]*pkoracle.BloomFilterSet, partitionID int64) []SnapshotItem {
	var segments []SegmentEntry
	for segID, bfs := range bfsBySegment {
		segments = append(segments, SegmentEntry{
			SegmentID:   segID,
			PartitionID: partitionID,
			Candidate:   bfs,
		})
	}
	return []SnapshotItem{{
		NodeID:   1,
		Segments: segments,
	}}
}

func marshalPlan(t *testing.T, plan *planpb.PlanNode) []byte {
	data, err := proto.Marshal(plan)
	require.NoError(t, err)
	return data
}

// Helper to build a PK TermExpr predicate for test plans.
func pkTermExpr(values ...int64) *planpb.Expr {
	gv := make([]*planpb.GenericValue, len(values))
	for i, v := range values {
		gv[i] = &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: v}}
	}
	return &planpb.Expr{
		Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Values:     gv,
			},
		},
	}
}

// Helper to build a PK Equal UnaryRangeExpr.
func pkEqualExpr(val int64) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: val}},
			},
		},
	}
}

// Helper to build a non-PK expression (age > 18 style).
func nonPkExpr() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 102, DataType: schemapb.DataType_Int64},
			},
		},
	}
}

// Helper to build binary expression (AND/OR).
func binaryExpr(op planpb.BinaryExpr_BinaryOp, left, right *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{Op: op, Left: left, Right: right},
		},
	}
}

// Helper to build NOT expression.
func notExpr(inner *planpb.Expr) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryExpr{
			UnaryExpr: &planpb.UnaryExpr{Op: planpb.UnaryExpr_Not, Child: inner},
		},
	}
}

func queryPlan(predicates *planpb.Expr) *planpb.PlanNode {
	return &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: predicates}}}
}

func vectorAnnsPlan(predicates *planpb.Expr) *planpb.PlanNode {
	return &planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{Predicates: predicates}}}
}

// extractPkValues is a test helper that calls extractOptimizablePkValues.
func extractPkValues(plan *planpb.PlanNode) ([]*planpb.GenericValue, []storage.PrimaryKey) {
	vals, pks, _ := extractOptimizablePkValues(plan)
	return vals, pks
}

func TestExtractOptimizablePkValues(t *testing.T) {
	t.Run("pk IN [1,2,3]", func(t *testing.T) {
		values, pks := extractPkValues(queryPlan(pkTermExpr(1, 2, 3)))
		assert.Len(t, values, 3)
		assert.Len(t, pks, 3)
		assert.Equal(t, int64(1), pks[0].GetValue())
		assert.Equal(t, int64(2), pks[1].GetValue())
		assert.Equal(t, int64(3), pks[2].GetValue())
	})

	t.Run("non-PK TermExpr returns nil", func(t *testing.T) {
		plan := queryPlan(&planpb.Expr{
			Expr: &planpb.Expr_TermExpr{
				TermExpr: &planpb.TermExpr{
					ColumnInfo: &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64, IsPrimaryKey: false},
					Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}},
				},
			},
		})
		values, pks := extractPkValues(plan)
		assert.Nil(t, values)
		assert.Nil(t, pks)
	})

	t.Run("VectorAnns with pk IN AND non-PK", func(t *testing.T) {
		plan := vectorAnnsPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, pkTermExpr(10, 20), nonPkExpr()))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 2)
		assert.Len(t, pks, 2)
		assert.Equal(t, int64(10), pks[0].GetValue())
		assert.Equal(t, int64(20), pks[1].GetValue())
	})

	t.Run("VarChar PK type", func(t *testing.T) {
		plan := queryPlan(&planpb.Expr{
			Expr: &planpb.Expr_TermExpr{
				TermExpr: &planpb.TermExpr{
					ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
					Values: []*planpb.GenericValue{
						{Val: &planpb.GenericValue_StringVal{StringVal: "pk1"}},
						{Val: &planpb.GenericValue_StringVal{StringVal: "pk2"}},
					},
				},
			},
		})
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 2)
		assert.Equal(t, "pk1", pks[0].GetValue())
		assert.Equal(t, "pk2", pks[1].GetValue())
	})

	t.Run("empty predicates returns nil", func(t *testing.T) {
		values, pks := extractPkValues(queryPlan(nil))
		assert.Nil(t, values)
		assert.Nil(t, pks)
	})

	// --- NEW: pk = value (Equal UnaryRangeExpr) ---
	t.Run("pk = 5 treated as pk IN [5]", func(t *testing.T) {
		values, pks := extractPkValues(queryPlan(pkEqualExpr(5)))
		assert.Len(t, values, 1)
		require.Len(t, pks, 1)
		assert.Equal(t, int64(5), pks[0].GetValue())
	})

	t.Run("pk = 5 AND age > 18", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, pkEqualExpr(5), nonPkExpr()))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 1)
		require.Len(t, pks, 1)
		assert.Equal(t, int64(5), pks[0].GetValue())
	})

	// --- NEW: AND intersection ---
	t.Run("pk IN [1,2,3] AND pk IN [2,3,4] → intersection [2,3]", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, pkTermExpr(1, 2, 3), pkTermExpr(2, 3, 4)))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 2)
		require.Len(t, pks, 2)
		assert.Equal(t, int64(2), pks[0].GetValue())
		assert.Equal(t, int64(3), pks[1].GetValue())
	})

	t.Run("pk IN [1] AND pk IN [2] → empty intersection → nil", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, pkTermExpr(1), pkTermExpr(2)))
		values, pks := extractPkValues(plan)
		assert.Nil(t, values)
		assert.Nil(t, pks)
	})

	t.Run("pk IN [1,2,3] AND pk = 2 → intersection [2]", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, pkTermExpr(1, 2, 3), pkEqualExpr(2)))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 1)
		require.Len(t, pks, 1)
		assert.Equal(t, int64(2), pks[0].GetValue())
	})

	// --- NEW: OR union ---
	t.Run("pk IN [1,2] OR pk IN [3,4] → union [1,2,3,4]", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalOr, pkTermExpr(1, 2), pkTermExpr(3, 4)))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 4)
		assert.Len(t, pks, 4)
	})

	t.Run("pk = 1 OR pk = 2 → union [1,2]", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalOr, pkEqualExpr(1), pkEqualExpr(2)))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 2)
		assert.Len(t, pks, 2)
	})

	t.Run("pk IN [1,2] OR pk IN [2,3] → union deduped [1,2,3]", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalOr, pkTermExpr(1, 2), pkTermExpr(2, 3)))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 3)
		assert.Len(t, pks, 3)
	})

	t.Run("pk IN [1,2] OR age > 18 → unconstrained → nil", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalOr, pkTermExpr(1, 2), nonPkExpr()))
		values, pks := extractPkValues(plan)
		assert.Nil(t, values)
		assert.Nil(t, pks)
	})

	// --- NEW: NOT passthrough ---
	t.Run("pk IN [1,2] AND NOT(status=1) still optimizes pk", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, pkTermExpr(1, 2), notExpr(nonPkExpr())))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 2)
		assert.Len(t, pks, 2)
	})

	t.Run("NOT(pk IN [1,2]) → no optimization", func(t *testing.T) {
		plan := queryPlan(notExpr(pkTermExpr(1, 2)))
		values, pks := extractPkValues(plan)
		assert.Nil(t, values)
		assert.Nil(t, pks)
	})

	t.Run("pk IN [1,2,3] AND NOT(pk IN [2]) → still optimizes on [1,2,3]", func(t *testing.T) {
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, pkTermExpr(1, 2, 3), notExpr(pkTermExpr(2))))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 3)
		assert.Len(t, pks, 3)
	})

	// --- Complex combinations ---
	t.Run("(pk IN [1,2] OR pk IN [3]) AND pk IN [2,3,4] → union then intersect → [2,3]", func(t *testing.T) {
		orExpr := binaryExpr(planpb.BinaryExpr_LogicalOr, pkTermExpr(1, 2), pkTermExpr(3))
		plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, orExpr, pkTermExpr(2, 3, 4)))
		values, pks := extractPkValues(plan)
		assert.Len(t, values, 2)
		require.Len(t, pks, 2)
		assert.Equal(t, int64(2), pks[0].GetValue())
		assert.Equal(t, int64(3), pks[1].GetValue())
	})
}

func TestFilterSegmentsByPartitions(t *testing.T) {
	bfs1 := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs2 := pkoracle.NewBloomFilterSet(2, 2, commonpb.SegmentState_Sealed)
	bfs3 := pkoracle.NewBloomFilterSet(3, 3, commonpb.SegmentState_Sealed)

	sealed := []SnapshotItem{
		{
			NodeID: 1,
			Segments: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1, Candidate: bfs1},
				{SegmentID: 2, PartitionID: 2, Candidate: bfs2},
				{SegmentID: 3, PartitionID: 3, Candidate: bfs3},
			},
		},
	}

	growing := []SegmentEntry{
		{SegmentID: 10, PartitionID: 1, Candidate: bfs1},
		{SegmentID: 11, PartitionID: 3, Candidate: bfs3},
	}

	filteredSealed, filteredGrowing := filterSegmentsByPartitions([]int64{1, 2}, sealed, growing)
	require.Len(t, filteredSealed, 1)
	require.Len(t, filteredSealed[0].Segments, 2)
	assert.Equal(t, int64(1), filteredSealed[0].Segments[0].SegmentID)
	assert.Equal(t, int64(2), filteredSealed[0].Segments[1].SegmentID)

	require.Len(t, filteredGrowing, 1)
	assert.Equal(t, int64(10), filteredGrowing[0].SegmentID)
}

func TestBuildSegmentHint(t *testing.T) {
	// Create mock bloom filter sets for segments
	// Segment 1: contains pk=1
	// Segment 2: contains pk=2
	// Segment 3: contains pk=3
	bfs1 := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs1.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})

	bfs2 := pkoracle.NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
	bfs2.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(2)})

	bfs3 := pkoracle.NewBloomFilterSet(3, 1, commonpb.SegmentState_Sealed)
	bfs3.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(3)})

	sealed := buildTestSealedSegments(map[int64]*pkoracle.BloomFilterSet{
		1: bfs1, 2: bfs2, 3: bfs3,
	}, 1)

	t.Run("Build hints with partial filtering", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									FieldId:      100,
									DataType:     schemapb.DataType_Int64,
									IsPrimaryKey: true,
								},
								Values: []*planpb.GenericValue{
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 3}},
								},
							},
						},
					},
				},
			},
		}

		segHint := buildSegmentHintFromPlan(plan, []int64{1}, sealed, nil)

		require.NotNil(t, segHint)
		// Each segment should only have one PK in hints (filtered from 3 to 1)
		assert.Len(t, segHint.hints, 3)
		assert.Empty(t, segHint.skippedSegments)

		// Verify hints content
		for _, hint := range segHint.hints {
			assert.Len(t, hint.FilteredPkValues, 1)
		}
	})

	t.Run("Build hints with segment to skip", func(t *testing.T) {
		// Query for pk=100, which doesn't exist in any segment
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									FieldId:      100,
									DataType:     schemapb.DataType_Int64,
									IsPrimaryKey: true,
								},
								Values: []*planpb.GenericValue{
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 100}},
								},
							},
						},
					},
				},
			},
		}

		segHint := buildSegmentHintFromPlan(plan, []int64{1}, sealed, nil)

		require.NotNil(t, segHint)
		assert.Empty(t, segHint.hints)
		// All segments should be skipped since pk=100 doesn't exist anywhere
		assert.Len(t, segHint.skippedSegments, 3)
	})

	t.Run("Build hints with no reduction still emits per-segment markers", func(t *testing.T) {
		// Build another set where every segment contains all queried PKs.
		allHit1 := pkoracle.NewBloomFilterSet(11, 1, commonpb.SegmentState_Sealed)
		allHit1.UpdateBloomFilter([]storage.PrimaryKey{
			storage.NewInt64PrimaryKey(1),
			storage.NewInt64PrimaryKey(2),
		})
		allHit2 := pkoracle.NewBloomFilterSet(12, 1, commonpb.SegmentState_Sealed)
		allHit2.UpdateBloomFilter([]storage.PrimaryKey{
			storage.NewInt64PrimaryKey(1),
			storage.NewInt64PrimaryKey(2),
		})
		allHitSealed := buildTestSealedSegments(map[int64]*pkoracle.BloomFilterSet{
			11: allHit1,
			12: allHit2,
		}, 1)

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									FieldId:      100,
									DataType:     schemapb.DataType_Int64,
									IsPrimaryKey: true,
								},
								Values: []*planpb.GenericValue{
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
								},
							},
						},
					},
				},
			},
		}

		segHint := buildSegmentHintFromPlan(plan, []int64{1}, allHitSealed, nil)

		require.NotNil(t, segHint)
		assert.Empty(t, segHint.skippedSegments)
		assert.Len(t, segHint.hints, 2)
		for _, hint := range segHint.hints {
			// Empty filtered values is a marker: worker should fallback to original TermExpr values.
			assert.Empty(t, hint.GetFilteredPkValues())
		}
	})

	t.Run("Build hints with missing BF result emits fallback marker", func(t *testing.T) {
		// Add segment 9999 without a Candidate to test fallback when BF result is missing.
		// BatchGetFromSegments skips entries with nil Candidate.
		sealedWithExtra := []SnapshotItem{{
			NodeID: 1,
			Segments: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1, Candidate: bfs1},
				{SegmentID: 9999, PartitionID: 1, Candidate: nil},
			},
		}}

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									FieldId:      100,
									DataType:     schemapb.DataType_Int64,
									IsPrimaryKey: true,
								},
								Values: []*planpb.GenericValue{
									{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
								},
							},
						},
					},
				},
			},
		}

		segHint := buildSegmentHintFromPlan(plan, []int64{1}, sealedWithExtra, nil)

		require.NotNil(t, segHint)
		assert.Empty(t, segHint.skippedSegments)
		assert.Len(t, segHint.hints, 2)
		for _, hint := range segHint.hints {
			assert.Empty(t, hint.GetFilteredPkValues())
		}
	})

	t.Run("No PK TermExpr in plan", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: &planpb.ColumnInfo{
									FieldId:      101,
									DataType:     schemapb.DataType_Int64,
									IsPrimaryKey: false,
								},
							},
						},
					},
				},
			},
		}

		segHint := buildSegmentHintFromPlan(plan, []int64{1}, sealed, nil)
		assert.Nil(t, segHint)
	})

	t.Run("FilterSegments removes skipped segments", func(t *testing.T) {
		hint := &SegmentHint{
			skippedSegments: map[int64]struct{}{2: {}, 3: {}},
		}
		filteredSealed, filteredGrowing := hint.FilterSegments(sealed, []SegmentEntry{
			{SegmentID: 10}, {SegmentID: 2},
		})
		// Only segment 1 should remain in sealed (2 and 3 skipped)
		totalSegs := 0
		for _, item := range filteredSealed {
			totalSegs += len(item.Segments)
		}
		assert.Equal(t, 1, totalSegs)
		// Only segment 10 should remain in growing (2 skipped)
		assert.Len(t, filteredGrowing, 1)
		assert.Equal(t, int64(10), filteredGrowing[0].SegmentID)
	})

	t.Run("GetHintsForSegments returns matching hints", func(t *testing.T) {
		hint := &SegmentHint{
			hints: map[int64]*planpb.SegmentPkHint{
				1: {SegmentId: 1, FilteredPkValues: []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}}},
				2: {SegmentId: 2},
			},
		}

		result := hint.GetHintsForSegments([]int64{1, 99})
		require.NotNil(t, result)
		assert.Len(t, result.Hints, 1)
		assert.Equal(t, int64(1), result.Hints[0].SegmentId)

		// No matching segments
		result = hint.GetHintsForSegments([]int64{99})
		assert.Nil(t, result)
	})
}

func TestBuildAndApplySegmentHintsModes(t *testing.T) {
	paramtable.Init()
	paramtable.Get().QueryNodeCfg.EnableSegmentPkHint.SwapTempValue("true")

	bfs := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})
	sealed := buildTestSealedSegments(map[int64]*pkoracle.BloomFilterSet{
		1: bfs,
	}, 1)

	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				Predicates: &planpb.Expr{
					Expr: &planpb.Expr_TermExpr{
						TermExpr: &planpb.TermExpr{
							ColumnInfo: &planpb.ColumnInfo{
								FieldId:      100,
								DataType:     schemapb.DataType_Int64,
								IsPrimaryKey: true,
							},
							Values: []*planpb.GenericValue{
								{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
							},
						},
					},
				},
			},
		},
	}
	serializedPlan := marshalPlan(t, plan)

	t.Run("hints mode", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")
		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			serializedPlan,
			common.PkFilterHintHasPkFilter,
			[]int64{1},
			sealed,
			nil,
			100,
			metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
		assert.Len(t, filteredSealed, 1)
		assert.Len(t, filteredGrowing, 0)
	})

	t.Run("segment mode", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("segment")
		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			serializedPlan,
			common.PkFilterHintHasPkFilter,
			[]int64{1},
			sealed,
			nil,
			100,
			metrics.QueryLabel,
		)
		assert.Nil(t, segHint)
		assert.Len(t, filteredSealed, 1)
		assert.Len(t, filteredGrowing, 0)
	})

	t.Run("hints mode also prunes unary range predicates", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

		rangePlan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: &planpb.ColumnInfo{
									FieldId:      100,
									DataType:     schemapb.DataType_Int64,
									IsPrimaryKey: true,
								},
								Op: planpb.OpType_LessThan,
								Value: &planpb.GenericValue{
									Val: &planpb.GenericValue_Int64Val{Int64Val: 5},
								},
							},
						},
					},
				},
			},
		}
		rangeSealed := buildTestSealedSegments(map[int64]*pkoracle.BloomFilterSet{
			1: bfs,
			2: func() *pkoracle.BloomFilterSet {
				b := pkoracle.NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
				b.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(10)})
				return b
			}(),
		}, 1)

		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			marshalPlan(t, rangePlan),
			common.PkFilterHintHasPkFilter,
			[]int64{1},
			rangeSealed,
			nil,
			100,
			metrics.QueryLabel,
		)

		assert.Nil(t, segHint)
		assert.Len(t, filteredGrowing, 0)
		require.Len(t, filteredSealed, 1)
		require.Len(t, filteredSealed[0].Segments, 1)
		assert.Equal(t, int64(1), filteredSealed[0].Segments[0].SegmentID)
	})

	t.Run("hints mode optimizes OR of PK terms via union", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

		// pk IN [1] OR pk IN [99] → union → pk IN [1, 99]
		// seg1 has pk=1, seg2 has pk=2
		// BF: seg1=[hit(1), miss(99)], seg2=[miss(1), miss(99)]
		// → seg2 skipped (all miss), seg1 gets filtered hint [pk=1]
		orPlan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalOr, pkTermExpr(1), pkTermExpr(99)))
		orSealed := buildTestSealedSegments(map[int64]*pkoracle.BloomFilterSet{
			1: bfs,
			2: func() *pkoracle.BloomFilterSet {
				b := pkoracle.NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
				b.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(2)})
				return b
			}(),
		}, 1)

		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			marshalPlan(t, orPlan),
			common.PkFilterHintHasPkFilter,
			[]int64{1},
			orSealed,
			nil,
			100,
			metrics.QueryLabel,
		)

		// OR of PK terms is now optimized: seg2 skipped, seg1 has hint.
		require.NotNil(t, segHint)
		assert.Len(t, filteredGrowing, 0)
		require.Len(t, filteredSealed, 1)
		require.Len(t, filteredSealed[0].Segments, 1)
		assert.Equal(t, int64(1), filteredSealed[0].Segments[0].SegmentID)
	})

	t.Run("OR with non-PK side is not optimizable", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

		// pk IN [1] OR age > 18 → unconstrained → no optimization
		orPlan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalOr, pkTermExpr(1), nonPkExpr()))
		orSealed := buildTestSealedSegments(map[int64]*pkoracle.BloomFilterSet{1: bfs}, 1)

		segHint, filteredSealed, _ := buildAndApplySegmentHints(
			marshalPlan(t, orPlan),
			common.PkFilterHintHasPkFilter,
			[]int64{1},
			orSealed,
			nil,
			100,
			metrics.QueryLabel,
		)

		assert.Nil(t, segHint)
		require.Len(t, filteredSealed, 1)
		require.Len(t, filteredSealed[0].Segments, 1) // no pruning
	})

	t.Run("disabled hint skips everything", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.EnableSegmentPkHint.SwapTempValue("false")
		defer paramtable.Get().QueryNodeCfg.EnableSegmentPkHint.SwapTempValue("true")

		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			serializedPlan, common.PkFilterHintHasPkFilter, []int64{1}, sealed, nil, 100, metrics.QueryLabel,
		)
		assert.Nil(t, segHint)
		assert.Len(t, filteredSealed, 1)
		assert.Len(t, filteredGrowing, 0)
	})

	t.Run("pkFilterHint=NoPkFilter skips unmarshal", func(t *testing.T) {
		segHint, filteredSealed, _ := buildAndApplySegmentHints(
			serializedPlan, common.PkFilterHintNoPkFilter, []int64{1}, sealed, nil, 100, metrics.QueryLabel,
		)
		assert.Nil(t, segHint)
		assert.Len(t, filteredSealed, 1)
	})

	t.Run("invalid plan bytes returns gracefully", func(t *testing.T) {
		segHint, filteredSealed, _ := buildAndApplySegmentHints(
			[]byte("invalid-proto"), common.PkFilterHintHasPkFilter, []int64{1}, sealed, nil, 100, metrics.QueryLabel,
		)
		assert.Nil(t, segHint)
		assert.Len(t, filteredSealed, 1)
	})

	t.Run("invalid mode falls back to hints", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("bogus_mode")
		defer paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

		segHint, _, _ := buildAndApplySegmentHints(
			serializedPlan, common.PkFilterHintHasPkFilter, []int64{1}, sealed, nil, 100, metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
	})

	t.Run("empty string mode defaults to hints", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("")
		defer paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

		segHint, _, _ := buildAndApplySegmentHints(
			serializedPlan, common.PkFilterHintHasPkFilter, []int64{1}, sealed, nil, 100, metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
	})

	t.Run("segment mode with no optimizable plan returns false", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("segment")
		defer paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

		// Non-PK predicate → no segHint, no skipped
		noOptPlan := queryPlan(nonPkExpr())
		segHint, _, _ := buildAndApplySegmentHints(
			marshalPlan(t, noOptPlan), common.PkFilterHintHasPkFilter, []int64{1}, sealed, nil, 100, metrics.QueryLabel,
		)
		assert.Nil(t, segHint)
	})
}

// --- Additional coverage tests ---

func TestCollectUniqueSegmentIDsDedup(t *testing.T) {
	// Same segment ID in sealed and growing should be deduped.
	bfs := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{
		{SegmentID: 1, Candidate: bfs},
		{SegmentID: 1, Candidate: bfs}, // duplicate in sealed
	}}}
	growing := []SegmentEntry{
		{SegmentID: 1}, // duplicate across sealed and growing
		{SegmentID: 2},
	}
	ids := collectUniqueSegmentIDs(sealed, growing)
	assert.Len(t, ids, 2)
	assert.Contains(t, ids, int64(1))
	assert.Contains(t, ids, int64(2))
}

func TestExtractPlanPredicates(t *testing.T) {
	t.Run("nil plan", func(t *testing.T) {
		assert.Nil(t, extractPlanPredicates(nil))
	})
	t.Run("plan with no Query or VectorAnns", func(t *testing.T) {
		assert.Nil(t, extractPlanPredicates(&planpb.PlanNode{}))
	})
	t.Run("VectorAnns plan", func(t *testing.T) {
		pred := pkTermExpr(1)
		plan := vectorAnnsPlan(pred)
		result := extractPlanPredicates(plan)
		assert.NotNil(t, result)
	})
}

func TestEvalExprWithCandidate(t *testing.T) {
	// Build a candidate with known min/max
	bfs := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(10)})

	t.Run("nil expr returns true", func(t *testing.T) {
		assert.True(t, evalExprWithCandidate(nil, bfs))
	})

	t.Run("nil candidate returns true", func(t *testing.T) {
		assert.True(t, evalExprWithCandidate(pkTermExpr(1), nil))
	})

	t.Run("BinaryExpr OR returns true (conservative)", func(t *testing.T) {
		expr := binaryExpr(planpb.BinaryExpr_LogicalOr, pkTermExpr(999), pkTermExpr(999))
		assert.True(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("BinaryExpr AND propagates", func(t *testing.T) {
		// pk IN [10] AND pk IN [10] → both true for this candidate (min=max=10)
		expr := binaryExpr(planpb.BinaryExpr_LogicalAnd, pkTermExpr(10), pkTermExpr(10))
		assert.True(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("UnaryRangeExpr non-PK returns true", func(t *testing.T) {
		assert.True(t, evalExprWithCandidate(nonPkExpr(), bfs))
	})

	t.Run("UnaryRangeExpr PK GreaterThan", func(t *testing.T) {
		// min=max=10; pk > 100 → max(10) <= 100 → skip
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 100}},
			},
		}}
		assert.False(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("UnaryRangeExpr PK LessThan", func(t *testing.T) {
		// min=max=10; pk < 5 → min(10) >= 5 → skip
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_LessThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 5}},
			},
		}}
		assert.False(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("UnaryRangeExpr PK GreaterEqual in range", func(t *testing.T) {
		// min=max=10; pk >= 10 → max(10) >= 10 → keep
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_GreaterEqual,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		}}
		assert.True(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("UnaryRangeExpr PK LessEqual in range", func(t *testing.T) {
		// min=max=10; pk <= 10 → min(10) <= 10 → keep
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_LessEqual,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 10}},
			},
		}}
		assert.True(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("TermExpr with nil values returns false", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Values:     nil,
			},
		}}
		assert.False(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("TermExpr PK IN [10] hits min/max", func(t *testing.T) {
		assert.True(t, evalExprWithCandidate(pkTermExpr(10), bfs))
	})

	t.Run("TermExpr PK IN [999] misses min/max", func(t *testing.T) {
		assert.False(t, evalExprWithCandidate(pkTermExpr(999), bfs))
	})

	t.Run("default expr type returns true", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_BinaryArithExpr{
			BinaryArithExpr: &planpb.BinaryArithExpr{},
		}}
		assert.True(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("UnaryRangeExpr with nil ColumnInfo returns true", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{ColumnInfo: nil},
		}}
		assert.True(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("UnaryRangeExpr unsupported PK type returns true", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Float, IsPrimaryKey: true},
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			},
		}}
		assert.True(t, evalExprWithCandidate(expr, bfs))
	})

	t.Run("VarChar PK TermExpr evalExprWithCandidate", func(t *testing.T) {
		vcBfs := pkoracle.NewBloomFilterSet(10, 1, commonpb.SegmentState_Sealed)
		vcBfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewVarCharPrimaryKey("hello")})
		expr := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
				Values: []*planpb.GenericValue{
					{Val: &planpb.GenericValue_StringVal{StringVal: "hello"}},
				},
			},
		}}
		assert.True(t, evalExprWithCandidate(expr, vcBfs))
	})

	t.Run("TermExpr non-PK returns true", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 200, DataType: schemapb.DataType_Int64, IsPrimaryKey: false},
				Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}},
			},
		}}
		assert.True(t, evalExprWithCandidate(expr, bfs))
	})
}

func TestMayMatchByMinMax(t *testing.T) {
	minPk := storage.NewInt64PrimaryKey(10)
	maxPk := storage.NewInt64PrimaryKey(20)

	t.Run("Equal in range", func(t *testing.T) {
		assert.True(t, mayMatchByMinMax(planpb.OpType_Equal, storage.NewInt64PrimaryKey(15), minPk, maxPk, true))
	})
	t.Run("Equal below min", func(t *testing.T) {
		assert.False(t, mayMatchByMinMax(planpb.OpType_Equal, storage.NewInt64PrimaryKey(5), minPk, maxPk, true))
	})
	t.Run("Equal above max", func(t *testing.T) {
		assert.False(t, mayMatchByMinMax(planpb.OpType_Equal, storage.NewInt64PrimaryKey(25), minPk, maxPk, true))
	})
	t.Run("GreaterThan below max", func(t *testing.T) {
		assert.True(t, mayMatchByMinMax(planpb.OpType_GreaterThan, storage.NewInt64PrimaryKey(15), minPk, maxPk, true))
	})
	t.Run("GreaterThan at max", func(t *testing.T) {
		// pk > 20, max=20 → max LE pk → false
		assert.False(t, mayMatchByMinMax(planpb.OpType_GreaterThan, storage.NewInt64PrimaryKey(20), minPk, maxPk, true))
	})
	t.Run("GreaterEqual at max", func(t *testing.T) {
		assert.True(t, mayMatchByMinMax(planpb.OpType_GreaterEqual, storage.NewInt64PrimaryKey(20), minPk, maxPk, true))
	})
	t.Run("GreaterEqual above max", func(t *testing.T) {
		assert.False(t, mayMatchByMinMax(planpb.OpType_GreaterEqual, storage.NewInt64PrimaryKey(25), minPk, maxPk, true))
	})
	t.Run("LessThan above min", func(t *testing.T) {
		assert.True(t, mayMatchByMinMax(planpb.OpType_LessThan, storage.NewInt64PrimaryKey(15), minPk, maxPk, true))
	})
	t.Run("LessThan at min", func(t *testing.T) {
		// pk < 10, min=10 → min GE pk → false
		assert.False(t, mayMatchByMinMax(planpb.OpType_LessThan, storage.NewInt64PrimaryKey(10), minPk, maxPk, true))
	})
	t.Run("LessEqual at min", func(t *testing.T) {
		assert.True(t, mayMatchByMinMax(planpb.OpType_LessEqual, storage.NewInt64PrimaryKey(10), minPk, maxPk, true))
	})
	t.Run("LessEqual below min", func(t *testing.T) {
		assert.False(t, mayMatchByMinMax(planpb.OpType_LessEqual, storage.NewInt64PrimaryKey(5), minPk, maxPk, true))
	})
	t.Run("unknown op returns true", func(t *testing.T) {
		assert.True(t, mayMatchByMinMax(planpb.OpType_NotEqual, storage.NewInt64PrimaryKey(15), minPk, maxPk, true))
	})
	t.Run("no min/max always returns true", func(t *testing.T) {
		assert.True(t, mayMatchByMinMax(planpb.OpType_Equal, storage.NewInt64PrimaryKey(999), nil, nil, false))
	})
}

func TestCandidateMinMax(t *testing.T) {
	t.Run("candidateMinMaxProvider path (baseSegment style)", func(t *testing.T) {
		// BloomFilterSet also implements GetMinPk/GetMaxPk via candidateMinMaxProvider
		bfs := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(42)})
		minPk, maxPk, ok := candidateMinMax(bfs)
		assert.True(t, ok)
		assert.Equal(t, int64(42), minPk.GetValue())
		assert.Equal(t, int64(42), maxPk.GetValue())
	})

	t.Run("candidate with no min/max", func(t *testing.T) {
		// New BFS without any update has nil min/max
		bfs := pkoracle.NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
		_, _, ok := candidateMinMax(bfs)
		assert.False(t, ok)
	})
}

func TestBuildSkippedSegmentsByPredicates(t *testing.T) {
	bfs := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(10)})

	bfs2 := pkoracle.NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
	bfs2.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(50)})

	sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{
		{SegmentID: 1, PartitionID: 1, Candidate: bfs},
		{SegmentID: 2, PartitionID: 1, Candidate: bfs2},
	}}}

	t.Run("nil predicates returns nil", func(t *testing.T) {
		assert.Nil(t, buildSkippedSegmentsByPredicates(nil, nil, sealed, nil))
	})

	t.Run("pk > 100 skips all segments", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 100}},
			},
		}}
		skipped := buildSkippedSegmentsByPredicates(expr, []int64{1}, sealed, nil)
		assert.Len(t, skipped, 2)
	})

	t.Run("growing segments are also evaluated", func(t *testing.T) {
		growBfs := pkoracle.NewBloomFilterSet(3, 1, commonpb.SegmentState_Growing)
		growBfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(5)})
		growing := []SegmentEntry{{SegmentID: 3, PartitionID: 1, Candidate: growBfs}}

		// pk > 100 → all miss
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 100}},
			},
		}}
		skipped := buildSkippedSegmentsByPredicates(expr, []int64{1}, sealed, growing)
		assert.Contains(t, skipped, int64(3))
	})

	t.Run("offline segment is skipped in evaluation", func(t *testing.T) {
		offlineSealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{
			{SegmentID: 99, PartitionID: 1, Candidate: bfs, Offline: true},
		}}}
		// pk > 100 → should skip but offline is ignored
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 100}},
			},
		}}
		skipped := buildSkippedSegmentsByPredicates(expr, []int64{1}, offlineSealed, nil)
		assert.Nil(t, skipped) // offline segments aren't evaluated
	})

	t.Run("nil candidate segment is skipped in evaluation", func(t *testing.T) {
		nilCandSealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{
			{SegmentID: 88, PartitionID: 1, Candidate: nil},
		}}}
		expr := pkTermExpr(999)
		skipped := buildSkippedSegmentsByPredicates(expr, []int64{1}, nilCandSealed, nil)
		assert.Nil(t, skipped)
	})

	t.Run("partition mismatch segment is skipped", func(t *testing.T) {
		// Single partition filter → segments in other partitions are skipped
		expr := pkTermExpr(999)
		skipped := buildSkippedSegmentsByPredicates(expr, []int64{99}, sealed, nil)
		assert.Nil(t, skipped) // partition 1 != 99, entries skipped, nothing evaluated
	})

	t.Run("duplicate segment IDs evaluated once", func(t *testing.T) {
		dupSealed := []SnapshotItem{
			{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 1, PartitionID: 1, Candidate: bfs}}},
			{NodeID: 2, Segments: []SegmentEntry{{SegmentID: 1, PartitionID: 1, Candidate: bfs}}},
		}
		// pk > 100 → skip seg1
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 100}},
			},
		}}
		skipped := buildSkippedSegmentsByPredicates(expr, nil, dupSealed, nil)
		assert.Len(t, skipped, 1) // only counted once
	})

	t.Run("no skipped returns nil", func(t *testing.T) {
		// pk > 5 → seg1(min=10) passes, seg2(min=50) passes
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 5}},
			},
		}}
		skipped := buildSkippedSegmentsByPredicates(expr, nil, sealed, nil)
		assert.Nil(t, skipped)
	})
}

func TestExtractPkConstraintEdgeCases(t *testing.T) {
	t.Run("non-Equal range on PK returns nil", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_GreaterThan,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 5}},
			},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})

	t.Run("Equal on non-PK returns nil", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 200, DataType: schemapb.DataType_Int64, IsPrimaryKey: false},
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 5}},
			},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})

	t.Run("Equal on unsupported PK datatype returns nil", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Float, IsPrimaryKey: true},
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 5}},
			},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})

	t.Run("Equal with nil value returns nil", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Op:         planpb.OpType_Equal,
				Value:      nil,
			},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})

	t.Run("TermExpr with unsupported PK datatype returns nil", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Float, IsPrimaryKey: true},
				Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}},
			},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})

	t.Run("TermExpr with empty values returns nil", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				Values:     []*planpb.GenericValue{},
			},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})

	t.Run("TermExpr with nil ColumnInfo returns nil", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{ColumnInfo: nil, Values: []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}}},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})

	t.Run("VarChar pk = value", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: "abc"}},
			},
		}}
		c := extractPkConstraint(expr)
		require.NotNil(t, c)
		assert.Len(t, c.values, 1)
		assert.Equal(t, schemapb.DataType_VarChar, c.dataType)
	})

	t.Run("VarChar pk IN OR union", func(t *testing.T) {
		left := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
				Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_StringVal{StringVal: "a"}}},
			},
		}}
		right := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
				Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_StringVal{StringVal: "b"}}},
			},
		}}
		expr := binaryExpr(planpb.BinaryExpr_LogicalOr, left, right)
		c := extractPkConstraint(expr)
		require.NotNil(t, c)
		assert.Len(t, c.values, 2)
	})

	t.Run("VarChar pk IN AND intersection with overlap", func(t *testing.T) {
		left := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
				Values: []*planpb.GenericValue{
					{Val: &planpb.GenericValue_StringVal{StringVal: "a"}},
					{Val: &planpb.GenericValue_StringVal{StringVal: "b"}},
				},
			},
		}}
		right := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
			TermExpr: &planpb.TermExpr{
				ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
				Values: []*planpb.GenericValue{
					{Val: &planpb.GenericValue_StringVal{StringVal: "b"}},
					{Val: &planpb.GenericValue_StringVal{StringVal: "c"}},
				},
			},
		}}
		expr := binaryExpr(planpb.BinaryExpr_LogicalAnd, left, right)
		c := extractPkConstraint(expr)
		require.NotNil(t, c)
		assert.Len(t, c.values, 1) // only "b"
		assert.Equal(t, "b", c.values[0].GetStringVal())
	})

	t.Run("BinaryExpr with non-AND non-OR op returns nil", func(t *testing.T) {
		// Use an unexpected BinaryExpr op value
		expr := &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{
				Op:   100, // invalid op
				Left: pkTermExpr(1),
			},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})

	t.Run("nil expr returns nil", func(t *testing.T) {
		assert.Nil(t, extractPkConstraint(nil))
	})

	t.Run("Equal with nil ColumnInfo returns nil", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: nil,
				Op:         planpb.OpType_Equal,
				Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			},
		}}
		assert.Nil(t, extractPkConstraint(expr))
	})
}

func TestPkValueKey(t *testing.T) {
	t.Run("Int64", func(t *testing.T) {
		v := &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 42}}
		assert.Equal(t, "42", pkValueKey(schemapb.DataType_Int64, v))
	})
	t.Run("VarChar", func(t *testing.T) {
		v := &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: "hello"}}
		assert.Equal(t, "hello", pkValueKey(schemapb.DataType_VarChar, v))
	})
	t.Run("unsupported type returns empty", func(t *testing.T) {
		v := &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}
		assert.Equal(t, "", pkValueKey(schemapb.DataType_Float, v))
	})
}

func TestConvertGenericValuesToPKs(t *testing.T) {
	t.Run("Int64", func(t *testing.T) {
		values := []*planpb.GenericValue{
			{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}},
		}
		pks := convertGenericValuesToPKs(schemapb.DataType_Int64, values)
		assert.Len(t, pks, 2)
	})
	t.Run("VarChar", func(t *testing.T) {
		values := []*planpb.GenericValue{
			{Val: &planpb.GenericValue_StringVal{StringVal: "a"}},
		}
		pks := convertGenericValuesToPKs(schemapb.DataType_VarChar, values)
		assert.Len(t, pks, 1)
	})
	t.Run("unsupported returns nil", func(t *testing.T) {
		values := []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}}
		assert.Nil(t, convertGenericValuesToPKs(schemapb.DataType_Float, values))
	})
}

func TestGetHintsForSegmentsEmptyHints(t *testing.T) {
	h := &SegmentHint{hints: map[int64]*planpb.SegmentPkHint{}}
	assert.Nil(t, h.GetHintsForSegments([]int64{1, 2}))
}

func TestFilterSegmentsByPartitionsEdgeCases(t *testing.T) {
	sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{
		{SegmentID: 1, PartitionID: 1},
		{SegmentID: 2, PartitionID: 2},
	}}}
	growing := []SegmentEntry{{SegmentID: 3, PartitionID: 1}}

	t.Run("empty partitionIDs returns all", func(t *testing.T) {
		s, g := filterSegmentsByPartitions(nil, sealed, growing)
		assert.Len(t, s[0].Segments, 2)
		assert.Len(t, g, 1)
	})

	t.Run("all partitions sentinel returns all", func(t *testing.T) {
		s, g := filterSegmentsByPartitions([]int64{common.AllPartitionsID}, sealed, growing)
		assert.Len(t, s[0].Segments, 2)
		assert.Len(t, g, 1)
	})

	t.Run("no matching partition returns empty", func(t *testing.T) {
		s, g := filterSegmentsByPartitions([]int64{99}, sealed, growing)
		assert.Empty(t, s)
		assert.Empty(t, g)
	})
}

func TestBuildSegmentHintWithGrowingSegments(t *testing.T) {
	// Test that growing segments are properly included in hint building.
	bfs := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})
	sealed := buildTestSealedSegments(map[int64]*pkoracle.BloomFilterSet{1: bfs}, 1)

	growBfs := pkoracle.NewBloomFilterSet(2, 1, commonpb.SegmentState_Growing)
	growBfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(2)})
	growing := []SegmentEntry{{SegmentID: 2, PartitionID: 1, Candidate: growBfs}}

	plan := queryPlan(pkTermExpr(1, 2))
	segHint := buildSegmentHintFromPlan(plan, []int64{1}, sealed, growing)

	require.NotNil(t, segHint)
	// seg1 has pk=1 (filtered), seg2 has pk=2 (filtered)
	assert.Len(t, segHint.hints, 2)
	assert.Empty(t, segHint.skippedSegments)
}

func TestBuildSegmentHintEmptySegments(t *testing.T) {
	// empty sealed+growing → segmentIDs=0 → nil
	plan := queryPlan(pkTermExpr(1))
	result := buildSegmentHintFromPlan(plan, []int64{1}, nil, nil)
	assert.Nil(t, result)
}

func TestBuildSegmentHintAllNilCandidates(t *testing.T) {
	// all candidates nil → bfResults empty → nil
	sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{
		{SegmentID: 1, PartitionID: 1, Candidate: nil},
		{SegmentID: 2, PartitionID: 1, Candidate: nil},
	}}}
	plan := queryPlan(pkTermExpr(1))
	result := buildSegmentHintFromPlan(plan, []int64{1}, sealed, nil)
	assert.Nil(t, result)
}

func TestEvalExprTermExprUnsupportedPkType(t *testing.T) {
	// L317-319: TermExpr PK with unsupported data type → genericValueToPrimaryKey fails → return true
	bfs := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})

	expr := &planpb.Expr{Expr: &planpb.Expr_TermExpr{
		TermExpr: &planpb.TermExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Float, IsPrimaryKey: true},
			Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}},
		},
	}}
	assert.True(t, evalExprWithCandidate(expr, bfs))
}

// mockMinMaxCandidate is a minimal Candidate that implements candidateMinMaxProvider
// but NOT candidateStatsProvider, to test the first branch of candidateMinMax.
type mockMinMaxCandidate struct {
	id        int64
	partition int64
	minPk     *storage.PrimaryKey
	maxPk     *storage.PrimaryKey
}

func (m *mockMinMaxCandidate) MayPkExist(_ *storage.LocationsCache) bool          { return true }
func (m *mockMinMaxCandidate) BatchPkExist(_ *storage.BatchLocationsCache) []bool { return nil }
func (m *mockMinMaxCandidate) ID() int64                                          { return m.id }
func (m *mockMinMaxCandidate) Partition() int64                                   { return m.partition }
func (m *mockMinMaxCandidate) Type() commonpb.SegmentState                        { return commonpb.SegmentState_Sealed }
func (m *mockMinMaxCandidate) GetMinPk() *storage.PrimaryKey                      { return m.minPk }
func (m *mockMinMaxCandidate) GetMaxPk() *storage.PrimaryKey                      { return m.maxPk }

func TestCandidateMinMaxProviderPath(t *testing.T) {
	// L349-354: candidateMinMaxProvider path (GetMinPk/GetMaxPk)
	var minPk storage.PrimaryKey = storage.NewInt64PrimaryKey(10)
	var maxPk storage.PrimaryKey = storage.NewInt64PrimaryKey(20)
	mock := &mockMinMaxCandidate{id: 1, partition: 1, minPk: &minPk, maxPk: &maxPk}

	min, max, ok := candidateMinMax(mock)
	assert.True(t, ok)
	assert.Equal(t, int64(10), min.GetValue())
	assert.Equal(t, int64(20), max.GetValue())
}

func TestCandidateMinMaxProviderNilPk(t *testing.T) {
	// L349-354: candidateMinMaxProvider with nil min/max → falls through
	mock := &mockMinMaxCandidate{id: 1, partition: 1, minPk: nil, maxPk: nil}
	_, _, ok := candidateMinMax(mock)
	// Should fall through to candidateStatsProvider (not implemented) → false
	assert.False(t, ok)
}

func TestExtractPkConstraintDefaultExprType(t *testing.T) {
	// L547-548: default case in extractPkConstraint switch
	expr := &planpb.Expr{Expr: &planpb.Expr_BinaryArithExpr{
		BinaryArithExpr: &planpb.BinaryArithExpr{},
	}}
	assert.Nil(t, extractPkConstraint(expr))
}

func TestIntersectPkConstraintsNilLeft(t *testing.T) {
	// L559-561: intersect(nil, Some) → returns right
	// This is the "age > 18 AND pk IN [1,2]" pattern where left is non-PK.
	plan := queryPlan(binaryExpr(planpb.BinaryExpr_LogicalAnd, nonPkExpr(), pkTermExpr(1, 2)))
	values, pks, _ := extractOptimizablePkValues(plan)
	assert.Len(t, values, 2)
	assert.Len(t, pks, 2)
}

func TestBuildSegmentHintWithPkEqual(t *testing.T) {
	// pk = 1 should trigger hint optimization (new feature).
	bfs := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})
	bfs2 := pkoracle.NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
	bfs2.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(2)})
	sealed := buildTestSealedSegments(map[int64]*pkoracle.BloomFilterSet{1: bfs, 2: bfs2}, 1)

	plan := queryPlan(pkEqualExpr(1))
	segHint := buildSegmentHintFromPlan(plan, []int64{1}, sealed, nil)

	require.NotNil(t, segHint)
	// seg1 hits pk=1 (allHit → empty hint), seg2 misses → skipped
	assert.Contains(t, segHint.skippedSegments, int64(2))
}

// ==================== Partition Filtering Integration Tests ====================

func TestPartitionFilteringIntegration(t *testing.T) {
	paramtable.Init()
	paramtable.Get().QueryNodeCfg.EnableSegmentPkHint.SwapTempValue("true")
	paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

	// Setup: 3 segments across 3 partitions, each contains one PK.
	// seg1(partition=1, pk=1), seg2(partition=2, pk=2), seg3(partition=3, pk=3)
	bfs1 := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs1.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})
	bfs2 := pkoracle.NewBloomFilterSet(2, 2, commonpb.SegmentState_Sealed)
	bfs2.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(2)})
	bfs3 := pkoracle.NewBloomFilterSet(3, 3, commonpb.SegmentState_Sealed)
	bfs3.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(3)})

	sealed := []SnapshotItem{{
		NodeID: 1,
		Segments: []SegmentEntry{
			{SegmentID: 1, PartitionID: 1, Candidate: bfs1},
			{SegmentID: 2, PartitionID: 2, Candidate: bfs2},
			{SegmentID: 3, PartitionID: 3, Candidate: bfs3},
		},
	}}

	// Growing segments across partitions.
	growBfs1 := pkoracle.NewBloomFilterSet(10, 1, commonpb.SegmentState_Growing)
	growBfs1.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})
	growBfs3 := pkoracle.NewBloomFilterSet(11, 3, commonpb.SegmentState_Growing)
	growBfs3.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(3)})
	growing := []SegmentEntry{
		{SegmentID: 10, PartitionID: 1, Candidate: growBfs1},
		{SegmentID: 11, PartitionID: 3, Candidate: growBfs3},
	}

	// Query: pk IN [1, 2, 3]
	plan := queryPlan(pkTermExpr(1, 2, 3))
	serializedPlan := marshalPlan(t, plan)

	t.Run("single partition filters out other partitions before hint", func(t *testing.T) {
		// Query partition=1 only → only seg1 and growing seg10 survive.
		// BF: seg1 hits pk=1, growing seg10 hits pk=1; pk=2,3 miss → filtered.
		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			serializedPlan, common.PkFilterHintHasPkFilter, []int64{1}, sealed, growing, 100, metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
		// Only partition=1 segments remain.
		totalSealed := 0
		for _, item := range filteredSealed {
			totalSealed += len(item.Segments)
		}
		assert.Equal(t, 1, totalSealed, "only seg1 should survive")
		assert.Len(t, filteredGrowing, 1, "only growing seg10 should survive")
		assert.Equal(t, int64(10), filteredGrowing[0].SegmentID)
	})

	t.Run("multi partition keeps matching segments", func(t *testing.T) {
		// Query partition=[1,2] → seg1,seg2 and growing seg10 survive.
		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			serializedPlan, common.PkFilterHintHasPkFilter, []int64{1, 2}, sealed, growing, 100, metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
		totalSealed := 0
		for _, item := range filteredSealed {
			totalSealed += len(item.Segments)
		}
		assert.Equal(t, 2, totalSealed, "seg1 and seg2 should survive")
		assert.Len(t, filteredGrowing, 1, "only growing seg10 (partition=1) survives")
	})

	t.Run("empty partitionIDs keeps all segments", func(t *testing.T) {
		// nil/empty partitionIDs = AllPartitionsID → no partition filtering.
		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			serializedPlan, common.PkFilterHintHasPkFilter, nil, sealed, growing, 100, metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
		totalSealed := 0
		for _, item := range filteredSealed {
			totalSealed += len(item.Segments)
		}
		assert.Equal(t, 3, totalSealed, "all 3 sealed segments should survive")
		assert.Len(t, filteredGrowing, 2, "both growing segments should survive")
	})

	t.Run("non-matching partition returns no hint", func(t *testing.T) {
		// Query partition=99 → no segments match → no hint.
		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			serializedPlan, common.PkFilterHintHasPkFilter, []int64{99}, sealed, growing, 100, metrics.QueryLabel,
		)
		assert.Nil(t, segHint, "no segments left after partition filter → nil hint")
		assert.Empty(t, filteredSealed)
		assert.Empty(t, filteredGrowing)
	})

	t.Run("partition filter then BF hint prunes further", func(t *testing.T) {
		// Query: pk IN [1, 99] with partition=1 → only seg1 + growing seg10 after partition filter.
		// BF: seg1 hits pk=1, misses pk=99 → hint with pk=1.
		//     growing seg10 hits pk=1, misses pk=99 → hint with pk=1.
		planSubset := queryPlan(pkTermExpr(1, 99))
		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			marshalPlan(t, planSubset), common.PkFilterHintHasPkFilter, []int64{1}, sealed, growing, 100, metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
		// seg1 hit pk=1 → kept with hint; growing seg10 hit pk=1 → kept.
		totalSealed := 0
		for _, item := range filteredSealed {
			totalSealed += len(item.Segments)
		}
		assert.Equal(t, 1, totalSealed)
		assert.Len(t, filteredGrowing, 1)
		// Verify hint for seg1 contains filtered pk=1 (not pk=99).
		hint1 := segHint.hints[1]
		require.NotNil(t, hint1)
		if len(hint1.GetFilteredPkValues()) > 0 {
			assert.Equal(t, int64(1), hint1.GetFilteredPkValues()[0].GetInt64Val())
		}
	})

	t.Run("partition filter with segment mode prunes correctly", func(t *testing.T) {
		paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("segment")
		defer paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

		// Segment mode: partition filter still applies, then segment-level pruning.
		// Query pk IN [1] with partition=[2] → seg2 survives partition filter, but pk=1 not in seg2 → skipped.
		planOne := queryPlan(pkTermExpr(1))
		segHint, filteredSealed, filteredGrowing := buildAndApplySegmentHints(
			marshalPlan(t, planOne), common.PkFilterHintHasPkFilter, []int64{2}, sealed, growing, 100, metrics.QueryLabel,
		)
		assert.Nil(t, segHint, "segment mode nullifies hint")
		// After partition filter: only seg2 (partition=2). pk=1 not in seg2 → skipped.
		totalSealed := 0
		for _, item := range filteredSealed {
			totalSealed += len(item.Segments)
		}
		assert.Equal(t, 0, totalSealed, "seg2 skipped because pk=1 not in it")
		assert.Empty(t, filteredGrowing)
	})

	t.Run("multi-worker sealed with partition filter", func(t *testing.T) {
		// Segments on different workers but same partition should all survive.
		multiWorkerSealed := []SnapshotItem{
			{NodeID: 1, Segments: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1, Candidate: bfs1},
				{SegmentID: 2, PartitionID: 2, Candidate: bfs2},
			}},
			{NodeID: 2, Segments: []SegmentEntry{
				{SegmentID: 3, PartitionID: 1, Candidate: bfs3}, // partition=1 on worker 2
			}},
		}
		planOne := queryPlan(pkTermExpr(1, 3))
		segHint, filteredSealed, _ := buildAndApplySegmentHints(
			marshalPlan(t, planOne), common.PkFilterHintHasPkFilter, []int64{1}, multiWorkerSealed, nil, 100, metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
		// Both worker nodes should have partition=1 segments.
		assert.Len(t, filteredSealed, 2, "both worker nodes preserved")
		assert.Len(t, filteredSealed[0].Segments, 1, "worker1: only seg1 (partition=1)")
		assert.Equal(t, int64(1), filteredSealed[0].Segments[0].SegmentID)
		assert.Len(t, filteredSealed[1].Segments, 1, "worker2: only seg3 (partition=1)")
		assert.Equal(t, int64(3), filteredSealed[1].Segments[0].SegmentID)
	})

	t.Run("partition filter removes entire worker node", func(t *testing.T) {
		// Worker 2 has only partition=2 segments → filtered out entirely.
		multiWorkerSealed := []SnapshotItem{
			{NodeID: 1, Segments: []SegmentEntry{
				{SegmentID: 1, PartitionID: 1, Candidate: bfs1},
			}},
			{NodeID: 2, Segments: []SegmentEntry{
				{SegmentID: 2, PartitionID: 2, Candidate: bfs2},
			}},
		}
		planOne := queryPlan(pkTermExpr(1))
		segHint, filteredSealed, _ := buildAndApplySegmentHints(
			marshalPlan(t, planOne), common.PkFilterHintHasPkFilter, []int64{1}, multiWorkerSealed, nil, 100, metrics.QueryLabel,
		)
		require.NotNil(t, segHint)
		// Worker 2 entirely removed (no partition=1 segments).
		assert.Len(t, filteredSealed, 1, "only worker1 survives")
		assert.Equal(t, int64(1), filteredSealed[0].NodeID)
	})
}

func TestPartitionFilterWithMinMaxPruning(t *testing.T) {
	paramtable.Init()
	paramtable.Get().QueryNodeCfg.EnableSegmentPkHint.SwapTempValue("true")
	paramtable.Get().QueryNodeCfg.SegmentPkFilterMode.SwapTempValue("hints")

	// seg1(partition=1, pk range [1,10]), seg2(partition=2, pk range [100,200])
	bfs1 := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs1.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1), storage.NewInt64PrimaryKey(10)})
	bfs2 := pkoracle.NewBloomFilterSet(2, 2, commonpb.SegmentState_Sealed)
	bfs2.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(100), storage.NewInt64PrimaryKey(200)})

	sealed := []SnapshotItem{{
		NodeID: 1,
		Segments: []SegmentEntry{
			{SegmentID: 1, PartitionID: 1, Candidate: bfs1},
			{SegmentID: 2, PartitionID: 2, Candidate: bfs2},
		},
	}}

	t.Run("partition filter + range predicate prune combined", func(t *testing.T) {
		// Query: pk > 50 with partition=[1,2].
		// Partition filter: both survive.
		// Min/max pruning: seg1 max=10 < 50 → skipped. seg2 min=100 > 50 → kept.
		rangePlan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
								Op:         planpb.OpType_GreaterThan,
								Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 50}},
							},
						},
					},
				},
			},
		}
		segHint, filteredSealed, _ := buildAndApplySegmentHints(
			marshalPlan(t, rangePlan), common.PkFilterHintHasPkFilter, []int64{1, 2}, sealed, nil, 100, metrics.QueryLabel,
		)
		assert.Nil(t, segHint, "range expr doesn't produce BF hints")
		totalSealed := 0
		for _, item := range filteredSealed {
			totalSealed += len(item.Segments)
		}
		assert.Equal(t, 1, totalSealed, "seg1 pruned by min/max, seg2 kept")
		assert.Equal(t, int64(2), filteredSealed[0].Segments[0].SegmentID)
	})

	t.Run("partition filter removes segment before min/max can evaluate it", func(t *testing.T) {
		// Query: pk > 50 with partition=[1] only.
		// Partition filter: only seg1 survives. seg2 is never evaluated.
		// Min/max: seg1 max=10 < 50 → skipped.
		rangePlan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
								Op:         planpb.OpType_GreaterThan,
								Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 50}},
							},
						},
					},
				},
			},
		}
		segHint, filteredSealed, _ := buildAndApplySegmentHints(
			marshalPlan(t, rangePlan), common.PkFilterHintHasPkFilter, []int64{1}, sealed, nil, 100, metrics.QueryLabel,
		)
		assert.Nil(t, segHint)
		totalSealed := 0
		for _, item := range filteredSealed {
			totalSealed += len(item.Segments)
		}
		assert.Equal(t, 0, totalSealed, "seg1 pruned by min/max after partition filter")
	})
}

func TestBuildSegmentHintPartitionFiltering(t *testing.T) {
	// Test buildSegmentHintFromPlan with partition filtering.
	bfs1 := pkoracle.NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs1.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})
	bfs2 := pkoracle.NewBloomFilterSet(2, 2, commonpb.SegmentState_Sealed)
	bfs2.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(2)})

	sealed := []SnapshotItem{{
		NodeID: 1,
		Segments: []SegmentEntry{
			{SegmentID: 1, PartitionID: 1, Candidate: bfs1},
			{SegmentID: 2, PartitionID: 2, Candidate: bfs2},
		},
	}}

	growBfs := pkoracle.NewBloomFilterSet(10, 1, commonpb.SegmentState_Growing)
	growBfs.UpdateBloomFilter([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})
	growing := []SegmentEntry{
		{SegmentID: 10, PartitionID: 1, Candidate: growBfs},
	}

	plan := queryPlan(pkTermExpr(1, 2))

	t.Run("buildSegmentHintFromPlan includes all partitions", func(t *testing.T) {
		// Pass both partitions → both segments get hints.
		segHint := buildSegmentHintFromPlan(plan, []int64{1, 2}, sealed, growing)
		require.NotNil(t, segHint)
		// seg1 hits pk=1, seg2 hits pk=2, growing seg10 hits pk=1.
		assert.Len(t, segHint.hints, 3)
		assert.Empty(t, segHint.skippedSegments)
	})

	t.Run("buildSegmentHintFromPlan single partition via pre-filtered input", func(t *testing.T) {
		// Simulate what buildAndApplySegmentHints does: pre-filter then build.
		filteredSealed, filteredGrowing := filterSegmentsByPartitions([]int64{1}, sealed, growing)
		segHint := buildSegmentHintFromPlan(plan, []int64{1}, filteredSealed, filteredGrowing)
		require.NotNil(t, segHint)
		// Only seg1 and growing seg10 in input → only they get hints.
		// seg1 hits pk=1, misses pk=2 → hint [pk=1]. growing seg10 same.
		assert.Len(t, segHint.hints, 2)
		assert.NotContains(t, segHint.hints, int64(2), "seg2 was not in input")
	})
}
