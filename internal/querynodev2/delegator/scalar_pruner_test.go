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
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestUnsupportedBloomLeavesStayConservative(t *testing.T) {
	const clusteringKeyFieldID = int64(100)
	const bloomFieldID = int64(101)

	bloomLeaf := func() *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_BloomFilterExpr{
			BloomFilterExpr: &planpb.BloomFilterExpr{
				ColumnInfo: &planpb.ColumnInfo{
					FieldId:  bloomFieldID,
					DataType: schemapb.DataType_Int64,
				},
			},
		}}
	}
	clusteringKeyEqualsOne := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{
		UnaryRangeExpr: &planpb.UnaryRangeExpr{
			ColumnInfo: &planpb.ColumnInfo{
				FieldId:  clusteringKeyFieldID,
				DataType: schemapb.DataType_Int64,
			},
			Op: planpb.OpType_Equal,
			Value: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{
				Int64Val: 1,
			}},
		},
	}}
	binary := func(op planpb.BinaryExpr_BinaryOp, left, right *planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{
			BinaryExpr: &planpb.BinaryExpr{Op: op, Left: left, Right: right},
		}}
	}

	// The first AND narrows its result to the first segment. The Bloom leaf in
	// the outer OR is unsupported by clustering-key pruning and must therefore
	// conservatively restore all segments instead of reusing that narrowed
	// mutable bitmap.
	predicate := binary(
		planpb.BinaryExpr_LogicalOr,
		binary(planpb.BinaryExpr_LogicalAnd, bloomLeaf(), clusteringKeyEqualsOne),
		bloomLeaf(),
	)
	parsed, err := ParseExpr(
		predicate,
		NewParseContext(clusteringKeyFieldID, schemapb.DataType_Int64),
	)
	require.NoError(t, err)

	segmentStats := []storage.SegmentStats{
		*storage.NewSegmentStats([]storage.FieldStats{{
			FieldID: clusteringKeyFieldID,
			Type:    schemapb.DataType_Int64,
			Min:     storage.NewInt64FieldValue(0),
			Max:     storage.NewInt64FieldValue(5),
		}}, 1),
		*storage.NewSegmentStats([]storage.FieldStats{{
			FieldID: clusteringKeyFieldID,
			Type:    schemapb.DataType_Int64,
			Min:     storage.NewInt64FieldValue(10),
			Max:     storage.NewInt64FieldValue(20),
		}}, 1),
	}
	filteredSegments := make(map[UniqueID]struct{})
	PruneByScalarField(
		parsed,
		segmentStats,
		[]UniqueID{1, 2},
		filteredSegments,
	)

	require.Empty(t, filteredSegments)
}

func TestScalarPrunerFloatingNaNAndLegacyBounds(t *testing.T) {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Float, schemapb.DataType_Double} {
		t.Run(dataType.String(), func(t *testing.T) {
			scalar := func(value float64) storage.ScalarFieldValue {
				if dataType == schemapb.DataType_Float {
					return storage.NewFloatFieldValue(float32(value))
				}
				return storage.NewDoubleFieldValue(value)
			}
			stats := func(min, max float64, complete bool) storage.SegmentStats {
				return storage.SegmentStats{FieldStats: []storage.FieldStats{{Type: dataType, Min: scalar(min), Max: scalar(max), FloatBoundsComplete: complete}}}
			}
			segments := []storage.SegmentStats{
				stats(-1, 1, true), stats(1, math.NaN(), true), stats(math.NaN(), math.NaN(), true),
				stats(-1, 1, false),
				{},
			}
			for _, test := range []struct {
				name     string
				expr     Expr
				filtered []UniqueID
			}{
				{"equal_nan", NewUnaryRangeExpr(scalar(math.NaN()), planpb.OpType_Equal), []UniqueID{1}},
				{"greater_inf", NewUnaryRangeExpr(scalar(math.Inf(1)), planpb.OpType_GreaterThan), []UniqueID{1}},
				{"nan_term", NewTermExpr([]storage.ScalarFieldValue{scalar(math.NaN())}), []UniqueID{1}},
				{"nan_then_finite_term", NewTermExpr([]storage.ScalarFieldValue{scalar(math.NaN()), scalar(1)}), nil},
				{"nan_range", NewBinaryRangeExpr(scalar(math.NaN()), scalar(math.NaN()), true, true), []UniqueID{1}},
				{"less_nan", NewUnaryRangeExpr(scalar(math.NaN()), planpb.OpType_LessThan), []UniqueID{3}},
			} {
				t.Run(test.name, func(t *testing.T) {
					filtered := map[UniqueID]struct{}{}
					PruneByScalarField(test.expr, segments, []UniqueID{1, 2, 3, 4, 5}, filtered)
					require.Len(t, filtered, len(test.filtered))
					for _, id := range test.filtered {
						require.Contains(t, filtered, id)
					}
				})
			}
		})
	}
}

func TestPruneSegmentsKeepsOmittedAndLegacyFloatingStats(t *testing.T) {
	paramtable.Init()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, Name: "f", DataType: schemapb.DataType_Double, IsClusteringKey: true}}}
	snapshot := storage.NewPartitionStatsSnapshot()
	finite := storage.FieldStats{FieldID: 101, Type: schemapb.DataType_Double, Min: storage.NewDoubleFieldValue(-1), Max: storage.NewDoubleFieldValue(1), FloatBoundsComplete: true}
	snapshot.UpdateSegmentStats(1, *storage.NewSegmentStats([]storage.FieldStats{finite}, 2))
	nonfinite := finite.Clone()
	nonfinite.Max = storage.NewDoubleFieldValue(math.NaN())
	snapshot.UpdateSegmentStats(2, *storage.NewSegmentStats([]storage.FieldStats{nonfinite}, 2))
	legacy := finite.Clone()
	legacy.FloatBoundsComplete = false
	snapshot.UpdateSegmentStats(3, *storage.NewSegmentStats([]storage.FieldStats{legacy}, 2))
	encoded, err := storage.SerializePartitionStatsSnapshot(snapshot)
	require.NoError(t, err)
	restored, err := storage.DeserializePartitionsStatsSnapshot(encoded)
	require.NoError(t, err)
	require.NotContains(t, restored.SegmentStats, int64(2))
	plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
		ColumnInfo: &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Double},
		Op:         planpb.OpType_Equal, Value: &planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: math.NaN()}},
	}}}}}}
	serializedPlan, err := proto.Marshal(plan)
	require.NoError(t, err)
	for _, partitions := range [][]int64{nil, {10}} {
		sealed := []SnapshotItem{{NodeID: 1, Segments: []SegmentEntry{{SegmentID: 1}, {SegmentID: 2}, {SegmentID: 3}}}}
		PruneSegments(context.Background(), map[UniqueID]*storage.PartitionStatsSnapshot{10: restored}, nil,
			&internalpb.RetrieveRequest{CollectionID: 1, SerializedExprPlan: serializedPlan, PartitionIDs: partitions}, schema, sealed, PruneInfo{})
		require.Equal(t, []SegmentEntry{{SegmentID: 2}, {SegmentID: 3}}, sealed[0].Segments)
	}
}
