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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
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
