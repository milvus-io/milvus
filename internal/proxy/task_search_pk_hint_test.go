package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func TestComputePkFilterHint(t *testing.T) {
	t.Run("pk term expr", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									IsPrimaryKey: true,
									DataType:     schemapb.DataType_Int64,
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
		assert.Equal(t, common.PkFilterHintHasPkFilter, computePkFilterHint(plan))
	})

	t.Run("pk unary range expr", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_UnaryRangeExpr{
							UnaryRangeExpr: &planpb.UnaryRangeExpr{
								ColumnInfo: &planpb.ColumnInfo{
									IsPrimaryKey: true,
									DataType:     schemapb.DataType_Int64,
								},
								Op:    planpb.OpType_GreaterThan,
								Value: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 100}},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, common.PkFilterHintHasPkFilter, computePkFilterHint(plan))
	})

	t.Run("non pk predicate", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									IsPrimaryKey: false,
									DataType:     schemapb.DataType_Int64,
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
		assert.Equal(t, common.PkFilterHintNoPkFilter, computePkFilterHint(plan))
	})

	t.Run("nil plan node", func(t *testing.T) {
		plan := &planpb.PlanNode{}
		assert.Equal(t, common.PkFilterHintNoPkFilter, computePkFilterHint(plan))
	})

	t.Run("nil predicates", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{},
			},
		}
		assert.Equal(t, common.PkFilterHintNoPkFilter, computePkFilterHint(plan))
	})

	t.Run("VectorAnns plan with pk predicate", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{
									IsPrimaryKey: true,
									DataType:     schemapb.DataType_Int64,
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
		assert.Equal(t, common.PkFilterHintHasPkFilter, computePkFilterHint(plan))
	})
}
