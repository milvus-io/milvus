package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestCheckIdentical(t *testing.T) {
	schema := newTestSchema(true)
	helper, err := typeutil.CreateSchemaHelper(schema)
	assert.NoError(t, err)

	exprStr1Arr := []string{
		`not (((Int64Field > 0) and (FloatField <= 20.0)) or ((Int32Field in [1, 2, 3]) and (VarCharField < "str")))`,
		`f1()`,
	}
	exprStr2Arr := []string{
		`Int32Field in [1, 2, 3]`,
		`f2(Int32Field, Int64Field)`,
	}
	for i := range exprStr1Arr {
		exprStr1 := exprStr1Arr[i]
		exprStr2 := exprStr2Arr[i]

		expr1, err := ParseExpr(helper, exprStr1, nil)
		assert.NoError(t, err)
		expr2, err := ParseExpr(helper, exprStr2, nil)
		assert.NoError(t, err)

		assert.True(t, CheckPredicatesIdentical(expr1, expr1))
		assert.True(t, CheckPredicatesIdentical(expr2, expr2))
		assert.False(t, CheckPredicatesIdentical(expr1, expr2))
	}
}

func TestCheckQueryInfoIdentical(t *testing.T) {
	type args struct {
		info1 *planpb.QueryInfo
		info2 *planpb.QueryInfo
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				info1: &planpb.QueryInfo{Topk: 1},
				info2: &planpb.QueryInfo{Topk: 2},
			},
			want: false,
		},
		{
			args: args{
				info1: &planpb.QueryInfo{Topk: 1, MetricType: "L2"},
				info2: &planpb.QueryInfo{Topk: 1, MetricType: "IP"},
			},
			want: false,
		},
		{
			args: args{
				info1: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`},
				info2: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: ""},
			},
			want: false,
		},
		{
			args: args{
				info1: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 5},
				info2: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
			},
			want: false,
		},
		{
			args: args{
				info1: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
				info2: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckQueryInfoIdentical(tt.args.info1, tt.args.info2); got != tt.want {
				t.Errorf("CheckQueryInfoIdentical() = %v, want %v\ninfo1:\n%v\ninfo2:\n%v", got, tt.want, tt.args.info1, tt.args.info2)
			}
		})
	}
}

func TestCheckVectorANNSIdentical(t *testing.T) {
	type args struct {
		node1 *planpb.VectorANNS
		node2 *planpb.VectorANNS
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				node1: &planpb.VectorANNS{VectorType: planpb.VectorType_BinaryVector},
				node2: &planpb.VectorANNS{VectorType: planpb.VectorType_FloatVector},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.VectorANNS{VectorType: planpb.VectorType_FloatVector, FieldId: 100},
				node2: &planpb.VectorANNS{VectorType: planpb.VectorType_FloatVector, FieldId: 101},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.VectorANNS{VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0"},
				node2: &planpb.VectorANNS{VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$1"},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.VectorANNS{VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 100}},
				node2: &planpb.VectorANNS{VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 10}},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.VectorANNS{
					VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_ColumnExpr{
							ColumnExpr: &planpb.ColumnExpr{
								Info: &planpb.ColumnInfo{},
							},
						},
					},
				},
				node2: &planpb.VectorANNS{
					VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_ValueExpr{
							ValueExpr: &planpb.ValueExpr{Value: NewInt(100)},
						},
					},
				},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.VectorANNS{
					VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_ValueExpr{
							ValueExpr: &planpb.ValueExpr{Value: NewInt(100)},
						},
					},
				},
				node2: &planpb.VectorANNS{
					VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_ValueExpr{
							ValueExpr: &planpb.ValueExpr{Value: NewInt(100)},
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckVectorANNSIdentical(tt.args.node1, tt.args.node2); got != tt.want {
				t.Errorf("CheckVectorANNSIdentical() = %v, want %v\nnode1:\n%v\nnode2:\n%v", got, tt.want, tt.args.node1, tt.args.node2)
			}
		})
	}
}

func TestCheckPlanNodeIdentical(t *testing.T) {
	type args struct {
		node1 *planpb.PlanNode
		node2 *planpb.PlanNode
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				node1: &planpb.PlanNode{
					Node:           &planpb.PlanNode_VectorAnns{},
					OutputFieldIds: []int64{100, 101},
				},
				node2: &planpb.PlanNode{
					Node:           &planpb.PlanNode_VectorAnns{},
					OutputFieldIds: []int64{100},
				},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.PlanNode{
					Node: &planpb.PlanNode_VectorAnns{
						VectorAnns: &planpb.VectorANNS{
							VectorType: planpb.VectorType_BinaryVector,
						},
					},
					OutputFieldIds: []int64{100},
				},
				node2: &planpb.PlanNode{
					Node: &planpb.PlanNode_VectorAnns{
						VectorAnns: &planpb.VectorANNS{
							VectorType: planpb.VectorType_FloatVector,
						},
					},
					OutputFieldIds: []int64{100},
				},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.PlanNode{
					Node: &planpb.PlanNode_VectorAnns{
						VectorAnns: &planpb.VectorANNS{
							VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
							Predicates: &planpb.Expr{
								Expr: &planpb.Expr_ValueExpr{
									ValueExpr: &planpb.ValueExpr{Value: NewInt(100)},
								},
							},
						},
					},
					OutputFieldIds: []int64{100},
				},
				node2: &planpb.PlanNode{
					Node: &planpb.PlanNode_VectorAnns{
						VectorAnns: &planpb.VectorANNS{
							VectorType: planpb.VectorType_FloatVector, FieldId: 100, PlaceholderTag: "$0", QueryInfo: &planpb.QueryInfo{Topk: 1, MetricType: "L2", SearchParams: `{"nprobe": 10}`, RoundDecimal: 6},
							Predicates: &planpb.Expr{
								Expr: &planpb.Expr_ValueExpr{
									ValueExpr: &planpb.ValueExpr{Value: NewInt(100)},
								},
							},
						},
					},
					OutputFieldIds: []int64{100},
				},
			},
			want: true,
		},
		{
			args: args{
				node1: &planpb.PlanNode{
					Node:           &planpb.PlanNode_VectorAnns{},
					OutputFieldIds: []int64{100, 101},
				},
				node2: &planpb.PlanNode{
					Node: &planpb.PlanNode_Predicates{},
				},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.PlanNode{
					Node:           &planpb.PlanNode_Predicates{},
					OutputFieldIds: []int64{100, 101},
				},
				node2: &planpb.PlanNode{
					Node:           &planpb.PlanNode_Predicates{},
					OutputFieldIds: []int64{100},
				},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.PlanNode{
					Node: &planpb.PlanNode_Predicates{
						Predicates: &planpb.Expr{
							Expr: &planpb.Expr_ColumnExpr{
								ColumnExpr: &planpb.ColumnExpr{
									Info: &planpb.ColumnInfo{},
								},
							},
						},
					},
					OutputFieldIds: []int64{100, 101},
				},
				node2: &planpb.PlanNode{
					Node: &planpb.PlanNode_Predicates{
						Predicates: &planpb.Expr{
							Expr: &planpb.Expr_ValueExpr{
								ValueExpr: &planpb.ValueExpr{Value: NewInt(100)},
							},
						},
					},
					OutputFieldIds: []int64{100, 101},
				},
			},
			want: false,
		},
		{
			args: args{
				node1: &planpb.PlanNode{
					Node: &planpb.PlanNode_Predicates{
						Predicates: &planpb.Expr{
							Expr: &planpb.Expr_ValueExpr{
								ValueExpr: &planpb.ValueExpr{Value: NewInt(100)},
							},
						},
					},
					OutputFieldIds: []int64{100, 101},
				},
				node2: &planpb.PlanNode{
					Node: &planpb.PlanNode_Predicates{
						Predicates: &planpb.Expr{
							Expr: &planpb.Expr_ValueExpr{
								ValueExpr: &planpb.ValueExpr{Value: NewInt(100)},
							},
						},
					},
					OutputFieldIds: []int64{100, 101},
				},
			},
			want: true,
		},
		{
			args: args{
				node1: &planpb.PlanNode{
					Node: &planpb.PlanNode_Predicates{},
				},
				node2: &planpb.PlanNode{
					Node: &planpb.PlanNode_VectorAnns{},
				},
			},
			want: false,
		},
		{
			args: args{
				node1: nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckPlanNodeIdentical(tt.args.node1, tt.args.node2); got != tt.want {
				t.Errorf("CheckPlanNodeIdentical() = %v, want %v\nnode1:\n%v\nnode2:\n%v", got, tt.want, tt.args.node1, tt.args.node2)
			}
		})
	}
}
