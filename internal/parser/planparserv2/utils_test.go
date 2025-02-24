package planparserv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
)

func Test_relationalCompatible(t *testing.T) {
	type args struct {
		t1 schemapb.DataType
		t2 schemapb.DataType
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			// both.
			args: args{
				t1: schemapb.DataType_VarChar,
				t2: schemapb.DataType_VarChar,
			},
			want: true,
		},
		{
			// neither.
			args: args{
				t1: schemapb.DataType_Float,
				t2: schemapb.DataType_Float,
			},
			want: true,
		},
		{
			// in-compatible.
			args: args{
				t1: schemapb.DataType_Float,
				t2: schemapb.DataType_VarChar,
			},
			want: false,
		},
		{
			// in-compatible.
			args: args{
				t1: schemapb.DataType_VarChar,
				t2: schemapb.DataType_Float,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := relationalCompatible(tt.args.t1, tt.args.t2); got != tt.want {
				t.Errorf("relationalCompatible() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsAlwaysTruePlan(t *testing.T) {
	type args struct {
		plan *planpb.PlanNode
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				plan: nil,
			},
			want: false,
		},
		{
			args: args{
				plan: &planpb.PlanNode{
					Node: &planpb.PlanNode_VectorAnns{
						VectorAnns: &planpb.VectorANNS{
							Predicates: alwaysTrueExpr(),
						},
					},
				},
			},
			want: true,
		},
		{
			args: args{
				plan: &planpb.PlanNode{
					Node: &planpb.PlanNode_Predicates{
						Predicates: alwaysTrueExpr(),
					},
				},
			},
			want: true,
		},
		{
			args: args{
				plan: &planpb.PlanNode{
					Node: &planpb.PlanNode_Query{
						Query: &planpb.QueryPlanNode{
							Predicates: alwaysTrueExpr(),
							IsCount:    false,
						},
					},
				},
			},
			want: true,
		},
		{
			args: args{
				plan: &planpb.PlanNode{
					Node: &planpb.PlanNode_Query{
						Query: &planpb.QueryPlanNode{
							Predicates: alwaysTrueExpr(),
							IsCount:    true,
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsAlwaysTruePlan(tt.args.plan), "IsAlwaysTruePlan(%v)", tt.args.plan)
		})
	}
}

func Test_canBeExecuted(t *testing.T) {
	type args struct {
		e *ExprWithType
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				e: &ExprWithType{
					dataType: schemapb.DataType_Int64,
				},
			},
			want: false,
		},
		{
			args: args{
				e: &ExprWithType{
					dataType:      schemapb.DataType_Bool,
					nodeDependent: true,
				},
			},
			want: false,
		},
		{
			args: args{
				e: &ExprWithType{
					dataType:      schemapb.DataType_Bool,
					nodeDependent: false,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, canBeExecuted(tt.args.e), "canBeExecuted(%v)", tt.args.e)
		})
	}
}

func Test_convertEscapeSingle(t *testing.T) {
	type testCases struct {
		input    string
		expected string
	}
	normalCases := []testCases{
		{`"\'"`, `'`},
		{`"\\'"`, `\'`},
		{`"\\\'"`, `\'`},
		{`"\\\\'"`, `\\'`},
		{`"\\\\\'"`, `\\'`},
		{`'"'`, `"`},
		{`'""'`, `""`},
		{`'"""'`, `"""`},
		{`'"\""'`, `"""`},
		{`'a"b\"c\\"d'`, `a"b"c\"d`},
		{`"a\"b\"c\\\"d"`, `a"b"c\"d`},
		{`'A "test"'`, `A "test"`},
		{`"A \"test\""`, `A "test"`},
		{`'\"'`, `"`},
		{`'\\"'`, `\"`},
		{`'\\\"'`, `\"`},
		{`'\\\\"'`, `\\"`},
		{`'\\\\\"'`, `\\"`},
	}
	for _, c := range normalCases {
		actual, err := convertEscapeSingle(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, actual)
	}

	unNormalCases := []testCases{
		{`"\423"`, ``},
		{`'\378'`, ``},
	}
	for _, c := range unNormalCases {
		actual, err := convertEscapeSingle(c.input)
		assert.Error(t, err)
		assert.Equal(t, c.expected, actual)
	}
}

func Test_canBeComparedDataType(t *testing.T) {
	type testCases struct {
		left     schemapb.DataType
		right    schemapb.DataType
		expected bool
	}

	cases := []testCases{
		{schemapb.DataType_Bool, schemapb.DataType_Bool, true},
		{schemapb.DataType_Bool, schemapb.DataType_JSON, true},
		{schemapb.DataType_Bool, schemapb.DataType_Int8, false},
		{schemapb.DataType_Bool, schemapb.DataType_Int16, false},
		{schemapb.DataType_Bool, schemapb.DataType_Int32, false},
		{schemapb.DataType_Bool, schemapb.DataType_Int64, false},
		{schemapb.DataType_Bool, schemapb.DataType_Float, false},
		{schemapb.DataType_Bool, schemapb.DataType_Double, false},
		{schemapb.DataType_Bool, schemapb.DataType_String, false},
		{schemapb.DataType_Int8, schemapb.DataType_Int16, true},
		{schemapb.DataType_Int16, schemapb.DataType_Int32, true},
		{schemapb.DataType_Int32, schemapb.DataType_Int64, true},
		{schemapb.DataType_Int64, schemapb.DataType_Float, true},
		{schemapb.DataType_Float, schemapb.DataType_Double, true},
		{schemapb.DataType_Double, schemapb.DataType_Int32, true},
		{schemapb.DataType_Double, schemapb.DataType_String, false},
		{schemapb.DataType_Int64, schemapb.DataType_String, false},
		{schemapb.DataType_Int64, schemapb.DataType_JSON, true},
		{schemapb.DataType_Double, schemapb.DataType_JSON, true},
		{schemapb.DataType_String, schemapb.DataType_Double, false},
		{schemapb.DataType_String, schemapb.DataType_Int64, false},
		{schemapb.DataType_String, schemapb.DataType_JSON, true},
		{schemapb.DataType_String, schemapb.DataType_String, true},
		{schemapb.DataType_String, schemapb.DataType_VarChar, true},
		{schemapb.DataType_VarChar, schemapb.DataType_VarChar, true},
		{schemapb.DataType_VarChar, schemapb.DataType_JSON, true},
		{schemapb.DataType_VarChar, schemapb.DataType_Int64, false},
		{schemapb.DataType_Array, schemapb.DataType_Int64, false},
		{schemapb.DataType_Array, schemapb.DataType_Array, false},
	}

	for _, c := range cases {
		assert.Equal(t, c.expected, canBeComparedDataType(c.left, c.right))
	}
}

func Test_getArrayElementType(t *testing.T) {
	t.Run("array element", func(t *testing.T) {
		expr := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ValueExpr{
					ValueExpr: &planpb.ValueExpr{
						Value: &planpb.GenericValue{
							Val: &planpb.GenericValue_ArrayVal{
								ArrayVal: &planpb.Array{
									Array:       nil,
									ElementType: schemapb.DataType_Int64,
								},
							},
						},
					},
				},
			},
			dataType:      schemapb.DataType_Array,
			nodeDependent: true,
		}

		assert.Equal(t, schemapb.DataType_Int64, getArrayElementType(expr))
	})

	t.Run("array field", func(t *testing.T) {
		expr := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:        101,
							DataType:       schemapb.DataType_Array,
							IsPrimaryKey:   false,
							IsAutoID:       false,
							NestedPath:     nil,
							IsPartitionKey: false,
							ElementType:    schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType:      schemapb.DataType_Array,
			nodeDependent: true,
		}

		assert.Equal(t, schemapb.DataType_Int64, getArrayElementType(expr))
	})

	t.Run("not array", func(t *testing.T) {
		expr := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  102,
							DataType: schemapb.DataType_String,
						},
					},
				},
			},
			dataType:      schemapb.DataType_String,
			nodeDependent: true,
		}

		assert.Equal(t, schemapb.DataType_None, getArrayElementType(expr))
	})
}

func Test_decodeUnicode(t *testing.T) {
	s1 := "A[\"\\u5e74\\u4efd\"][\"\\u6708\\u4efd\"]"

	assert.NotEqual(t, `A["年份"]["月份"]`, s1)
	assert.Equal(t, `A["年份"]["月份"]`, decodeUnicode(s1))
}
