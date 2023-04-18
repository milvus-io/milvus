package executor

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/mysqld/planner"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func GenNodeExpression(field string, c int64, op planner.ComparisonOperator) *planner.NodeExpression {
	n := planner.NewNodeExpression("", planner.WithPredicate(
		planner.NewNodePredicate("", planner.WithNodeBinaryComparisonPredicate(
			planner.NewNodeBinaryComparisonPredicate("",
				planner.NewNodePredicate("", planner.WithNodeExpressionAtomPredicate(
					planner.NewNodeExpressionAtomPredicate("",
						planner.NewNodeExpressionAtom("", planner.ExpressionAtomWithFullColumnName(
							planner.NewNodeFullColumnName("", field)))))),
				planner.NewNodePredicate("", planner.WithNodeExpressionAtomPredicate(
					planner.NewNodeExpressionAtomPredicate("",
						planner.NewNodeExpressionAtom("", planner.ExpressionAtomWithConstant(
							planner.NewNodeConstant("", planner.WithDecimalLiteral(c))))))),
				op),
		),
		)),
	)
	return n
}

func Test_getOutputFieldsOrMatchCountRule(t *testing.T) {
	t.Run("match count rule", func(t *testing.T) {
		fl := []*planner.NodeSelectElement{
			planner.NewNodeSelectElement("", planner.WithFunctionCall(
				planner.NewNodeFunctionCall("", planner.WithAgg(
					planner.NewNodeAggregateWindowedFunction("", planner.WithAggCount(
						planner.NewNodeCount(""))))))),
		}
		_, match, err := getOutputFieldsOrMatchCountRule(fl)
		assert.NoError(t, err)
		assert.True(t, match)
	})

	t.Run("star *, not supported", func(t *testing.T) {
		fl := []*planner.NodeSelectElement{
			planner.NewNodeSelectElement("", planner.WithStar()),
		}
		_, _, err := getOutputFieldsOrMatchCountRule(fl)
		assert.Error(t, err)
	})

	t.Run("combined", func(t *testing.T) {
		fl := []*planner.NodeSelectElement{
			planner.NewNodeSelectElement("", planner.WithFunctionCall(
				planner.NewNodeFunctionCall("", planner.WithAgg(
					planner.NewNodeAggregateWindowedFunction("", planner.WithAggCount(
						planner.NewNodeCount(""))))))),
			planner.NewNodeSelectElement("", planner.WithFullColumnName(
				planner.NewNodeFullColumnName("", "field"))),
		}
		_, _, err := getOutputFieldsOrMatchCountRule(fl)
		assert.Error(t, err)
	})

	t.Run("alias, not supported", func(t *testing.T) {
		fl := []*planner.NodeSelectElement{
			planner.NewNodeSelectElement("", planner.WithFullColumnName(
				planner.NewNodeFullColumnName("", "field", planner.FullColumnNameWithAlias("alias")))),
		}
		_, _, err := getOutputFieldsOrMatchCountRule(fl)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		fl := []*planner.NodeSelectElement{
			planner.NewNodeSelectElement("", planner.WithFullColumnName(
				planner.NewNodeFullColumnName("", "field1"))),
			planner.NewNodeSelectElement("", planner.WithFullColumnName(
				planner.NewNodeFullColumnName("", "field2"))),
		}
		outputFields, match, err := getOutputFieldsOrMatchCountRule(fl)
		assert.NoError(t, err)
		assert.False(t, match)
		assert.ElementsMatch(t, []string{"field1", "field2"}, outputFields)
	})
}

func Test_wrapCountResult(t *testing.T) {
	sqlRes := wrapCountResult(100, "count(*)")
	assert.Equal(t, 1, len(sqlRes.Fields))
	assert.Equal(t, "count(*)", sqlRes.Fields[0].Name)
	assert.Equal(t, query.Type_INT64, sqlRes.Fields[0].Type)
	assert.Equal(t, 1, len(sqlRes.Rows))
	assert.Equal(t, 1, len(sqlRes.Rows[0]))
	assert.Equal(t, query.Type_INT64, sqlRes.Rows[0][0].Type())
}

func Test_wrapQueryResults(t *testing.T) {
	res := &milvuspb.QueryResults{
		Status: &commonpb.Status{},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "field",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{
								Data: []int64{1, 2, 3, 4},
							},
						},
					},
				},
			},
		},
		CollectionName: "test",
	}
	sqlRes := wrapQueryResults(res)
	assert.Equal(t, 1, len(sqlRes.Fields))
	assert.Equal(t, 4, len(sqlRes.Rows))
	assert.Equal(t, "field", sqlRes.Fields[0].Name)
	assert.Equal(t, query.Type_INT64, sqlRes.Fields[0].Type)
	assert.Equal(t, 1, len(sqlRes.Rows[0]))
	assert.Equal(t, query.Type_INT64, sqlRes.Rows[0][0].Type())
}

func Test_getSQLField(t *testing.T) {
	f := &schemapb.FieldData{
		FieldName: "a",
		Type:      schemapb.DataType_Int64,
	}
	sf := getSQLField("t", f)
	assert.Equal(t, "a", sf.Name)
	assert.Equal(t, query.Type_INT64, sf.Type)
	assert.Equal(t, "t", sf.Table)
}

func Test_toSQLType(t *testing.T) {
	type args struct {
		t schemapb.DataType
	}
	tests := []struct {
		name string
		args args
		want query.Type
	}{
		{
			args: args{
				t: schemapb.DataType_Bool,
			},
			want: query.Type_UINT8,
		},
		{
			args: args{
				t: schemapb.DataType_Int8,
			},
			want: query.Type_INT8,
		},
		{
			args: args{
				t: schemapb.DataType_Int16,
			},
			want: query.Type_INT16,
		},
		{
			args: args{
				t: schemapb.DataType_Int32,
			},
			want: query.Type_INT32,
		},
		{
			args: args{
				t: schemapb.DataType_Int64,
			},
			want: query.Type_INT64,
		},
		{
			args: args{
				t: schemapb.DataType_Float,
			},
			want: query.Type_FLOAT32,
		},
		{
			args: args{
				t: schemapb.DataType_Double,
			},
			want: query.Type_FLOAT64,
		},
		{
			args: args{
				t: schemapb.DataType_VarChar,
			},
			want: query.Type_VARCHAR,
		},
		{
			args: args{
				t: schemapb.DataType_FloatVector,
			},
			want: query.Type_NULL_TYPE,
		},
		{
			args: args{
				t: schemapb.DataType_BinaryVector,
			},
			want: query.Type_NULL_TYPE,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, toSQLType(tt.args.t), "toSQLType(%v)", tt.args.t)
		})
	}
}
