package executor

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/mysqld/planner"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func Test_defaultExecutor_Run(t *testing.T) {
	t.Run("not sql statements", func(t *testing.T) {
		e := NewDefaultExecutor(nil)
		plan := &planner.PhysicalPlan{
			Node: planner.NewNodeConstant("20230306", planner.WithStringLiteral("20230306")),
		}
		_, err := e.Run(context.TODO(), plan)
		assert.Error(t, err)
	})

	t.Run("multiple statements", func(t *testing.T) {
		e := NewDefaultExecutor(nil)
		stmts := []*planner.NodeSqlStatement{
			planner.NewNodeSqlStatement("sql1"),
			planner.NewNodeSqlStatement("sql2"),
		}
		plan := &planner.PhysicalPlan{
			Node: planner.NewNodeSqlStatements("sql1; sql2", stmts),
		}
		_, err := e.Run(context.TODO(), plan)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
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
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // *milvuspb.QueryRequest
		).Return(res, nil)
		e := NewDefaultExecutor(s)
		stmts := []*planner.NodeSqlStatement{
			planner.NewNodeSqlStatement("", planner.WithDmlStatement(
				planner.NewNodeDmlStatement("", planner.WithSelectStatement(
					planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
						planner.NewNodeSimpleSelect("", planner.WithQuery(
							planner.NewNodeQuerySpecification("",
								nil,
								[]*planner.NodeSelectElement{
									planner.NewNodeSelectElement("", planner.WithFullColumnName(
										planner.NewNodeFullColumnName("", "field"))),
								}, planner.WithFrom(planner.NewNodeFromClause("",
									[]*planner.NodeTableSource{
										planner.NewNodeTableSource("", planner.WithTableName("test")),
									},
									planner.WithWhere(planner.NewNodeExpression("", planner.WithPredicate(
										planner.NewNodePredicate("", planner.WithNodeBinaryComparisonPredicate(
											planner.NewNodeBinaryComparisonPredicate("",
												planner.NewNodePredicate("", planner.WithNodeExpressionAtomPredicate(
													planner.NewNodeExpressionAtomPredicate("",
														planner.NewNodeExpressionAtom("", planner.ExpressionAtomWithFullColumnName(
															planner.NewNodeFullColumnName("", "field")))))),
												planner.NewNodePredicate("", planner.WithNodeExpressionAtomPredicate(
													planner.NewNodeExpressionAtomPredicate("",
														planner.NewNodeExpressionAtom("", planner.ExpressionAtomWithFullColumnName(
															planner.NewNodeFullColumnName("", "field")))))),
												planner.ComparisonOperatorEqual),
										),
										)),
									),
									),
								),
								),
							),
						)),
					)),
				)),
			)),
		}
		plan := &planner.PhysicalPlan{
			Node: planner.NewNodeSqlStatements("", stmts),
		}
		sqlRes, err := e.Run(context.TODO(), plan)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sqlRes.Fields))
		assert.Equal(t, 4, len(sqlRes.Rows))
		assert.Equal(t, "field", sqlRes.Fields[0].Name)
		assert.Equal(t, querypb.Type_INT64, sqlRes.Fields[0].Type)
		assert.Equal(t, 1, len(sqlRes.Rows[0]))
		assert.Equal(t, querypb.Type_INT64, sqlRes.Rows[0][0].Type())
	})
}

func Test_defaultExecutor_dispatch(t *testing.T) {
	t.Run("not dml", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSqlStatement("")
		_, err := e.dispatch(context.TODO(), n)
		assert.Error(t, err)
	})
}

func Test_defaultExecutor_dispatchDmlStatement(t *testing.T) {
	t.Run("not select", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeDmlStatement("")
		_, err := e.dispatchDmlStatement(context.TODO(), n)
		assert.Error(t, err)
	})
}

func Test_defaultExecutor_execSelect(t *testing.T) {
	t.Run("not simple select", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("")
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("lock clause, not supported", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithLockClause(
				planner.NewNodeLockClause("", planner.LockClauseOptionForUpdate)))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("not query", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("")))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("pagination, not supported", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					nil,
					planner.WithLimit(planner.NewNodeLimitClause("", 1, 2)))))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("select specs, not supported", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					[]*planner.NodeSelectSpec{
						planner.NewNodeSelectSpec(""),
					},
					nil)))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("no from clause", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					nil)))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("no table source", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					nil,
					planner.WithFrom(planner.NewNodeFromClause("", nil)))))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("multiple table source", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					nil,
					planner.WithFrom(planner.NewNodeFromClause("",
						[]*planner.NodeTableSource{
							planner.NewNodeTableSource(""),
							planner.NewNodeTableSource(""),
						})))))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("target entry as alias, not supported", func(t *testing.T) {
		e := NewDefaultExecutor(nil).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					[]*planner.NodeSelectElement{
						planner.NewNodeSelectElement("", planner.WithFullColumnName(
							planner.NewNodeFullColumnName("", "field", planner.FullColumnNameWithAlias("alias")))),
					},
					planner.WithFrom(planner.NewNodeFromClause("",
						[]*planner.NodeTableSource{
							planner.NewNodeTableSource("", planner.WithTableName("test")),
						})))))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("failed to execute count", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		s.On("GetCollectionStatistics",
			mock.Anything, // context.Context
			mock.Anything, // *milvuspb.GetCollectionStatisticsRequest
		).Return(nil, errors.New("error mock GetCollectionStatistics"))
		e := NewDefaultExecutor(s).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					[]*planner.NodeSelectElement{
						planner.NewNodeSelectElement("", planner.WithFunctionCall(
							planner.NewNodeFunctionCall("", planner.WithAgg(
								planner.NewNodeAggregateWindowedFunction("", planner.WithAggCount(
									planner.NewNodeCount(""))))))),
					},
					planner.WithFrom(planner.NewNodeFromClause("",
						[]*planner.NodeTableSource{
							planner.NewNodeTableSource("", planner.WithTableName("test")),
						})))))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("count without filter", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		s.On("GetCollectionStatistics",
			mock.Anything, // context.Context
			mock.Anything, // *milvuspb.GetCollectionStatisticsRequest
		).Return(&milvuspb.GetCollectionStatisticsResponse{
			Status: &commonpb.Status{},
			Stats:  []*commonpb.KeyValuePair{{Value: "2"}},
		}, nil)
		e := NewDefaultExecutor(s).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					[]*planner.NodeSelectElement{
						planner.NewNodeSelectElement("", planner.WithFunctionCall(
							planner.NewNodeFunctionCall("", planner.WithAgg(
								planner.NewNodeAggregateWindowedFunction("", planner.WithAggCount(
									planner.NewNodeCount(""))))))),
					},
					planner.WithFrom(planner.NewNodeFromClause("",
						[]*planner.NodeTableSource{
							planner.NewNodeTableSource("", planner.WithTableName("test")),
						})))))))
		res, err := e.execSelect(context.TODO(), n)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res.Fields))
		assert.Equal(t, "count(*)", res.Fields[0].Name)
		assert.Equal(t, querypb.Type_INT64, res.Fields[0].Type)
		assert.Equal(t, 1, len(res.Rows))
		assert.Equal(t, 1, len(res.Rows[0]))
		assert.Equal(t, querypb.Type_INT64, res.Rows[0][0].Type())
	})

	t.Run("count with filter", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // *milvuspb.QueryRequest
		).Return(&milvuspb.QueryResults{
			Status: &commonpb.Status{},
			FieldsData: []*schemapb.FieldData{
				{
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
		}, nil)
		e := NewDefaultExecutor(s).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					[]*planner.NodeSelectElement{
						planner.NewNodeSelectElement("", planner.WithFunctionCall(
							planner.NewNodeFunctionCall("", planner.WithAgg(
								planner.NewNodeAggregateWindowedFunction("", planner.WithAggCount(
									planner.NewNodeCount(""))))))),
					},
					planner.WithFrom(planner.NewNodeFromClause("",
						[]*planner.NodeTableSource{
							planner.NewNodeTableSource("", planner.WithTableName("test")),
						},
						planner.WithWhere(GenNodeExpression("field", 1, planner.ComparisonOperatorGreaterEqual)))))))))
		res, err := e.execSelect(context.TODO(), n)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(res.Fields))
		assert.Equal(t, "count(*)", res.Fields[0].Name)
		assert.Equal(t, querypb.Type_INT64, res.Fields[0].Type)
		assert.Equal(t, 1, len(res.Rows))
		assert.Equal(t, 1, len(res.Rows[0]))
		assert.Equal(t, querypb.Type_INT64, res.Rows[0][0].Type())
	})

	t.Run("query without filter", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		e := NewDefaultExecutor(s).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					[]*planner.NodeSelectElement{
						planner.NewNodeSelectElement("", planner.WithFullColumnName(
							planner.NewNodeFullColumnName("", "field"))),
					},
					planner.WithFrom(planner.NewNodeFromClause("",
						[]*planner.NodeTableSource{
							planner.NewNodeTableSource("", planner.WithTableName("test")),
						})))))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("failed to query with filter", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // *milvuspb.QueryRequest
		).Return(nil, errors.New("error mock Query"))
		e := NewDefaultExecutor(s).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					[]*planner.NodeSelectElement{
						planner.NewNodeSelectElement("", planner.WithFullColumnName(
							planner.NewNodeFullColumnName("", "field"))),
					},
					planner.WithFrom(planner.NewNodeFromClause("",
						[]*planner.NodeTableSource{
							planner.NewNodeTableSource("", planner.WithTableName("test")),
						},
						planner.WithWhere(GenNodeExpression("field", 100, planner.ComparisonOperatorEqual)))))))))
		_, err := e.execSelect(context.TODO(), n)
		assert.Error(t, err)
	})

	t.Run("query with filter", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
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
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // *milvuspb.QueryRequest
		).Return(res, nil)
		e := NewDefaultExecutor(s).(*defaultExecutor)
		n := planner.NewNodeSelectStatement("", planner.WithSimpleSelect(
			planner.NewNodeSimpleSelect("", planner.WithQuery(
				planner.NewNodeQuerySpecification("",
					nil,
					[]*planner.NodeSelectElement{
						planner.NewNodeSelectElement("", planner.WithFullColumnName(
							planner.NewNodeFullColumnName("", "field"))),
					},
					planner.WithFrom(planner.NewNodeFromClause("",
						[]*planner.NodeTableSource{
							planner.NewNodeTableSource("", planner.WithTableName("test")),
						},
						planner.WithWhere(GenNodeExpression("field", 100, planner.ComparisonOperatorEqual)))))))))
		sqlRes, err := e.execSelect(context.TODO(), n)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sqlRes.Fields))
		assert.Equal(t, 4, len(sqlRes.Rows))
		assert.Equal(t, "field", sqlRes.Fields[0].Name)
		assert.Equal(t, querypb.Type_INT64, sqlRes.Fields[0].Type)
		assert.Equal(t, 1, len(sqlRes.Rows[0]))
		assert.Equal(t, querypb.Type_INT64, sqlRes.Rows[0][0].Type())
	})
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

func Test_defaultExecutor_execCountWithFilter(t *testing.T) {
	t.Run("failed to query", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // milvuspb.QueryRequest
		).Return(nil, errors.New("error mock Query"))
		e := NewDefaultExecutor(s).(*defaultExecutor)
		_, err := e.execCountWithFilter(context.TODO(), "t", "a > 2")
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
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
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // *milvuspb.QueryRequest
		).Return(res, nil)
		e := NewDefaultExecutor(s).(*defaultExecutor)
		sqlRes, err := e.execCountWithFilter(context.TODO(), "t", "a > 2")
		assert.NoError(t, err)
		assert.Equal(t, 1, len(sqlRes.Fields))
		assert.Equal(t, "count(*)", sqlRes.Fields[0].Name)
		assert.Equal(t, querypb.Type_INT64, sqlRes.Fields[0].Type)
		assert.Equal(t, 1, len(sqlRes.Rows))
		assert.Equal(t, 1, len(sqlRes.Rows[0]))
		assert.Equal(t, querypb.Type_INT64, sqlRes.Rows[0][0].Type())
	})
}

func Test_defaultExecutor_execQuery(t *testing.T) {
	t.Run("rpc failure", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // milvuspb.QueryRequest
		).Return(nil, errors.New("error mock Query"))
		e := NewDefaultExecutor(s).(*defaultExecutor)
		_, err := e.execQuery(context.TODO(), "t", "a > 2", []string{"a"})
		assert.Error(t, err)
	})

	t.Run("not success", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // milvuspb.QueryRequest
		).Return(&milvuspb.QueryResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "error mock reason",
			},
		}, nil)
		e := NewDefaultExecutor(s).(*defaultExecutor)
		_, err := e.execQuery(context.TODO(), "t", "a > 2", []string{"a"})
		assert.Error(t, err)
	})

	t.Run("success", func(t *testing.T) {
		s := mocks.NewProxyComponent(t)
		s.On("Query",
			mock.Anything, // context.Context
			mock.Anything, // milvuspb.QueryRequest
		).Return(&milvuspb.QueryResults{
			Status: &commonpb.Status{},
		}, nil)
		e := NewDefaultExecutor(s).(*defaultExecutor)
		_, err := e.execQuery(context.TODO(), "t", "a > 2", []string{"a"})
		assert.NoError(t, err)
	})
}

func Test_wrapCountResult(t *testing.T) {
	sqlRes := wrapCountResult(100, "count(*)")
	assert.Equal(t, 1, len(sqlRes.Fields))
	assert.Equal(t, "count(*)", sqlRes.Fields[0].Name)
	assert.Equal(t, querypb.Type_INT64, sqlRes.Fields[0].Type)
	assert.Equal(t, 1, len(sqlRes.Rows))
	assert.Equal(t, 1, len(sqlRes.Rows[0]))
	assert.Equal(t, querypb.Type_INT64, sqlRes.Rows[0][0].Type())
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
	assert.Equal(t, querypb.Type_INT64, sqlRes.Fields[0].Type)
	assert.Equal(t, 1, len(sqlRes.Rows[0]))
	assert.Equal(t, querypb.Type_INT64, sqlRes.Rows[0][0].Type())
}

func Test_getSQLField(t *testing.T) {
	f := &schemapb.FieldData{
		FieldName: "a",
		Type:      schemapb.DataType_Int64,
	}
	sf := getSQLField("t", f)
	assert.Equal(t, "a", sf.Name)
	assert.Equal(t, querypb.Type_INT64, sf.Type)
	assert.Equal(t, "t", sf.Table)
}

func Test_toSQLType(t *testing.T) {
	type args struct {
		t schemapb.DataType
	}
	tests := []struct {
		name string
		args args
		want querypb.Type
	}{
		{
			args: args{
				t: schemapb.DataType_Bool,
			},
			want: querypb.Type_UINT8,
		},
		{
			args: args{
				t: schemapb.DataType_Int8,
			},
			want: querypb.Type_INT8,
		},
		{
			args: args{
				t: schemapb.DataType_Int16,
			},
			want: querypb.Type_INT16,
		},
		{
			args: args{
				t: schemapb.DataType_Int32,
			},
			want: querypb.Type_INT32,
		},
		{
			args: args{
				t: schemapb.DataType_Int64,
			},
			want: querypb.Type_INT64,
		},
		{
			args: args{
				t: schemapb.DataType_Float,
			},
			want: querypb.Type_FLOAT32,
		},
		{
			args: args{
				t: schemapb.DataType_Double,
			},
			want: querypb.Type_FLOAT64,
		},
		{
			args: args{
				t: schemapb.DataType_VarChar,
			},
			want: querypb.Type_VARCHAR,
		},
		{
			args: args{
				t: schemapb.DataType_FloatVector,
			},
			want: querypb.Type_NULL_TYPE,
		},
		{
			args: args{
				t: schemapb.DataType_BinaryVector,
			},
			want: querypb.Type_NULL_TYPE,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, toSQLType(tt.args.t), "toSQLType(%v)", tt.args.t)
		})
	}
}
