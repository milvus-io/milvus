package executor

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/mysqld/parser/antlrparser"

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

func Test_defaultExecutor_execANNS(t *testing.T) {
	f1 := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "pk",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{6, 5, 4, 3, 2, 1},
					},
				},
			},
		},
	}
	f2 := &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: "random",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: []float32{6.6, 5.5, 4.4, 3.3, 2.2, 1.1},
					},
				},
			},
		},
	}

	res := &milvuspb.SearchResults{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Results: &schemapb.SearchResultData{
			NumQueries: 2,
			TopK:       3,
			FieldsData: []*schemapb.FieldData{f1, f2},
			Scores:     []float32{1.1, 2.2, 3.3, 4.4, 5.5, 6.6},
			Topks:      []int64{2, 2},
		},
		CollectionName: "hello_milvus",
	}
	s := mocks.NewProxyComponent(t)
	s.On("Search",
		mock.Anything, // context.Context
		mock.Anything, // *milvuspb.SearchRequest
	).Return(res, nil)

	sql := `
select 
$query_number, pk, random, $distance
from hello_milvus
where random > 0.5
anns by embeddings -> ([0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8], [0.8, 0.7, 0.6, 0.6, 0.4, 0.3, 0.2, 0.1])
params = (metric_type=L2, nprobe=10)
limit 3
`
	plan, warns, err := antlrparser.NewAntlrParser().Parse(sql)
	assert.NoError(t, err)
	assert.Nil(t, warns)

	e := NewDefaultExecutor(s).(*defaultExecutor)
	_, err = e.execANNS(context.TODO(),
		antlrparser.GetSqlStatements(plan.Node).Statements[0].DmlStatement.Unwrap().SelectStatement.Unwrap().SimpleSelect.Unwrap().Query.Unwrap(),
		[]string{"$query_number", "pk", "random", "$distance"})
	assert.NoError(t, err)
}
