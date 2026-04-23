package optimizers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks/util/searchutil/mock_optimizers"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type QueryHookSuite struct {
	suite.Suite
	queryHook QueryHook
}

func (suite *QueryHookSuite) SetupTest() {
}

func (suite *QueryHookSuite) TearDownTest() {
	suite.queryHook = nil
}

func (suite *QueryHookSuite) TestOptimizeSearchParam() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().AutoIndexConfig.EnableOptimize.Key, "true")

	suite.Run("normal_run", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		mockHook.EXPECT().Run(mock.Anything).Run(func(params map[string]any) {
			params[common.TopKKey] = int64(50)
			params[common.SearchParamKey] = `{"param": 2}`
			params[common.RecallEvalKey] = true
		}).Return(nil)
		suite.queryHook = mockHook
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
			suite.queryHook = nil
		}()

		getPlan := func(topk int64, groupByField int64) *planpb.PlanNode {
			return &planpb.PlanNode{
				Node: &planpb.PlanNode_VectorAnns{
					VectorAnns: &planpb.VectorANNS{
						QueryInfo: &planpb.QueryInfo{
							Topk:           topk,
							SearchParams:   `{"param": 1}`,
							GroupByFieldId: groupByField,
						},
					},
				},
			}
		}

		bs, err := proto.Marshal(getPlan(100, 101))
		suite.Require().NoError(err)

		req, err := OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
				IsTopkReduce:       true,
			},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.NoError(err)
		suite.verifyQueryInfo(req, 50, true, false, `{"param": 2}`)

		bs, err = proto.Marshal(getPlan(50, -1))
		suite.Require().NoError(err)
		req, err = OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
				IsTopkReduce:       true,
			},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.NoError(err)
		suite.verifyQueryInfo(req, 50, false, true, `{"param": 2}`)
	})

	suite.Run("disable optimization", func() {
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		suite.queryHook = mockHook
		defer func() { suite.queryHook = nil }()

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					QueryInfo: &planpb.QueryInfo{
						Topk:         100,
						SearchParams: `{"param": 1}`,
					},
				},
			},
		}
		bs, err := proto.Marshal(plan)
		suite.Require().NoError(err)

		req, err := OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
			},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.NoError(err)
		suite.verifyQueryInfo(req, 100, false, false, `{"param": 1}`)
	})

	suite.Run("no_hook", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
		suite.queryHook = nil
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					QueryInfo: &planpb.QueryInfo{
						Topk:         100,
						SearchParams: `{"param": 1}`,
					},
				},
			},
		}
		bs, err := proto.Marshal(plan)
		suite.Require().NoError(err)

		req, err := OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
				IsTopkReduce:       true,
			},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.NoError(err)
		suite.verifyQueryInfo(req, 100, false, false, `{"param": 1}`)
	})

	suite.Run("other_plannode", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		mockHook.EXPECT().Run(mock.Anything).Run(func(params map[string]any) {
			params[common.TopKKey] = int64(50)
			params[common.SearchParamKey] = `{"param": 2}`
		}).Return(nil).Maybe()
		suite.queryHook = mockHook
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
			suite.queryHook = nil
		}()

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{},
		}
		bs, err := proto.Marshal(plan)
		suite.Require().NoError(err)

		req, err := OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
			},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.NoError(err)
		suite.Equal(bs, req.GetReq().GetSerializedExprPlan())
	})

	suite.Run("no_serialized_plan", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		suite.queryHook = mockHook
		defer func() { suite.queryHook = nil }()

		_, err := OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req:             &internalpb.SearchRequest{},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.Error(err)
	})

	suite.Run("hook_run_error", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		mockHook.EXPECT().Run(mock.Anything).Run(func(params map[string]any) {
			params[common.TopKKey] = int64(50)
			params[common.SearchParamKey] = `{"param": 2}`
		}).Return(merr.WrapErrServiceInternal("mocked"))
		suite.queryHook = mockHook
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
			suite.queryHook = nil
		}()

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					QueryInfo: &planpb.QueryInfo{
						Topk:         100,
						SearchParams: `{"param": 1}`,
					},
				},
			},
		}
		bs, err := proto.Marshal(plan)
		suite.Require().NoError(err)

		_, err = OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
			},
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.Error(err)
	})

	suite.Run("global_refine_enabled", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.GlobalRefineEnable.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.GlobalRefineMinDimThreshold.Key, "256")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.GlobalRefineSearchTopkRatio.Key, "4")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.GlobalRefineRefineTopkRatio.Key, "2")
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		mockHook.EXPECT().Run(mock.Anything).Run(func(params map[string]any) {
			suite.Equal(float32(4), params[common.SearchTopkRatioKey])
			suite.Equal(float32(2), params[common.RefineTopkRatioKey])
			params[common.GlobalRefineKey] = true
		}).Return(nil)
		suite.queryHook = mockHook
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.GlobalRefineEnable.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.GlobalRefineMinDimThreshold.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.GlobalRefineSearchTopkRatio.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.GlobalRefineRefineTopkRatio.Key)
			suite.queryHook = nil
		}()

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					FieldId:    100,
					VectorType: planpb.VectorType_FloatVector,
					QueryInfo: &planpb.QueryInfo{
						Topk:           100,
						SearchParams:   `{"param": 1}`,
						GroupByFieldId: -1,
					},
				},
			},
		}
		bs, err := proto.Marshal(plan)
		suite.Require().NoError(err)

		req, err := OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
				SearchType:         internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER,
			},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(fieldID int64) int64 {
			suite.EqualValues(100, fieldID)
			return 512
		})
		suite.NoError(err)
		suite.verifyQueryInfo(req, 100, false, false, `{"param": 1}`)
		suite.verifyGlobalRefineRatios(req, 4, 2)
	})

	suite.Run("global_refine_ineligible", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.GlobalRefineEnable.Key, "true")
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		mockHook.EXPECT().Run(mock.Anything).Run(func(params map[string]any) {
			_, searchRatioExist := params[common.SearchTopkRatioKey]
			_, refineRatioExist := params[common.RefineTopkRatioKey]
			suite.False(searchRatioExist)
			suite.False(refineRatioExist)
		}).Return(nil)
		suite.queryHook = mockHook
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.GlobalRefineEnable.Key)
			suite.queryHook = nil
		}()

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					FieldId:    100,
					VectorType: planpb.VectorType_FloatVector,
					QueryInfo: &planpb.QueryInfo{
						Topk:           100,
						SearchParams:   `{"param": 1}`,
						GroupByFieldId: 101,
					},
				},
			},
		}
		bs, err := proto.Marshal(plan)
		suite.Require().NoError(err)

		req, err := OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
				SearchType:         internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER,
			},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.NoError(err)
		suite.verifyQueryInfo(req, 100, false, false, `{"param": 1}`)
		suite.verifyGlobalRefineRatios(req, 0, 0)
	})

	suite.Run("global_refine_skipped_for_non_pure_ann_search_type", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.GlobalRefineEnable.Key, "true")
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.GlobalRefineMinDimThreshold.Key, "256")
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		mockHook.EXPECT().Run(mock.Anything).Run(func(params map[string]any) {
			_, searchRatioExist := params[common.SearchTopkRatioKey]
			_, refineRatioExist := params[common.RefineTopkRatioKey]
			suite.False(searchRatioExist)
			suite.False(refineRatioExist)
		}).Return(nil)
		suite.queryHook = mockHook
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.GlobalRefineEnable.Key)
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.GlobalRefineMinDimThreshold.Key)
			suite.queryHook = nil
		}()

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					FieldId:    100,
					VectorType: planpb.VectorType_FloatVector,
					QueryInfo: &planpb.QueryInfo{
						Topk:           100,
						SearchParams:   `{"param": 1}`,
						GroupByFieldId: -1,
					},
				},
			},
		}
		bs, err := proto.Marshal(plan)
		suite.Require().NoError(err)

		req, err := OptimizeSearchParams(ctx, &querypb.SearchRequest{
			Req: &internalpb.SearchRequest{
				SerializedExprPlan: bs,
				SearchType:         internalpb.SearchType_DEFAULT,
			},
			TotalChannelNum: 2,
		}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		suite.NoError(err)
		suite.verifyQueryInfo(req, 100, false, false, `{"param": 1}`)
		suite.verifyGlobalRefineRatios(req, 0, 0)
	})

	suite.Run("global_refine_type_assertion_panic", func() {
		paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
		mockHook := mock_optimizers.NewMockQueryHook(suite.T())
		mockHook.EXPECT().Run(mock.Anything).Run(func(params map[string]any) {
			params[common.GlobalRefineKey] = "true"
		}).Return(nil)
		suite.queryHook = mockHook
		defer func() {
			paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.Enable.Key)
			suite.queryHook = nil
		}()

		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_VectorAnns{
				VectorAnns: &planpb.VectorANNS{
					QueryInfo: &planpb.QueryInfo{
						Topk:         100,
						SearchParams: `{"param": 1}`,
					},
				},
			},
		}
		bs, err := proto.Marshal(plan)
		suite.Require().NoError(err)

		suite.Panics(func() {
			_, _ = OptimizeSearchParams(ctx, &querypb.SearchRequest{
				Req: &internalpb.SearchRequest{
					SerializedExprPlan: bs,
				},
			}, suite.queryHook, 2, false, func(int64) int64 { return 512 })
		})
	})
}

func (suite *QueryHookSuite) verifyQueryInfo(req *querypb.SearchRequest, topK int64, isTopkReduce bool, isRecallEvaluation bool, param string) {
	queryInfo := suite.getQueryInfo(req)
	suite.Equal(topK, queryInfo.GetTopk())
	suite.Equal(param, queryInfo.GetSearchParams())
	suite.Equal(isTopkReduce, req.GetReq().GetIsTopkReduce())
	suite.Equal(isRecallEvaluation, req.GetReq().GetIsRecallEvaluation())
}

func (suite *QueryHookSuite) verifyGlobalRefineRatios(req *querypb.SearchRequest, searchTopkRatio float32, refineTopkRatio float32) {
	queryInfo := suite.getQueryInfo(req)
	suite.Equal(searchTopkRatio, queryInfo.GetSearchTopkRatio())
	suite.Equal(refineTopkRatio, queryInfo.GetRefineTopkRatio())
}

func (suite *QueryHookSuite) getQueryInfo(req *querypb.SearchRequest) *planpb.QueryInfo {
	planBytes := req.GetReq().GetSerializedExprPlan()

	plan := planpb.PlanNode{}
	err := proto.Unmarshal(planBytes, &plan)
	suite.Require().NoError(err)

	return plan.GetVectorAnns().GetQueryInfo()
}

func TestOptimizeSearchParam(t *testing.T) {
	suite.Run(t, new(QueryHookSuite))
}
