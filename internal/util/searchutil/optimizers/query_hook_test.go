package optimizers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks/util/searchutil/mock_optimizers"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

func TestShouldUseTwoStageSearch(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key, "2000")
	paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key, "5")
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key)
		paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.Key)
		paramtable.Get().Reset(paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.Key)
	})

	tests := []struct {
		name                string
		twoStageEnabled     string
		topk                int64
		effectiveSegmentNum int
		searchType          internalpb.SearchType
		want                bool
	}{
		{
			name:                "disabled",
			twoStageEnabled:     "false",
			topk:                3000,
			effectiveSegmentNum: 10,
			searchType:          internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			want:                false,
		},
		{
			name:                "segments_below_threshold",
			topk:                3000,
			effectiveSegmentNum: 3,
			searchType:          internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			want:                false,
		},
		{
			name:                "topk_below_threshold",
			topk:                1000,
			effectiveSegmentNum: 10,
			searchType:          internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			want:                false,
		},
		{
			name:                "thresholds_met_exactly",
			topk:                2000,
			effectiveSegmentNum: 5,
			searchType:          internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			want:                true,
		},
		{
			name:                "thresholds_exceeded",
			topk:                3000,
			effectiveSegmentNum: 10,
			searchType:          internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER,
			want:                true,
		},
		{
			name:                "wrong_search_type",
			topk:                3000,
			effectiveSegmentNum: 10,
			searchType:          internalpb.SearchType_PURE_ANN_SEARCH_NO_FILTER,
			want:                false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.twoStageEnabled != "" {
				paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, test.twoStageEnabled)
				t.Cleanup(func() {
					paramtable.Get().Save(paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.Key, "true")
				})
			}

			req := &querypb.SearchRequest{
				Req: &internalpb.SearchRequest{
					Topk:       test.topk,
					SearchType: test.searchType,
				},
			}
			if got := ShouldUseTwoStageSearch(req, test.effectiveSegmentNum); got != test.want {
				t.Fatalf("ShouldUseTwoStageSearch() = %v, want %v", got, test.want)
			}
		})
	}
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

func (suite *QueryHookSuite) TestStrictGroupServerSettings() {
	for _, tc := range []struct {
		name     string
		singular int64
		plural   []int64
	}{
		{"legacy singular", 101, nil},
		{"plural with unset singular", 0, []int64{101}},
		{"aggregation plural", -1, []int64{101}},
		{"both representations", 101, []int64{101}},
		{"multiple fields", -1, []int64{101, 102}},
	} {
		suite.Run(tc.name, func() {
			suite.checkStrictGroupServerSettings(tc.singular, tc.plural)
		})
	}
}

func (suite *QueryHookSuite) checkStrictGroupServerSettings(singular int64, plural []int64) {
	paramtable.Init()
	cfg := paramtable.Get()
	vKey := cfg.QueryNodeCfg.StrictGroupAcceptanceThreshold.Key
	tKey := cfg.QueryNodeCfg.StrictGroupProbeCandidates.Key
	defer cfg.Reset(vKey)
	defer cfg.Reset(tKey)
	defer cfg.Reset(cfg.AutoIndexConfig.Enable.Key)
	makeRequest := func(strict bool, raw string) *querypb.SearchRequest {
		p := &planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{
			QueryInfo: &planpb.QueryInfo{
				Topk: 10, GroupByFieldId: singular, GroupByFieldIds: plural,
				GroupSize: 3, StrictGroupSize: strict, SearchParams: raw,
			},
		}}}
		bs, err := proto.Marshal(p)
		suite.Require().NoError(err)
		return &querypb.SearchRequest{Req: &internalpb.SearchRequest{SerializedExprPlan: bs}}
	}
	readParams := func(req *querypb.SearchRequest) map[string]json.RawMessage {
		p := &planpb.PlanNode{}
		suite.Require().NoError(proto.Unmarshal(req.GetReq().GetSerializedExprPlan(), p))
		var values map[string]json.RawMessage
		suite.Require().NoError(json.Unmarshal([]byte(p.GetVectorAnns().GetQueryInfo().GetSearchParams()), &values))
		return values
	}
	raw := `{"large":9007199254740993,"text":"0.5","strict_group_acceptance_threshold":"bad","strict_group_probe_candidates":-1}`
	// Exercise no hook, AutoIndex disabled, a hook dropping all caller keys,
	// and a hook injecting conflicting/invalid values.
	for _, secondStage := range []bool{false, true} {
		for _, enabled := range []string{"false", "true"} {
			cfg.Save(cfg.AutoIndexConfig.Enable.Key, enabled)
			for _, hookOutput := range []string{"none", `{"large":9007199254740993,"text":"0.5"}`, raw} {
				var hook QueryHook
				if hookOutput != "none" {
					h := mock_optimizers.NewMockQueryHook(suite.T())
					if enabled == "true" {
						h.EXPECT().Run(mock.Anything).Run(func(p map[string]any) {
							p[common.SearchParamKey] = hookOutput
						}).Return(nil)
					}
					hook = h
				}
				cfg.Save(vKey, "0.5")
				cfg.Save(tKey, "17")
				req, err := OptimizeSearchParams(context.Background(), makeRequest(true, raw), hook, 1, secondStage, func(int64) int64 { return 128 })
				suite.Require().NoError(err)
				values := readParams(req)
				suite.Equal("0.5", string(values[common.StrictGroupAcceptanceThresholdKey]))
				suite.Equal("17", string(values[common.StrictGroupProbeCandidatesKey]))
				suite.Equal("9007199254740993", string(values["large"]))
				suite.Equal(`"0.5"`, string(values["text"]))
				// Updating config affects a later request, not the serialized snapshot.
				cfg.Save(vKey, "0")
				cfg.Save(tKey, "1")
				next, err := OptimizeSearchParams(context.Background(), makeRequest(true, raw), nil, 1, false, func(int64) int64 { return 128 })
				suite.Require().NoError(err)
				suite.Equal("0", string(readParams(next)[common.StrictGroupAcceptanceThresholdKey]))
				suite.Equal("1", string(readParams(next)[common.StrictGroupProbeCandidatesKey]))
				suite.Equal("17", string(readParams(req)[common.StrictGroupProbeCandidatesKey]))
			}
		}
	}
	cfg.Reset(vKey)
	cfg.Reset(tKey)
	defaultReq, err := OptimizeSearchParams(context.Background(), makeRequest(true, "{}"), nil, 1, false, func(int64) int64 { return 128 })
	suite.Require().NoError(err)
	suite.Equal("0.1", string(readParams(defaultReq)[common.StrictGroupAcceptanceThresholdKey]))
	suite.Equal("100", string(readParams(defaultReq)[common.StrictGroupProbeCandidatesKey]))
	// Caller-controlled values are removed even on non-strict queries.
	plain, err := OptimizeSearchParams(context.Background(), makeRequest(false, raw), nil, 1, false, func(int64) int64 { return 128 })
	suite.Require().NoError(err)
	suite.NotContains(readParams(plain), common.StrictGroupAcceptanceThresholdKey)
	suite.NotContains(readParams(plain), common.StrictGroupProbeCandidatesKey)
	for key, badValues := range map[string][]string{
		vKey: {"-0.1", "1.1", "NaN", "+Inf", "bad"},
		tKey: {"0", "-1", "1.5", "9223372036854775808", "bad"},
	} {
		for _, value := range badValues {
			cfg.Save(key, value)
			_, err := OptimizeSearchParams(context.Background(), makeRequest(true, "{}"), nil, 1, false, func(int64) int64 { return 128 })
			suite.ErrorIs(err, merr.ErrServiceUnavailable)
			cfg.Reset(key)
		}
	}
	for _, raw := range []string{"invalid", "[]", "1"} {
		_, err := OptimizeSearchParams(context.Background(), makeRequest(true, raw), nil, 1, false, func(int64) int64 { return 128 })
		suite.Error(err)
	}
}
