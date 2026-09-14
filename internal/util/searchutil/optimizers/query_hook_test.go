package optimizers

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/mocks/util/searchutil/mock_optimizers"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
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
		}, suite.queryHook, 2)
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
		}, suite.queryHook, 2)
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
		}, suite.queryHook, 2)
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
		}, suite.queryHook, 2)
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
		}, suite.queryHook, 2)
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
		}, suite.queryHook, 2)
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
		}, suite.queryHook, 2)
		suite.Error(err)
	})
}

func (suite *QueryHookSuite) verifyQueryInfo(req *querypb.SearchRequest, topK int64, isTopkReduce bool, isRecallEvaluation bool, param string) {
	planBytes := req.GetReq().GetSerializedExprPlan()

	plan := planpb.PlanNode{}
	err := proto.Unmarshal(planBytes, &plan)
	suite.Require().NoError(err)

	queryInfo := plan.GetVectorAnns().GetQueryInfo()
	suite.Equal(topK, queryInfo.GetTopk())
	suite.Equal(param, queryInfo.GetSearchParams())
	suite.Equal(isTopkReduce, req.GetReq().GetIsTopkReduce())
	suite.Equal(isRecallEvaluation, req.GetReq().GetIsRecallEvaluation())
}

func TestOptimizeSearchParam(t *testing.T) {
	suite.Run(t, new(QueryHookSuite))
}

func (suite *QueryHookSuite) TestStrictGroupConfigSnapshotLog() {
	paramtable.Init()
	cfg := paramtable.Get()
	q := &cfg.QueryNodeCfg
	defer cfg.Reset(q.StrictGroupDebug.Key)
	defer cfg.Reset(q.StrictGroupStrategy.Key)
	core, observed := observer.New(zap.InfoLevel)
	ctx := context.WithValue(context.Background(), log.CtxLogKey, &log.MLogger{Logger: zap.New(core)})
	for _, debug := range []bool{false, true, false} {
		cfg.Save(q.StrictGroupDebug.Key, strconv.FormatBool(debug))
		cfg.Save(q.StrictGroupStrategy.Key, "original")
		info := &planpb.QueryInfo{
			Topk: 1, GroupByFieldId: 101, GroupSize: 3, StrictGroupSize: true,
			SearchParams: `{"private_payload":"must-not-be-logged"}`,
		}
		changed, err := applyStrictGroupSettings(ctx, info)
		suite.Require().NoError(err)
		suite.True(changed)
		entries := observed.TakeAll()
		if !debug {
			suite.Empty(entries)
			continue
		}
		suite.Require().Len(entries, 1)
		suite.Equal("strict_group_config_snapshot", entries[0].Message)
		fields := entries[0].ContextMap()
		suite.Equal("original", fields["strategy"])
		suite.NotContains(fields, "private_payload")
		suite.NotContains(fields, "search_params")
	}
}

func (suite *QueryHookSuite) TestStrictGroupServerSettings() {
	paramtable.Init()
	cfg := paramtable.Get()
	sKey := cfg.QueryNodeCfg.StrictGroupStrategy.Key
	dKey := cfg.QueryNodeCfg.StrictGroupDebug.Key
	defer cfg.Reset(sKey)
	defer cfg.Reset(dKey)
	defer cfg.Reset(cfg.AutoIndexConfig.Enable.Key)
	makeRequest := func(strict bool, raw string) *querypb.SearchRequest {
		p := &planpb.PlanNode{Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{
			QueryInfo: &planpb.QueryInfo{
				Topk: 10, GroupByFieldId: 101,
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
	raw := `{"large":9007199254740993,"text":"0.5","strict_group_strategy":"invalid-client","strict_group_debug":"invalid-client"}`
	// Exercise no hook, AutoIndex disabled, a hook dropping all caller keys,
	// and a hook injecting conflicting/invalid values.
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
			cfg.Save(sKey, "per_group")
			cfg.Save(dKey, "true")
			req, err := OptimizeSearchParams(context.Background(), makeRequest(true, raw), hook, 1)
			suite.Require().NoError(err)
			values := readParams(req)
			suite.Equal(`"per_group"`, string(values[common.StrictGroupStrategyKey]))
			suite.Equal("true", string(values[common.StrictGroupDebugKey]))
			suite.Equal("9007199254740993", string(values["large"]))
			suite.Equal(`"0.5"`, string(values["text"]))
			// Updating config affects a later request, not the serialized snapshot.
			cfg.Save(sKey, "original")
			cfg.Save(dKey, "false")
			next, err := OptimizeSearchParams(context.Background(), makeRequest(true, raw), nil, 1)
			suite.Require().NoError(err)
			suite.Equal(`"original"`, string(readParams(next)[common.StrictGroupStrategyKey]))
			suite.Equal(`"per_group"`, string(readParams(req)[common.StrictGroupStrategyKey]))
			suite.Equal("false", string(readParams(next)[common.StrictGroupDebugKey]))
			suite.Equal("true", string(readParams(req)[common.StrictGroupDebugKey]))
		}
	}
	cfg.Reset(sKey)
	defaultReq, err := OptimizeSearchParams(context.Background(), makeRequest(true, "{}"), nil, 1)
	suite.Require().NoError(err)
	suite.Equal(`"per_group"`, string(readParams(defaultReq)[common.StrictGroupStrategyKey]))
	suite.Equal("false", string(readParams(defaultReq)[common.StrictGroupDebugKey]))
	// All strategies are server controlled, including direct union filtering.
	for _, strategy := range []string{"per_group", "original"} {
		cfg.Save(sKey, strategy)
		req, err := OptimizeSearchParams(context.Background(), makeRequest(true, raw), nil, 1)
		suite.Require().NoError(err)
		suite.Equal(strconv.Quote(strategy), string(readParams(req)[common.StrictGroupStrategyKey]))
	}
	cfg.Reset(sKey)
	// Caller-controlled values are removed even on non-strict queries.
	plain, err := OptimizeSearchParams(context.Background(), makeRequest(false, raw), nil, 1)
	suite.Require().NoError(err)
	suite.NotContains(readParams(plain), common.StrictGroupStrategyKey)
	suite.NotContains(readParams(plain), common.StrictGroupDebugKey)
	for key, badValues := range map[string][]string{
		sKey: {"", "PER_GROUP", "other", "1", "sampling", "filtered_iterator"},
		dKey: {"", "other", "0.5"},
	} {
		for _, value := range badValues {
			cfg.Save(key, value)
			_, err := OptimizeSearchParams(context.Background(), makeRequest(true, "{}"), nil, 1)
			suite.ErrorIs(err, merr.ErrServiceUnavailable)
			cfg.Reset(key)
		}
	}
	for _, raw := range []string{"invalid", "[]", "1"} {
		_, err := OptimizeSearchParams(context.Background(), makeRequest(true, raw), nil, 1)
		suite.Error(err)
	}
}

func (suite *QueryHookSuite) TestStrictGroupPhase1AndRefineSettings() {
	paramtable.Init()
	cfg := paramtable.Get()
	budgetKey := cfg.QueryNodeCfg.StrictGroupPhase1MaxCandidates.Key
	skipKey := cfg.QueryNodeCfg.StrictGroupSkipRefine.Key
	defer cfg.Reset(budgetKey)
	defer cfg.Reset(skipKey)
	var previous *planpb.QueryInfo
	for _, budget := range []string{"0", "7000", "13", "0"} {
		for _, skip := range []string{"false", "true"} {
			cfg.Save(budgetKey, budget)
			cfg.Save(skipKey, skip)
			info := &planpb.QueryInfo{
				Topk: 50, GroupByFieldId: 101, GroupSize: 3, StrictGroupSize: true,
				SearchParams: `{"strict_group_phase1_max_candidates":"bad","strict_group_skip_refine":"bad","nprobe":128}`,
			}
			before := ""
			if previous != nil {
				before = previous.SearchParams
			}
			changed, err := applyStrictGroupSettings(context.Background(), info)
			suite.Require().NoError(err)
			suite.True(changed)
			var values map[string]json.RawMessage
			suite.Require().NoError(json.Unmarshal([]byte(info.SearchParams), &values))
			suite.Equal(budget, string(values[common.StrictGroupPhase1MaxCandidatesKey]))
			suite.Equal(skip, string(values[common.StrictGroupSkipRefineKey]))
			suite.Equal("128", string(values["nprobe"]))
			if previous != nil {
				suite.Equal(before, previous.SearchParams)
			}
			previous = info
			for _, strict := range []bool{false, true} {
				ineligible := &planpb.QueryInfo{
					GroupByFieldId: 101, GroupSize: 1, StrictGroupSize: strict,
					SearchParams: `{"strict_group_phase1_max_candidates":5,"strict_group_skip_refine":true}`,
				}
				_, err = applyStrictGroupSettings(context.Background(), ineligible)
				suite.Require().NoError(err)
				suite.Equal("{}", ineligible.SearchParams)
			}
		}
	}
	for key, bad := range map[string][]string{
		budgetKey: {"-1", "1.5", "9223372036854775808", "bad"},
		skipKey:   {"bad", "0.5", ""},
	} {
		for _, value := range bad {
			cfg.Save(key, value)
			_, err := applyStrictGroupSettings(context.Background(), &planpb.QueryInfo{
				GroupByFieldId: 101, GroupSize: 3, StrictGroupSize: true,
			})
			suite.ErrorIs(err, merr.ErrServiceUnavailable)
			cfg.Reset(key)
		}
	}
}
