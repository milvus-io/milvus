package optimizers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// QueryHook is the interface for search/query parameter optimizer.
type QueryHook interface {
	Run(map[string]any) error
	Init(string) error
	InitTuningConfig(map[string]string) error
	DeleteTuningConfig(string) error
}

// OptimizeSearchParams optimizes search parameters using the query hook and applies Knowhere search defaults.
func OptimizeSearchParams(ctx context.Context, req *querypb.SearchRequest, queryHook QueryHook, numSegments int, indexType string) (*querypb.SearchRequest, error) {
	useQueryHook := queryHook != nil && paramtable.Get().AutoIndexConfig.Enable.GetAsBool()
	useKnowhereDefaults := paramtable.Get().KnowhereConfig.Enable.GetAsBool() &&
		paramtable.Get().KnowhereConfig.HasIndexParams(indexType, paramtable.SearchStage)
	if !useQueryHook {
		req.Req.IsTopkReduce = false
		req.Req.IsRecallEvaluation = false
	}

	collectionId := req.GetReq().GetCollectionID()
	log := log.Ctx(ctx).With(zap.Int64("collection", collectionId))

	serializedPlan := req.GetReq().GetSerializedExprPlan()
	// plan not found
	if serializedPlan == nil {
		if !useQueryHook && !useKnowhereDefaults {
			return req, nil
		}
		log.Warn("serialized plan not found")
		return req, merr.WrapErrParameterInvalid("serialized search plan", "nil")
	}

	channelNum := req.GetTotalChannelNum()
	// not set, change to conservative channel num 1
	if channelNum <= 0 {
		channelNum = 1
	}

	plan := planpb.PlanNode{}
	err := proto.Unmarshal(serializedPlan, &plan)
	if err != nil {
		log.Warn("failed to unmarshal plan", zap.Error(err))
		return nil, merr.WrapErrParameterInvalid("valid serialized search plan", "no unmarshalable one", err.Error())
	}

	switch plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		queryInfo := plan.GetVectorAnns().GetQueryInfo()
		if queryInfo == nil {
			return nil, merr.WrapErrParameterInvalidMsg("missing search query info")
		}
		var params map[string]any
		if useQueryHook {
			// use shardNum * segments num in shard to estimate total segment number
			estSegmentNum := numSegments * int(channelNum)
			metrics.QueryNodeSearchHitSegmentNum.WithLabelValues(fmt.Sprint(paramtable.GetNodeID()), fmt.Sprint(collectionId), metrics.SearchLabel).Observe(float64(estSegmentNum))

			withFilter := (plan.GetVectorAnns().GetPredicates() != nil)
			params = map[string]any{
				common.TopKKey:         queryInfo.GetTopk(),
				common.SearchParamKey:  queryInfo.GetSearchParams(),
				common.SegmentNumKey:   estSegmentNum,
				common.WithFilterKey:   withFilter,
				common.DataTypeKey:     int32(plan.GetVectorAnns().GetVectorType()),
				common.WithOptimizeKey: paramtable.Get().AutoIndexConfig.EnableOptimize.GetAsBool() && req.GetReq().GetIsTopkReduce() && queryInfo.GetGroupByFieldId() < 0,
				common.CollectionKey:   req.GetReq().GetCollectionID(),
				common.RecallEvalKey:   req.GetReq().GetIsRecallEvaluation(),
			}
			if withFilter && channelNum > 1 {
				params[common.ChannelNumKey] = channelNum
			}
			if err := queryHook.Run(params); err != nil {
				log.Warn("failed to execute queryHook", zap.Error(err))
				return nil, merr.WrapErrServiceUnavailable(err.Error(), "queryHook execution failed")
			}
			finalTopk := params[common.TopKKey].(int64)
			req.Req.IsTopkReduce = req.GetReq().GetIsTopkReduce() && (finalTopk < queryInfo.GetTopk())
			queryInfo.Topk = finalTopk
			if isRecallEvaluation, ok := params[common.RecallEvalKey]; ok {
				req.Req.IsRecallEvaluation = isRecallEvaluation.(bool) && queryInfo.GetGroupByFieldId() < 0
			} else {
				req.Req.IsRecallEvaluation = false
			}
		}

		if useKnowhereDefaults {
			if params == nil {
				params = map[string]any{common.SearchParamKey: queryInfo.GetSearchParams()}
			}
			if err := paramtable.Get().KnowhereConfig.MergeIndexParamsJSON(indexType, paramtable.SearchStage, params); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("invalid search params: %s", err.Error())
			}
		}
		if params != nil {
			queryInfo.SearchParams = params[common.SearchParamKey].(string)
		}

		changed, err := applyStrictGroupSettings(ctx, queryInfo)
		if err != nil {
			return nil, err
		}
		if useQueryHook || useKnowhereDefaults || changed {
			serializedExprPlan, err := proto.Marshal(&plan)
			if err != nil {
				log.Warn("failed to marshal optimized plan", zap.Error(err))
				return nil, merr.WrapErrParameterInvalid("marshalable search plan", "plan with marshal error", err.Error())
			}
			req.Req.SerializedExprPlan = serializedExprPlan
		}
		log.Debug("optimized search params done", zap.Any("queryInfo", queryInfo))
	default:
		log.Warn("not supported node type", zap.String("nodeType", fmt.Sprintf("%T", plan.GetNode())))
	}
	return req, nil
}

// applyStrictGroupSettings runs after the hook, including when it is disabled.
// Server settings override caller/hook values; unrelated JSON values retain
// their exact numeric/string types. The serialized plan freezes this snapshot.
func applyStrictGroupSettings(ctx context.Context, info *planpb.QueryInfo) (bool, error) {
	raw := info.GetSearchParams()
	if raw == "" {
		raw = "{}"
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &params); err != nil {
		return false, merr.WrapErrParameterInvalidMsg("invalid search params: %s", err)
	}
	if params == nil {
		params = make(map[string]json.RawMessage)
	}
	_, hadStrategy := params[common.StrictGroupStrategyKey]
	_, hadDebug := params[common.StrictGroupDebugKey]
	_, hadPhase1 := params[common.StrictGroupPhase1CandidateWeightKey]
	_, hadSkipRefine := params[common.StrictGroupSkipRefineKey]
	delete(params, common.StrictGroupStrategyKey)
	delete(params, common.StrictGroupDebugKey)
	delete(params, common.StrictGroupPhase1CandidateWeightKey)
	delete(params, common.StrictGroupSkipRefineKey)
	eligible := info.GetStrictGroupSize() && info.GetGroupSize() > 1 && info.GetGroupByFieldId() > 0
	if eligible {
		cfg := &paramtable.Get().QueryNodeCfg
		phase1, err := strconv.ParseInt(cfg.StrictGroupPhase1CandidateWeight.GetValue(), 10, 64)
		if err != nil || phase1 < 0 {
			return false, merr.WrapErrServiceUnavailable("invalid server config: " + cfg.StrictGroupPhase1CandidateWeight.Key)
		}
		skipRefine, err := strconv.ParseBool(cfg.StrictGroupSkipRefine.GetValue())
		if err != nil {
			return false, merr.WrapErrServiceUnavailable("invalid server config: " + cfg.StrictGroupSkipRefine.Key)
		}
		params[common.StrictGroupPhase1CandidateWeightKey] = json.RawMessage(strconv.FormatInt(phase1, 10))
		params[common.StrictGroupSkipRefineKey] = json.RawMessage(strconv.FormatBool(skipRefine))
		debug, err := strconv.ParseBool(cfg.StrictGroupDebug.GetValue())
		if err != nil {
			return false, merr.WrapErrServiceUnavailable("invalid server config: " + cfg.StrictGroupDebug.Key)
		}
		params[common.StrictGroupDebugKey] = json.RawMessage(strconv.FormatBool(debug))
		strategy := cfg.StrictGroupStrategy.GetValue()
		if strategy != "original" && strategy != "per_group" {
			return false, merr.WrapErrServiceUnavailable("invalid server config: " + cfg.StrictGroupStrategy.Key)
		}
		params[common.StrictGroupStrategyKey] = json.RawMessage(strconv.Quote(strategy))
		if debug {
			// Log the exact snapshot injected after the hook, not another config
			// read that might race a refresh. Never log caller search payloads.
			log.Ctx(ctx).Info("strict_group_config_snapshot",
				zap.Int64("node_id", paramtable.GetNodeID()),
				zap.String("strategy", strategy),
				zap.Int64("phase1_candidate_weight", phase1),
				zap.Bool("skip_refine", skipRefine),
				zap.Bool("strict_group_debug", debug))
		}
	}
	if !eligible && !hadStrategy && !hadDebug && !hadPhase1 && !hadSkipRefine {
		return false, nil
	}
	encoded, err := json.Marshal(params)
	if err != nil {
		return false, err
	}
	info.SearchParams = string(encoded)
	return true, nil
}
