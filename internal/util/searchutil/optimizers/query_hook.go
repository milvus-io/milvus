package optimizers

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
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
	CalculateEffectiveSegmentNum(rowCounts []int64, topk int64) int
}

// OptimizeSearchParams optimizes search parameters using the query hook.
// numSegments is the effective segment number, pre-computed by the caller via CalculateEffectiveSegmentNum.
// isSecondStageSearch is true for the vector search stage of two-stage search, refer to delegator_twostage.go.
// At this time, we need to set WithFilterKey to false to allow some aggressive optimizations.
func OptimizeSearchParams(ctx context.Context, req *querypb.SearchRequest, queryHook QueryHook, numSegments int, isSecondStageSearch bool) (*querypb.SearchRequest, error) {
	// no hook applied or disabled, just return
	if queryHook == nil || !paramtable.Get().AutoIndexConfig.Enable.GetAsBool() {
		req.Req.IsTopkReduce = false
		req.Req.IsRecallEvaluation = false
		return req, nil
	}

	collectionId := req.GetReq().GetCollectionID()
	log := log.Ctx(ctx).With(zap.Int64("collection", collectionId))

	serializedPlan := req.GetReq().GetSerializedExprPlan()
	// plan not found
	if serializedPlan == nil {
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
		// use shardNum * segments num in shard to estimate total segment number
		estSegmentNum := numSegments * int(channelNum)
		metrics.QueryNodeSearchHitSegmentNum.WithLabelValues(paramtable.GetStringNodeID(), fmt.Sprint(collectionId), metrics.SearchLabel).Observe(float64(estSegmentNum))

		withFilter := (plan.GetVectorAnns().GetPredicates() != nil)
		queryInfo := plan.GetVectorAnns().GetQueryInfo()
		params := map[string]any{
			common.TopKKey:         queryInfo.GetTopk(),
			common.SearchParamKey:  queryInfo.GetSearchParams(),
			common.SegmentNumKey:   estSegmentNum,
			common.WithFilterKey:   withFilter && !isSecondStageSearch,
			common.DataTypeKey:     int32(plan.GetVectorAnns().GetVectorType()),
			common.WithOptimizeKey: paramtable.Get().AutoIndexConfig.EnableOptimize.GetAsBool() && req.GetReq().GetIsTopkReduce() && queryInfo.GetGroupByFieldId() < 0,
			common.CollectionKey:   req.GetReq().GetCollectionID(),
			common.RecallEvalKey:   req.GetReq().GetIsRecallEvaluation(),
		}
		if withFilter && channelNum > 1 {
			params[common.ChannelNumKey] = channelNum
		}
		err := queryHook.Run(params)
		if err != nil {
			log.Warn("failed to execute queryHook", zap.Error(err))
			return nil, merr.WrapErrServiceUnavailable(err.Error(), "queryHook execution failed")
		}
		finalTopk := params[common.TopKKey].(int64)
		isTopkReduce := req.GetReq().GetIsTopkReduce() && (finalTopk < queryInfo.GetTopk()) && !isSecondStageSearch
		queryInfo.Topk = finalTopk
		queryInfo.SearchParams = params[common.SearchParamKey].(string)
		serializedExprPlan, err := proto.Marshal(&plan)
		if err != nil {
			log.Warn("failed to marshal optimized plan", zap.Error(err))
			return nil, merr.WrapErrParameterInvalid("marshalable search plan", "plan with marshal error", err.Error())
		}
		req.Req.SerializedExprPlan = serializedExprPlan
		req.Req.IsTopkReduce = isTopkReduce
		if isRecallEvaluation, ok := params[common.RecallEvalKey]; ok {
			req.Req.IsRecallEvaluation = isRecallEvaluation.(bool) && queryInfo.GetGroupByFieldId() < 0
		} else {
			req.Req.IsRecallEvaluation = false
		}
		log.Debug("optimized search params done", zap.Any("queryInfo", queryInfo))
	default:
		log.Warn("not supported node type", zap.String("nodeType", fmt.Sprintf("%T", plan.GetNode())))
	}
	return req, nil
}

// CalculateEffectiveSegmentNum delegates to queryHook.CalculateEffectiveSegmentNum when
// a hook is available; otherwise returns len(rowCounts) (the raw sealed segment count).
func CalculateEffectiveSegmentNum(queryHook QueryHook, rowCounts []int64, topk int64) int {
	if queryHook != nil && paramtable.Get().AutoIndexConfig.Enable.GetAsBool() {
		return queryHook.CalculateEffectiveSegmentNum(rowCounts, topk)
	}
	return len(rowCounts)
}

// ShouldUseTwoStageSearch determines if two-stage search should be used for this request
// based on paramtable config, segment count, topk, and search type.
func ShouldUseTwoStageSearch(req *querypb.SearchRequest, effectiveSegmentNum int) bool {
	if !paramtable.Get().AutoIndexConfig.TwoStageSearchEnabled.GetAsBool() {
		return false
	}
	if effectiveSegmentNum < paramtable.Get().AutoIndexConfig.TwoStageSearchMinNumSegments.GetAsInt() && req.GetReq().GetTopk() < paramtable.Get().AutoIndexConfig.TwoStageSearchMinTopk.GetAsInt64() {
		return false
	}
	return req.GetReq().GetSearchType() == internalpb.SearchType_PURE_ANN_SEARCH_WITH_FILTER
}
