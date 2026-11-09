package delegator

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func SetBM25Params(req *internalpb.SearchRequest, avgdl float64) error {
	log := mlog.With(mlog.Int64("collection", req.GetCollectionID()))

	serializedPlan := req.GetSerializedExprPlan()
	// plan not found
	if serializedPlan == nil {
		log.Warn(context.TODO(), "serialized plan not found")
		return merr.WrapErrParameterInvalid("serialized search plan", "nil")
	}

	plan := planpb.PlanNode{}
	err := proto.Unmarshal(serializedPlan, &plan)
	if err != nil {
		log.Warn(context.TODO(), "failed to unmarshal plan", mlog.Err(err))
		return merr.WrapErrParameterInvalid("valid serialized search plan", "no unmarshalable one", err.Error())
	}

	switch plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		queryInfo := plan.GetVectorAnns().GetQueryInfo()
		queryInfo.Bm25Avgdl = avgdl
		serializedExprPlan, err := proto.Marshal(&plan)
		if err != nil {
			log.Warn(context.TODO(), "failed to marshal optimized plan", mlog.Err(err))
			return merr.WrapErrParameterInvalid("marshalable search plan", "plan with marshal error", err.Error())
		}
		req.SerializedExprPlan = serializedExprPlan
		log.Debug(context.TODO(), "add bm25 avgdl to search params done", mlog.Any("queryInfo", queryInfo))
	default:
		log.Warn(context.TODO(), "not supported node type", mlog.String("nodeType", fmt.Sprintf("%T", plan.GetNode())))
	}
	return nil
}
