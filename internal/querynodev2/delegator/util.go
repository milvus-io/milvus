package delegator

import (
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func BuildSparseFieldData(field *schemapb.FieldSchema, sparseArray *schemapb.SparseFloatArray) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: sparseArray.GetDim(),
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: sparseArray,
				},
			},
		},
		FieldId: field.GetFieldID(),
	}
}

func SetBM25Params(req *internalpb.SearchRequest, avgdl float64) error {
	log := log.With(zap.Int64("collection", req.GetCollectionID()))

	serializedPlan := req.GetSerializedExprPlan()
	// plan not found
	if serializedPlan == nil {
		log.Warn("serialized plan not found")
		return merr.WrapErrParameterInvalid("serialized search plan", "nil")
	}

	plan := planpb.PlanNode{}
	err := proto.Unmarshal(serializedPlan, &plan)
	if err != nil {
		log.Warn("failed to unmarshal plan", zap.Error(err))
		return merr.WrapErrParameterInvalid("valid serialized search plan", "no unmarshalable one", err.Error())
	}

	switch plan.GetNode().(type) {
	case *planpb.PlanNode_VectorAnns:
		queryInfo := plan.GetVectorAnns().GetQueryInfo()
		queryInfo.Bm25Avgdl = avgdl
		serializedExprPlan, err := proto.Marshal(&plan)
		if err != nil {
			log.Warn("failed to marshal optimized plan", zap.Error(err))
			return merr.WrapErrParameterInvalid("marshalable search plan", "plan with marshal error", err.Error())
		}
		req.SerializedExprPlan = serializedExprPlan
		log.Debug("add bm25 avgdl to search params done", zap.Any("queryInfo", queryInfo))
	default:
		log.Warn("not supported node type", zap.String("nodeType", fmt.Sprintf("%T", plan.GetNode())))
	}
	return nil
}
