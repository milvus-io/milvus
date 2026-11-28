package shallowcopy

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

func copyMsgBase(src *commonpb.MsgBase, targetID int64) *commonpb.MsgBase {
	base := &commonpb.MsgBase{}
	if src != nil {
		base = proto.Clone(src).(*commonpb.MsgBase)
	}
	base.TargetID = targetID
	return base
}

// ShallowCopySearchRequest creates a lightweight copy of SearchRequest that shares
// all slice/bytes fields with the original. Base is cloned with TargetID replaced.
func ShallowCopySearchRequest(src *internalpb.SearchRequest, targetID int64) *internalpb.SearchRequest {
	if src == nil {
		return nil
	}
	return &internalpb.SearchRequest{
		Base:                    copyMsgBase(src.GetBase(), targetID),
		ReqID:                   src.ReqID,
		DbID:                    src.DbID,
		CollectionID:            src.CollectionID,
		SchemaVersion:           src.SchemaVersion,
		PartitionIDs:            src.PartitionIDs,
		Dsl:                     src.Dsl,
		PlaceholderGroup:        src.PlaceholderGroup,
		DslType:                 src.DslType,
		SerializedExprPlan:      src.SerializedExprPlan,
		OutputFieldsId:          src.OutputFieldsId,
		MvccTimestamp:           src.MvccTimestamp,
		GuaranteeTimestamp:      src.GuaranteeTimestamp,
		TimeoutTimestamp:        src.TimeoutTimestamp,
		Nq:                      src.Nq,
		Topk:                    src.Topk,
		MetricType:              src.MetricType,
		IgnoreGrowing:           src.IgnoreGrowing,
		Username:                src.Username,
		SubReqs:                 src.SubReqs,
		IsAdvanced:              src.IsAdvanced,
		Offset:                  src.Offset,
		ConsistencyLevel:        src.ConsistencyLevel,
		GroupByFieldId:          src.GroupByFieldId,
		GroupSize:               src.GroupSize,
		FieldId:                 src.FieldId,
		IsTopkReduce:            src.IsTopkReduce,
		IsRecallEvaluation:      src.IsRecallEvaluation,
		IsIterator:              src.IsIterator,
		AnalyzerName:            src.AnalyzerName,
		CollectionTtlTimestamps: src.CollectionTtlTimestamps,
		EntityTtlPhysicalTime:   src.EntityTtlPhysicalTime,
		PkFilter:                src.PkFilter,
		SearchType:              src.SearchType,
		GroupByFieldIds:         src.GroupByFieldIds,
	}
}

// ShallowCopyRetrieveRequest creates a lightweight copy of RetrieveRequest that shares
// all slice/bytes fields with the original. Base is cloned with TargetID replaced.
func ShallowCopyRetrieveRequest(src *internalpb.RetrieveRequest, targetID int64) *internalpb.RetrieveRequest {
	if src == nil {
		return nil
	}
	return &internalpb.RetrieveRequest{
		Base:                         copyMsgBase(src.GetBase(), targetID),
		ReqID:                        src.ReqID,
		DbID:                         src.DbID,
		CollectionID:                 src.CollectionID,
		SchemaVersion:                src.SchemaVersion,
		PartitionIDs:                 src.PartitionIDs,
		SerializedExprPlan:           src.SerializedExprPlan,
		OutputFieldsId:               src.OutputFieldsId,
		MvccTimestamp:                src.MvccTimestamp,
		GuaranteeTimestamp:           src.GuaranteeTimestamp,
		TimeoutTimestamp:             src.TimeoutTimestamp,
		Limit:                        src.Limit,
		IgnoreGrowing:                src.IgnoreGrowing,
		IsCount:                      src.IsCount,
		IterationExtensionReduceRate: src.IterationExtensionReduceRate,
		Username:                     src.Username,
		ReduceStopForBest:            src.ReduceStopForBest,
		ReduceType:                   src.ReduceType,
		ConsistencyLevel:             src.ConsistencyLevel,
		IsIterator:                   src.IsIterator,
		CollectionTtlTimestamps:      src.CollectionTtlTimestamps,
		GroupByFieldIds:              src.GroupByFieldIds,
		Aggregates:                   src.Aggregates,
		EntityTtlPhysicalTime:        src.EntityTtlPhysicalTime,
		OrderByFields:                src.OrderByFields,
		QueryLabel:                   src.QueryLabel,
		PkFilter:                     src.PkFilter,
	}
}
