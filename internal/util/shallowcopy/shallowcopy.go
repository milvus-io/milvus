package shallowcopy

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

// ShallowCopySearchRequest creates a lightweight copy of SearchRequest that shares
// all slice/bytes fields with the original. Only Base is newly allocated with TargetID set.
func ShallowCopySearchRequest(src *internalpb.SearchRequest, targetID int64) *internalpb.SearchRequest {
	if src == nil {
		return nil
	}
	return &internalpb.SearchRequest{
		Base:                    &commonpb.MsgBase{TargetID: targetID},
		ReqID:                   src.ReqID,
		DbID:                    src.DbID,
		CollectionID:            src.CollectionID,
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
	}
}

// ShallowCopyRetrieveRequest creates a lightweight copy of RetrieveRequest that shares
// all slice/bytes fields with the original. Only Base is newly allocated with TargetID set.
func ShallowCopyRetrieveRequest(src *internalpb.RetrieveRequest, targetID int64) *internalpb.RetrieveRequest {
	if src == nil {
		return nil
	}
	return &internalpb.RetrieveRequest{
		Base:                         &commonpb.MsgBase{TargetID: targetID},
		ReqID:                        src.ReqID,
		DbID:                         src.DbID,
		CollectionID:                 src.CollectionID,
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
