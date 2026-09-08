package shallowcopy

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
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
		GroupByFieldIds:         src.GroupByFieldIds,
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

// ShallowCopyLoadSegmentsRequest copies the request envelope while sharing all
// nested messages, slices, and maps. Callers must copy a shared child before
// mutating it.
func ShallowCopyLoadSegmentsRequest(src *querypb.LoadSegmentsRequest) *querypb.LoadSegmentsRequest {
	if src == nil {
		return nil
	}
	return &querypb.LoadSegmentsRequest{
		Base:           src.Base,
		DstNodeID:      src.DstNodeID,
		Infos:          src.Infos,
		Schema:         src.Schema,
		SourceNodeID:   src.SourceNodeID,
		CollectionID:   src.CollectionID,
		LoadMeta:       src.LoadMeta,
		ReplicaID:      src.ReplicaID,
		DeltaPositions: src.DeltaPositions,
		Version:        src.Version,
		NeedTransfer:   src.NeedTransfer,
		LoadScope:      src.LoadScope,
		IndexInfoList:  src.IndexInfoList,
		LazyLoad:       src.LazyLoad,
	}
}

// ShallowCopySegmentLoadInfo copies the segment envelope while sharing its
// potentially large binlog, stats, and manifest metadata. Callers must copy a
// shared child before mutating it.
func ShallowCopySegmentLoadInfo(src *querypb.SegmentLoadInfo) *querypb.SegmentLoadInfo {
	if src == nil {
		return nil
	}
	return &querypb.SegmentLoadInfo{
		SegmentID:            src.SegmentID,
		PartitionID:          src.PartitionID,
		CollectionID:         src.CollectionID,
		DbID:                 src.DbID,
		FlushTime:            src.FlushTime,
		BinlogPaths:          src.BinlogPaths,
		NumOfRows:            src.NumOfRows,
		Statslogs:            src.Statslogs,
		Deltalogs:            src.Deltalogs,
		CompactionFrom:       src.CompactionFrom,
		IndexInfos:           src.IndexInfos,
		SegmentSize:          src.SegmentSize,
		InsertChannel:        src.InsertChannel,
		StartPosition:        src.StartPosition,
		DeltaPosition:        src.DeltaPosition,
		ReadableVersion:      src.ReadableVersion,
		Level:                src.Level,
		StorageVersion:       src.StorageVersion,
		IsSorted:             src.IsSorted,
		TextStatsLogs:        src.TextStatsLogs,
		Bm25Logs:             src.Bm25Logs,
		JsonKeyStatsLogs:     src.JsonKeyStatsLogs,
		Priority:             src.Priority,
		ManifestPath:         src.ManifestPath,
		DataVersion:          src.DataVersion,
		UseTakeForOutput:     src.UseTakeForOutput,
		EstimatedBytesPerRow: src.EstimatedBytesPerRow,
		CommitTimestamp:      src.CommitTimestamp,
		ChildManifestPaths:   src.ChildManifestPaths,
		Stats:                src.Stats,
	}
}

// ShallowCopyFieldIndexInfo copies the index descriptor envelope while sharing
// its parameter messages and index-file paths. Callers must copy a shared slice
// before changing its length or elements.
func ShallowCopyFieldIndexInfo(src *querypb.FieldIndexInfo) *querypb.FieldIndexInfo {
	if src == nil {
		return nil
	}
	return &querypb.FieldIndexInfo{
		FieldID:                   src.FieldID,
		EnableIndex:               src.EnableIndex,
		IndexName:                 src.IndexName,
		IndexID:                   src.IndexID,
		BuildID:                   src.BuildID,
		IndexParams:               src.IndexParams,
		IndexFilePaths:            src.IndexFilePaths,
		IndexSize:                 src.IndexSize,
		IndexVersion:              src.IndexVersion,
		NumRows:                   src.NumRows,
		CurrentIndexVersion:       src.CurrentIndexVersion,
		IndexStoreVersion:         src.IndexStoreVersion,
		CurrentScalarIndexVersion: src.CurrentScalarIndexVersion,
		IndexStorePathVersion:     src.IndexStorePathVersion,
	}
}
