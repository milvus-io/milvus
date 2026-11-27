package model

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// SegmentIndexSummary describes index inventory and task state without file metadata.
// Completed indexes and active or pending-cleanup tasks share this read contract.
type SegmentIndexSummary struct {
	EncryptionState           indexpb.IndexEncryptionState
	CollectionID              int64
	PartitionID               int64
	SegmentID                 int64
	IndexID                   int64
	BuildID                   int64
	IndexState                commonpb.IndexState
	IsDeleted                 bool
	HasArtifact               bool
	IndexVersion              int64
	IndexSerializedSize       uint64
	IndexMemSize              uint64
	CurrentIndexVersion       int32
	CurrentScalarIndexVersion int32
	IndexType                 string
}

func SummarizeSegmentIndex(record *SegmentIndex) *SegmentIndexSummary {
	return &SegmentIndexSummary{
		CollectionID: record.CollectionID, PartitionID: record.PartitionID,
		SegmentID: record.SegmentID, IndexID: record.IndexID, BuildID: record.BuildID,
		IndexState: record.IndexState, IsDeleted: record.IsDeleted,
		HasArtifact:  record.HasArtifact(),
		IndexVersion: record.IndexVersion, IndexSerializedSize: record.IndexSerializedSize, IndexMemSize: record.IndexMemSize,
		CurrentIndexVersion: record.CurrentIndexVersion, CurrentScalarIndexVersion: record.CurrentScalarIndexVersion,
		IndexType: record.IndexType, EncryptionState: record.EncryptionState,
	}
}

func SegmentIndexSummaryFromProto(segment *datapb.SegmentInfo, summary *datapb.SegmentIndexSummary) *SegmentIndexSummary {
	return &SegmentIndexSummary{
		CollectionID: segment.GetCollectionID(), PartitionID: segment.GetPartitionID(), SegmentID: segment.GetID(),
		IndexID: summary.GetIndexId(), BuildID: summary.GetBuildId(), IndexState: commonpb.IndexState_Finished,
		HasArtifact: summary.GetHasArtifact(), IndexVersion: summary.GetIndexVersion(),
		IndexSerializedSize: summary.GetSerializedSize(), IndexMemSize: summary.GetMemSize(),
		CurrentIndexVersion: summary.GetVectorEngineVersion(), CurrentScalarIndexVersion: summary.GetScalarEngineVersion(),
		IndexType: summary.GetIndexType(), EncryptionState: summary.GetEncryptionState(),
	}
}

func MarshalSegmentIndexSummary(record *SegmentIndexSummary, fieldID int64) *datapb.SegmentIndexSummary {
	return &datapb.SegmentIndexSummary{
		IndexId: record.IndexID, FieldId: fieldID, BuildId: record.BuildID,
		HasArtifact: record.HasArtifact, IndexVersion: record.IndexVersion,
		SerializedSize: record.IndexSerializedSize, MemSize: record.IndexMemSize,
		VectorEngineVersion: record.CurrentIndexVersion, ScalarEngineVersion: record.CurrentScalarIndexVersion,
		IndexType: record.IndexType, EncryptionState: record.EncryptionState,
	}
}
