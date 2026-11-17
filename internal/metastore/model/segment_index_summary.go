package model

import "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

// SegmentIndexSummary describes index inventory and task state without file metadata.
// Completed indexes and active or pending-cleanup tasks share this read contract.
type SegmentIndexSummary struct {
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	IndexID      int64
	BuildID      int64
	IndexState   commonpb.IndexState
	IsDeleted    bool
	HasArtifact  bool
}

func SummarizeSegmentIndex(record *SegmentIndex) *SegmentIndexSummary {
	return &SegmentIndexSummary{
		CollectionID: record.CollectionID, PartitionID: record.PartitionID,
		SegmentID: record.SegmentID, IndexID: record.IndexID, BuildID: record.BuildID,
		IndexState: record.IndexState, IsDeleted: record.IsDeleted,
		HasArtifact: record.HasArtifact(),
	}
}
