package model

import (
	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type SegmentIndex struct {
	SegmentID      int64
	CollectionID   int64
	PartitionID    int64
	NumRows        int64
	IndexID        int64
	BuildID        int64
	NodeID         int64
	IndexVersion   int64
	IndexState     commonpb.IndexState
	FailReason     string
	IsDeleted      bool
	CreateTime     uint64
	IndexFilePaths []string
	IndexSize      uint64
}

func UnmarshalSegmentIndexModel(segIndex *indexpb.SegmentIndex) *SegmentIndex {
	if segIndex == nil {
		return nil
	}

	return &SegmentIndex{
		SegmentID:      segIndex.SegmentID,
		CollectionID:   segIndex.CollectionID,
		PartitionID:    segIndex.PartitionID,
		NumRows:        segIndex.NumRows,
		IndexID:        segIndex.IndexID,
		BuildID:        segIndex.BuildID,
		NodeID:         segIndex.NodeID,
		IndexState:     segIndex.State,
		FailReason:     segIndex.FailReason,
		IndexVersion:   segIndex.IndexVersion,
		IsDeleted:      segIndex.Deleted,
		CreateTime:     segIndex.CreateTime,
		IndexFilePaths: segIndex.IndexFilesPaths,
		IndexSize:      segIndex.SerializeSize,
	}
}

func MarshalSegmentIndexModel(segIdx *SegmentIndex) *indexpb.SegmentIndex {
	if segIdx == nil {
		return nil
	}

	return &indexpb.SegmentIndex{
		CollectionID:    segIdx.CollectionID,
		PartitionID:     segIdx.PartitionID,
		SegmentID:       segIdx.SegmentID,
		NumRows:         segIdx.NumRows,
		IndexID:         segIdx.IndexID,
		BuildID:         segIdx.BuildID,
		NodeID:          segIdx.NodeID,
		State:           segIdx.IndexState,
		FailReason:      segIdx.FailReason,
		IndexVersion:    segIdx.IndexVersion,
		IndexFilesPaths: segIdx.IndexFilePaths,
		Deleted:         segIdx.IsDeleted,
		CreateTime:      segIdx.CreateTime,
		SerializeSize:   segIdx.IndexSize,
	}
}

func CloneSegmentIndex(segIndex *SegmentIndex) *SegmentIndex {
	return &SegmentIndex{
		SegmentID:      segIndex.SegmentID,
		CollectionID:   segIndex.CollectionID,
		PartitionID:    segIndex.PartitionID,
		NumRows:        segIndex.NumRows,
		IndexID:        segIndex.IndexID,
		BuildID:        segIndex.BuildID,
		NodeID:         segIndex.NodeID,
		IndexState:     segIndex.IndexState,
		FailReason:     segIndex.FailReason,
		IndexVersion:   segIndex.IndexVersion,
		IsDeleted:      segIndex.IsDeleted,
		CreateTime:     segIndex.CreateTime,
		IndexFilePaths: segIndex.IndexFilePaths,
		IndexSize:      segIndex.IndexSize,
	}
}
