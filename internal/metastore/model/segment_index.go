package model

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/common"
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
	CreatedUTCTime uint64
	IndexFileKeys  []string
	IndexSize      uint64
	// deprecated
	WriteHandoff        bool
	CurrentIndexVersion int32
	IndexStoreVersion   int64
	FinishedUTCTime     uint64
}

func UnmarshalSegmentIndexModel(segIndex *indexpb.SegmentIndex) *SegmentIndex {
	if segIndex == nil {
		return nil
	}

	return &SegmentIndex{
		SegmentID:           segIndex.SegmentID,
		CollectionID:        segIndex.CollectionID,
		PartitionID:         segIndex.PartitionID,
		NumRows:             segIndex.NumRows,
		IndexID:             segIndex.IndexID,
		BuildID:             segIndex.BuildID,
		NodeID:              segIndex.NodeID,
		IndexState:          segIndex.State,
		FailReason:          segIndex.FailReason,
		IndexVersion:        segIndex.IndexVersion,
		IsDeleted:           segIndex.Deleted,
		CreatedUTCTime:      segIndex.CreateTime,
		IndexFileKeys:       common.CloneStringList(segIndex.IndexFileKeys),
		IndexSize:           segIndex.SerializeSize,
		WriteHandoff:        segIndex.WriteHandoff,
		CurrentIndexVersion: segIndex.GetCurrentIndexVersion(),
		FinishedUTCTime:     segIndex.FinishedTime,
	}
}

func MarshalSegmentIndexModel(segIdx *SegmentIndex) *indexpb.SegmentIndex {
	if segIdx == nil {
		return nil
	}

	return &indexpb.SegmentIndex{
		CollectionID:        segIdx.CollectionID,
		PartitionID:         segIdx.PartitionID,
		SegmentID:           segIdx.SegmentID,
		NumRows:             segIdx.NumRows,
		IndexID:             segIdx.IndexID,
		BuildID:             segIdx.BuildID,
		NodeID:              segIdx.NodeID,
		State:               segIdx.IndexState,
		FailReason:          segIdx.FailReason,
		IndexVersion:        segIdx.IndexVersion,
		IndexFileKeys:       common.CloneStringList(segIdx.IndexFileKeys),
		Deleted:             segIdx.IsDeleted,
		CreateTime:          segIdx.CreatedUTCTime,
		SerializeSize:       segIdx.IndexSize,
		WriteHandoff:        segIdx.WriteHandoff,
		CurrentIndexVersion: segIdx.CurrentIndexVersion,
		FinishedTime:        segIdx.FinishedUTCTime,
	}
}

func CloneSegmentIndex(segIndex *SegmentIndex) *SegmentIndex {
	return &SegmentIndex{
		SegmentID:           segIndex.SegmentID,
		CollectionID:        segIndex.CollectionID,
		PartitionID:         segIndex.PartitionID,
		NumRows:             segIndex.NumRows,
		IndexID:             segIndex.IndexID,
		BuildID:             segIndex.BuildID,
		NodeID:              segIndex.NodeID,
		IndexState:          segIndex.IndexState,
		FailReason:          segIndex.FailReason,
		IndexVersion:        segIndex.IndexVersion,
		IsDeleted:           segIndex.IsDeleted,
		CreatedUTCTime:      segIndex.CreatedUTCTime,
		IndexFileKeys:       common.CloneStringList(segIndex.IndexFileKeys),
		IndexSize:           segIndex.IndexSize,
		WriteHandoff:        segIndex.WriteHandoff,
		CurrentIndexVersion: segIndex.CurrentIndexVersion,
		FinishedUTCTime:     segIndex.FinishedUTCTime,
	}
}
