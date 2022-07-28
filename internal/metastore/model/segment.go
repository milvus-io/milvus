package model

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

type Segment struct {
	SegmentID           int64
	CollectionID        int64
	PartitionID         int64
	NumRows             int64
	BinLogs             []string
	MemSize             int64
	DmChannel           string
	CompactionFrom      []int64
	CreatedByCompaction bool
	SegmentState        commonpb.SegmentState
	IndexInfos          []*SegmentIndex
	ReplicaIds          []int64
	NodeIds             []int64
}

type SegmentIndex struct {
	Segment
	IndexID        int64
	BuildID        int64
	NodeID         int64
	State          commonpb.IndexState
	IndexVersion   int64
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
		Segment: Segment{
			SegmentID:    segIndex.SegmentID,
			CollectionID: segIndex.CollectionID,
			PartitionID:  segIndex.PartitionID,
			NumRows:      segIndex.NumRows,
		},
		IndexID:        segIndex.IndexID,
		BuildID:        segIndex.BuildID,
		NodeID:         segIndex.NodeID,
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
		State:           segIdx.State,
		IndexVersion:    segIdx.IndexVersion,
		IndexFilesPaths: segIdx.IndexFilePaths,
		Deleted:         segIdx.IsDeleted,
		CreateTime:      segIdx.CreateTime,
		SerializeSize:   segIdx.IndexSize,
	}
}
