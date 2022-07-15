package model

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
)

type Segment struct {
	SegmentID           int64
	PartitionID         int64
	NumRows             int64
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
	EnableIndex    bool
	CreateTime     uint64
	BuildID        int64
	IndexSize      uint64
	IndexFilePaths []string
}

func UnmarshalSegmentIndexModel(segIndex *pb.SegmentIndexInfo) *Index {
	if segIndex == nil {
		return nil
	}

	return &Index{
		CollectionID: segIndex.CollectionID,
		SegmentIndexes: map[int64]SegmentIndex{
			segIndex.SegmentID: {
				Segment: Segment{
					SegmentID:   segIndex.SegmentID,
					PartitionID: segIndex.PartitionID,
				},
				BuildID:     segIndex.BuildID,
				EnableIndex: segIndex.EnableIndex,
				CreateTime:  segIndex.CreateTime,
			},
		},
		FieldID: segIndex.FieldID,
		IndexID: segIndex.IndexID,
	}
}
