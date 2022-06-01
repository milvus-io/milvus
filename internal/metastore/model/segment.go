package model

import "github.com/milvus-io/milvus/internal/proto/commonpb"

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
	BuildID        int64
	IndexSize      uint64
	IndexFilePaths []string
}
