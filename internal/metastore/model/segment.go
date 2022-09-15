package model

import (
	"github.com/milvus-io/milvus/api/commonpb"
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
	//IndexInfos          []*SegmentIndex
	ReplicaIds []int64
	NodeIds    []int64
}
