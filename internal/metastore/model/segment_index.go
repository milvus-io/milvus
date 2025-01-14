package model

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/indexpb"
)

type SegmentIndex struct {
	SegmentID     int64
	CollectionID  int64
	PartitionID   int64
	NumRows       int64
	IndexID       int64
	BuildID       int64
	NodeID        int64
	IndexVersion  int64
	IndexState    commonpb.IndexState
	FailReason    string
	IsDeleted     bool
	CreateTime    uint64
	IndexFileKeys []string
	IndexSize     uint64
	// deprecated
	WriteHandoff        bool
	CurrentIndexVersion int32
	IndexStoreVersion   int64
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
		CreateTime:          segIndex.CreateTime,
		IndexFileKeys:       common.CloneStringList(segIndex.IndexFileKeys),
		IndexSize:           segIndex.SerializeSize,
		WriteHandoff:        segIndex.WriteHandoff,
		CurrentIndexVersion: segIndex.GetCurrentIndexVersion(),
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
		CreateTime:          segIdx.CreateTime,
		SerializeSize:       segIdx.IndexSize,
		WriteHandoff:        segIdx.WriteHandoff,
		CurrentIndexVersion: segIdx.CurrentIndexVersion,
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
		CreateTime:          segIndex.CreateTime,
		IndexFileKeys:       common.CloneStringList(segIndex.IndexFileKeys),
		IndexSize:           segIndex.IndexSize,
		WriteHandoff:        segIndex.WriteHandoff,
		CurrentIndexVersion: segIndex.CurrentIndexVersion,
	}
}
