package model

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	pb "github.com/milvus-io/milvus/internal/proto/etcdpb"
)

type Index struct {
	CollectionID   int64
	FieldID        int64
	IndexID        int64
	IndexName      string
	IsDeleted      bool
	CreateTime     uint64
	IndexParams    []*commonpb.KeyValuePair
	SegmentIndexes map[int64]SegmentIndex //segmentID -> segmentIndex
	Extra          map[string]string
}

func UnmarshalIndexModel(indexInfo *pb.IndexInfo) *Index {
	if indexInfo == nil {
		return nil
	}

	return &Index{
		IndexName:   indexInfo.IndexName,
		IndexID:     indexInfo.IndexID,
		IndexParams: indexInfo.IndexParams,
		IsDeleted:   indexInfo.Deleted,
		CreateTime:  indexInfo.CreateTime,
	}
}

func MarshalIndexModel(index *Index) *pb.IndexInfo {
	if index == nil {
		return nil
	}

	return &pb.IndexInfo{
		IndexName:   index.IndexName,
		IndexID:     index.IndexID,
		IndexParams: index.IndexParams,
		Deleted:     index.IsDeleted,
		CreateTime:  index.CreateTime,
	}
}

func MergeIndexModel(a *Index, b *Index) *Index {
	if a == nil {
		return b
	}

	if b == nil {
		return a
	}

	newIdx := *a
	if b.SegmentIndexes != nil {
		if newIdx.SegmentIndexes == nil {
			newIdx.SegmentIndexes = b.SegmentIndexes
		} else {
			for segID, segmentIndex := range b.SegmentIndexes {
				newIdx.SegmentIndexes[segID] = segmentIndex
			}
		}
	}

	if newIdx.CollectionID == 0 && b.CollectionID != 0 {
		newIdx.CollectionID = b.CollectionID
	}

	if newIdx.FieldID == 0 && b.FieldID != 0 {
		newIdx.FieldID = b.FieldID
	}

	if newIdx.IndexID == 0 && b.IndexID != 0 {
		newIdx.IndexID = b.IndexID
	}

	if newIdx.IndexName == "" && b.IndexName != "" {
		newIdx.IndexName = b.IndexName
	}

	if newIdx.IndexParams == nil && b.IndexParams != nil {
		newIdx.IndexParams = b.IndexParams
	}

	newIdx.IsDeleted = b.IsDeleted
	newIdx.CreateTime = b.CreateTime

	if newIdx.Extra == nil && b.Extra != nil {
		newIdx.Extra = b.Extra
	}

	return &newIdx
}
