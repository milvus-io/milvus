package model

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

type Index struct {
	TenantID        string
	CollectionID    int64
	FieldID         int64
	IndexID         int64
	IndexName       string
	IsDeleted       bool
	CreateTime      uint64
	TypeParams      []*commonpb.KeyValuePair
	IndexParams     []*commonpb.KeyValuePair
	IsAutoIndex     bool
	UserIndexParams []*commonpb.KeyValuePair
}

func UnmarshalIndexModel(indexInfo *indexpb.FieldIndex) *Index {
	if indexInfo == nil {
		return nil
	}

	return &Index{
		CollectionID:    indexInfo.IndexInfo.GetCollectionID(),
		FieldID:         indexInfo.IndexInfo.GetFieldID(),
		IndexID:         indexInfo.IndexInfo.GetIndexID(),
		IndexName:       indexInfo.IndexInfo.GetIndexName(),
		IsDeleted:       indexInfo.GetDeleted(),
		CreateTime:      indexInfo.CreateTime,
		TypeParams:      indexInfo.IndexInfo.GetTypeParams(),
		IndexParams:     indexInfo.IndexInfo.GetIndexParams(),
		IsAutoIndex:     indexInfo.IndexInfo.GetIsAutoIndex(),
		UserIndexParams: indexInfo.IndexInfo.GetUserIndexParams(),
	}
}

func MarshalIndexModel(index *Index) *indexpb.FieldIndex {
	if index == nil {
		return nil
	}

	return &indexpb.FieldIndex{
		IndexInfo: &indexpb.IndexInfo{
			CollectionID:    index.CollectionID,
			FieldID:         index.FieldID,
			IndexName:       index.IndexName,
			IndexID:         index.IndexID,
			TypeParams:      index.TypeParams,
			IndexParams:     index.IndexParams,
			IsAutoIndex:     index.IsAutoIndex,
			UserIndexParams: index.UserIndexParams,
		},
		Deleted:    index.IsDeleted,
		CreateTime: index.CreateTime,
	}
}

//func MergeIndexModel(a *Index, b *Index) *Index {
//	if a == nil {
//		return b
//	}
//
//	if b == nil {
//		return a
//	}
//
//	newIdx := *a
//	if b.SegmentIndexes != nil {
//		if newIdx.SegmentIndexes == nil {
//			newIdx.SegmentIndexes = b.SegmentIndexes
//		} else {
//			for segID, segmentIndex := range b.SegmentIndexes {
//				newIdx.SegmentIndexes[segID] = segmentIndex
//			}
//		}
//	}
//
//	if newIdx.CollectionID == 0 && b.CollectionID != 0 {
//		newIdx.CollectionID = b.CollectionID
//	}
//
//	if newIdx.FieldID == 0 && b.FieldID != 0 {
//		newIdx.FieldID = b.FieldID
//	}
//
//	if newIdx.IndexID == 0 && b.IndexID != 0 {
//		newIdx.IndexID = b.IndexID
//	}
//
//	if newIdx.IndexName == "" && b.IndexName != "" {
//		newIdx.IndexName = b.IndexName
//	}
//
//	if newIdx.IndexParams == nil && b.IndexParams != nil {
//		newIdx.IndexParams = b.IndexParams
//	}
//
//	newIdx.IsDeleted = b.IsDeleted
//	newIdx.CreatedUTCTime = b.CreatedUTCTime
//
//	if newIdx.Extra == nil && b.Extra != nil {
//		newIdx.Extra = b.Extra
//	}
//
//	return &newIdx
//}

func CloneIndex(index *Index) *Index {
	clonedIndex := &Index{
		TenantID:        index.TenantID,
		CollectionID:    index.CollectionID,
		FieldID:         index.FieldID,
		IndexID:         index.IndexID,
		IndexName:       index.IndexName,
		IsDeleted:       index.IsDeleted,
		CreateTime:      index.CreateTime,
		TypeParams:      make([]*commonpb.KeyValuePair, len(index.TypeParams)),
		IndexParams:     make([]*commonpb.KeyValuePair, len(index.IndexParams)),
		IsAutoIndex:     index.IsAutoIndex,
		UserIndexParams: make([]*commonpb.KeyValuePair, len(index.UserIndexParams)),
	}
	for i, param := range index.TypeParams {
		clonedIndex.TypeParams[i] = proto.Clone(param).(*commonpb.KeyValuePair)
	}
	for i, param := range index.IndexParams {
		clonedIndex.IndexParams[i] = proto.Clone(param).(*commonpb.KeyValuePair)
	}
	for i, param := range index.UserIndexParams {
		clonedIndex.UserIndexParams[i] = proto.Clone(param).(*commonpb.KeyValuePair)
	}
	return clonedIndex
}
