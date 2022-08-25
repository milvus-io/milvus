package indexcoord

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Catalog struct {
	Txn kv.TxnKV
}

func buildIndexKey(collectionID, indexID int64) string {
	return fmt.Sprintf("%s/%d/%d", FieldIndexPrefix, collectionID, indexID)
}

func buildSegmentIndexKey(collectionID, partitionID, segmentID, buildID int64) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", SegmentIndexPrefix, collectionID, partitionID, segmentID, buildID)
}

func (kc *Catalog) CreateIndex(ctx context.Context, index *model.Index) error {
	key := buildIndexKey(index.CollectionID, index.IndexID)

	value, err := proto.Marshal(model.MarshalIndexModel(index))
	if err != nil {
		return err
	}

	err = kc.Txn.Save(key, string(value))
	if err != nil {
		return err
	}
	return nil
}

func (kc *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	_, values, err := kc.Txn.LoadWithPrefix(FieldIndexPrefix)
	if err != nil {
		log.Error("list index meta fail", zap.String("prefix", FieldIndexPrefix), zap.Error(err))
		return nil, err
	}

	indexes := make([]*model.Index, 0)
	for _, value := range values {
		meta := &indexpb.FieldIndex{}
		err = proto.Unmarshal([]byte(value), meta)
		if err != nil {
			log.Warn("unmarshal index info failed", zap.Error(err))
			return nil, err
		}

		indexes = append(indexes, model.UnmarshalIndexModel(meta))
	}

	return indexes, nil
}

func (kc *Catalog) AlterIndex(ctx context.Context, index *model.Index) error {
	return kc.CreateIndex(ctx, index)
}

func (kc *Catalog) AlterIndexes(ctx context.Context, indexes []*model.Index) error {
	kvs := make(map[string]string)
	for _, index := range indexes {
		key := buildIndexKey(index.CollectionID, index.IndexID)

		value, err := proto.Marshal(model.MarshalIndexModel(index))
		if err != nil {
			return err
		}

		kvs[key] = string(value)
	}
	return kc.Txn.MultiSave(kvs)
}

func (kc *Catalog) DropIndex(ctx context.Context, collID typeutil.UniqueID, dropIdxID typeutil.UniqueID) error {
	key := buildIndexKey(collID, dropIdxID)

	err := kc.Txn.Remove(key)
	if err != nil {
		log.Error("drop collection index meta fail", zap.Int64("collectionID", collID),
			zap.Int64("indexID", dropIdxID), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) CreateSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error {
	key := buildSegmentIndexKey(segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.BuildID)

	value, err := proto.Marshal(model.MarshalSegmentIndexModel(segIdx))
	if err != nil {
		return err
	}
	err = kc.Txn.Save(key, string(value))
	if err != nil {
		log.Error("failed to save segment index meta in etcd", zap.Int64("buildID", segIdx.BuildID),
			zap.Int64("segmentID", segIdx.SegmentID), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error) {
	_, values, err := kc.Txn.LoadWithPrefix(SegmentIndexPrefix)
	if err != nil {
		log.Error("list segment index meta fail", zap.String("prefix", SegmentIndexPrefix), zap.Error(err))
		return nil, err
	}

	segIndexes := make([]*model.SegmentIndex, 0)
	for _, value := range values {
		segmentIndexInfo := &indexpb.SegmentIndex{}
		err = proto.Unmarshal([]byte(value), segmentIndexInfo)
		if err != nil {
			log.Warn("unmarshal segment index info failed", zap.Error(err))
			continue
		}

		segIndexes = append(segIndexes, model.UnmarshalSegmentIndexModel(segmentIndexInfo))
	}

	return segIndexes, nil
}

func (kc *Catalog) AlterSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error {
	return kc.CreateSegmentIndex(ctx, segIdx)
}

func (kc *Catalog) AlterSegmentIndexes(ctx context.Context, segIdxes []*model.SegmentIndex) error {
	kvs := make(map[string]string)
	for _, segIdx := range segIdxes {
		key := buildSegmentIndexKey(segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.BuildID)
		value, err := proto.Marshal(model.MarshalSegmentIndexModel(segIdx))
		if err != nil {
			return err
		}
		kvs[key] = string(value)
	}
	return kc.Txn.MultiSave(kvs)
}

func (kc *Catalog) DropSegmentIndex(ctx context.Context, collID, partID, segID, buildID typeutil.UniqueID) error {
	key := buildSegmentIndexKey(collID, partID, segID, buildID)

	err := kc.Txn.Remove(key)
	if err != nil {
		log.Error("drop segment index meta fail", zap.Int64("buildID", buildID), zap.Error(err))
		return err
	}

	return nil
}
