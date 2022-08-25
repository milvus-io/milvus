package indexcoord

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/contextutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Catalog struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
}

func NewTableCatalog(txImpl dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain) *Catalog {
	return &Catalog{
		txImpl:     txImpl,
		metaDomain: metaDomain,
	}
}

func (tc *Catalog) CreateIndex(ctx context.Context, index *model.Index) error {
	tenantID := contextutil.TenantID(ctx)

	indexParamsBytes, err := json.Marshal(index.IndexParams)
	if err != nil {
		log.Error("marshal IndexParams of index failed", zap.String("tenant", tenantID),
			zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID),
			zap.String("indexName", index.IndexName), zap.Error(err))
		return err
	}

	typeParamsBytes, err := json.Marshal(index.TypeParams)
	if err != nil {
		log.Error("marshal TypeParams of index failed", zap.String("tenant", tenantID),
			zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID),
			zap.String("indexName", index.IndexName), zap.Error(err))
		return err
	}

	idx := &dbmodel.Index{
		TenantID:     tenantID,
		CollectionID: index.CollectionID,
		FieldID:      index.FieldID,
		IndexID:      index.IndexID,
		IndexName:    index.IndexName,
		TypeParams:   string(typeParamsBytes),
		IndexParams:  string(indexParamsBytes),
		CreateTime:   index.CreateTime,
		IsDeleted:    index.IsDeleted,
	}

	err = tc.metaDomain.IndexDb(ctx).Insert([]*dbmodel.Index{idx})
	if err != nil {
		log.Error("insert indexes failed", zap.String("tenant", tenantID), zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID), zap.String("indexName", index.IndexName), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	tenantID := contextutil.TenantID(ctx)

	rs, err := tc.metaDomain.IndexDb(ctx).List(tenantID)
	if err != nil {
		return nil, err
	}

	result, err := dbmodel.UnmarshalIndexModel(rs)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (tc *Catalog) AlterIndex(ctx context.Context, index *model.Index) error {
	tenantID := contextutil.TenantID(ctx)

	indexParamsBytes, err := json.Marshal(index.IndexParams)
	if err != nil {
		log.Error("marshal IndexParams of index failed", zap.String("tenant", tenantID),
			zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID),
			zap.String("indexName", index.IndexName), zap.Error(err))
		return err
	}

	typeParamsBytes, err := json.Marshal(index.TypeParams)
	if err != nil {
		log.Error("marshal TypeParams of index failed", zap.String("tenant", tenantID),
			zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID),
			zap.String("indexName", index.IndexName), zap.Error(err))
		return err
	}

	idx := &dbmodel.Index{
		TenantID:     tenantID,
		CollectionID: index.CollectionID,
		FieldID:      index.FieldID,
		IndexID:      index.IndexID,
		IndexName:    index.IndexName,
		TypeParams:   string(typeParamsBytes),
		IndexParams:  string(indexParamsBytes),
		CreateTime:   index.CreateTime,
		IsDeleted:    index.IsDeleted,
	}
	err = tc.metaDomain.IndexDb(ctx).Update(idx)
	if err != nil {
		return err
	}
	return nil
}

func (tc *Catalog) AlterIndexes(ctx context.Context, indexes []*model.Index) error {
	tenantID := contextutil.TenantID(ctx)

	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		for _, index := range indexes {
			indexParamsBytes, err := json.Marshal(index.IndexParams)
			if err != nil {
				log.Error("marshal IndexParams of index failed", zap.String("tenant", tenantID),
					zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID),
					zap.String("indexName", index.IndexName), zap.Error(err))
				return err
			}

			typeParamsBytes, err := json.Marshal(index.TypeParams)
			if err != nil {
				log.Error("marshal TypeParams of index failed", zap.String("tenant", tenantID),
					zap.Int64("collID", index.CollectionID), zap.Int64("indexID", index.IndexID),
					zap.String("indexName", index.IndexName), zap.Error(err))
				return err
			}

			idx := &dbmodel.Index{
				TenantID:     tenantID,
				CollectionID: index.CollectionID,
				FieldID:      index.FieldID,
				IndexID:      index.IndexID,
				IndexName:    index.IndexName,
				TypeParams:   string(typeParamsBytes),
				IndexParams:  string(indexParamsBytes),
				CreateTime:   index.CreateTime,
				IsDeleted:    index.IsDeleted,
			}
			err = tc.metaDomain.IndexDb(ctx).Update(idx)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (tc *Catalog) DropIndex(ctx context.Context, collID, dropIdxID typeutil.UniqueID) error {
	tenantID := contextutil.TenantID(ctx)

	// TODO: really delete.
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// mark deleted for index
		err := tc.metaDomain.IndexDb(txCtx).MarkDeletedByIndexID(tenantID, dropIdxID)
		if err != nil {
			return err
		}

		return nil
	})
}

func (tc *Catalog) CreateSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error {
	tenantID := contextutil.TenantID(ctx)

	indexFilesBytes, err := json.Marshal(segIdx.IndexFilePaths)
	if err != nil {
		log.Error("marshal IndexFiles of segment index failed", zap.String("tenant", tenantID),
			zap.Int64("collID", segIdx.CollectionID), zap.Int64("indexID", segIdx.IndexID),
			zap.Int64("segID", segIdx.SegmentID), zap.Int64("buildID", segIdx.BuildID), zap.Error(err))
		return err
	}

	idx := &dbmodel.SegmentIndex{
		TenantID:       tenantID,
		CollectionID:   segIdx.CollectionID,
		PartitionID:    segIdx.PartitionID,
		SegmentID:      segIdx.SegmentID,
		NumRows:        segIdx.NumRows,
		IndexID:        segIdx.IndexID,
		BuildID:        segIdx.BuildID,
		NodeID:         segIdx.NodeID,
		IndexVersion:   segIdx.IndexVersion,
		IndexState:     int32(segIdx.IndexState),
		FailReason:     segIdx.FailReason,
		CreateTime:     segIdx.CreateTime,
		IndexFilePaths: string(indexFilesBytes),
		IndexSize:      segIdx.IndexSize,
		IsDeleted:      segIdx.IsDeleted,
	}

	err = tc.metaDomain.SegmentIndexDb(ctx).Insert([]*dbmodel.SegmentIndex{idx})
	if err != nil {
		log.Error("insert segment index failed", zap.String("tenant", tenantID),
			zap.Int64("collID", segIdx.CollectionID), zap.Int64("indexID", segIdx.IndexID),
			zap.Int64("segID", segIdx.SegmentID), zap.Int64("buildID", segIdx.BuildID), zap.Error(err))
		return err
	}

	return nil
}

func (tc *Catalog) ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error) {
	tenantID := contextutil.TenantID(ctx)

	rs, err := tc.metaDomain.SegmentIndexDb(ctx).List(tenantID)
	if err != nil {
		return nil, err
	}

	result, err := dbmodel.UnmarshalSegmentIndexModel(rs)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (tc *Catalog) AlterSegmentIndex(ctx context.Context, segIndex *model.SegmentIndex) error {
	tenantID := contextutil.TenantID(ctx)

	indexFilesBytes, err := json.Marshal(segIndex.IndexFilePaths)
	if err != nil {
		log.Error("marshal index files of segment index failed", zap.String("tenant", tenantID),
			zap.Int64("collID", segIndex.CollectionID), zap.Int64("indexID", segIndex.IndexID),
			zap.Int64("segID", segIndex.SegmentID), zap.Int64("buildID", segIndex.BuildID), zap.Error(err))
		return err
	}

	idx := &dbmodel.SegmentIndex{
		TenantID:       tenantID,
		CollectionID:   segIndex.CollectionID,
		PartitionID:    segIndex.PartitionID,
		SegmentID:      segIndex.SegmentID,
		NumRows:        segIndex.NumRows,
		IndexID:        segIndex.IndexID,
		BuildID:        segIndex.BuildID,
		NodeID:         segIndex.NodeID,
		IndexVersion:   segIndex.IndexVersion,
		IndexState:     int32(segIndex.IndexState),
		FailReason:     segIndex.FailReason,
		CreateTime:     segIndex.CreateTime,
		IndexFilePaths: string(indexFilesBytes),
		IndexSize:      segIndex.IndexSize,
		IsDeleted:      segIndex.IsDeleted,
	}
	err = tc.metaDomain.SegmentIndexDb(ctx).Update(idx)
	if err != nil {
		return err
	}
	return nil
}

func (tc *Catalog) AlterSegmentIndexes(ctx context.Context, segIdxes []*model.SegmentIndex) error {
	tenantID := contextutil.TenantID(ctx)

	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		for _, segIndex := range segIdxes {
			indexFilesBytes, err := json.Marshal(segIndex.IndexFilePaths)
			if err != nil {
				log.Error("marshal index files of segment index failed", zap.String("tenant", tenantID),
					zap.Int64("collID", segIndex.CollectionID), zap.Int64("indexID", segIndex.IndexID),
					zap.Int64("segID", segIndex.SegmentID), zap.Int64("buildID", segIndex.BuildID), zap.Error(err))
				return err
			}

			idx := &dbmodel.SegmentIndex{
				TenantID:       tenantID,
				CollectionID:   segIndex.CollectionID,
				PartitionID:    segIndex.PartitionID,
				SegmentID:      segIndex.SegmentID,
				NumRows:        segIndex.NumRows,
				IndexID:        segIndex.IndexID,
				BuildID:        segIndex.BuildID,
				NodeID:         segIndex.NodeID,
				IndexVersion:   segIndex.IndexVersion,
				IndexState:     int32(segIndex.IndexState),
				FailReason:     segIndex.FailReason,
				CreateTime:     segIndex.CreateTime,
				IndexFilePaths: string(indexFilesBytes),
				IndexSize:      segIndex.IndexSize,
				IsDeleted:      segIndex.IsDeleted,
			}
			err = tc.metaDomain.SegmentIndexDb(ctx).Update(idx)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (tc *Catalog) DropSegmentIndex(ctx context.Context, collID, partID, segID, buildID typeutil.UniqueID) error {
	tenantID := contextutil.TenantID(ctx)

	err := tc.metaDomain.SegmentIndexDb(ctx).MarkDeletedByBuildID(tenantID, buildID)
	if err != nil {
		return err
	}

	return nil
}
