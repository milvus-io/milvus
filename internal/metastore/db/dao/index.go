package dao

import (
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type indexDb struct {
	db *gorm.DB
}

func (s *indexDb) Get(tenantID string, collectionID typeutil.UniqueID) ([]*dbmodel.Index, error) {
	var r []*dbmodel.Index

	err := s.db.Model(&dbmodel.Index{}).Where("tenant_id = ? AND collection_id = ?", tenantID, collectionID).Find(&r).Error
	if err != nil {
		log.Error("get indexes by collection_id failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collectionID), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *indexDb) List(tenantID string) ([]*dbmodel.IndexResult, error) {
	tx := s.db.Table("indexes").
		Select("indexes.field_id AS field_id, indexes.collection_id AS collection_id, indexes.index_id AS index_id, "+
			"indexes.index_name AS index_name, indexes.index_params AS index_params, indexes.type_params AS type_params, "+
			"indexes.is_deleted AS is_deleted, indexes.create_time AS create_time").
		Where("indexes.tenant_id = ?", tenantID)

	var rs []*dbmodel.IndexResult
	err := tx.Scan(&rs).Error
	if err != nil {
		log.Error("list indexes by join failed", zap.String("tenant", tenantID), zap.Error(err))
		return nil, err
	}

	return rs, nil
}

func (s *indexDb) Insert(in []*dbmodel.Index) error {
	err := s.db.CreateInBatches(in, 100).Error
	if err != nil {
		log.Error("insert index failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *indexDb) Update(in *dbmodel.Index) error {
	err := s.db.Model(&dbmodel.Index{}).Where("tenant_id = ? AND collection_id = ? AND index_id = ?", in.TenantID, in.CollectionID, in.IndexID).Updates(dbmodel.Index{
		CreateTime: in.CreateTime, // if in.CreateTime is not set, column CreateTime will not be updated
		IsDeleted:  in.IsDeleted,
	}).Error

	if err != nil {
		log.Error("update indexes failed", zap.String("tenant", in.TenantID), zap.Int64("collectionID", in.CollectionID), zap.Int64("indexID", in.IndexID), zap.Error(err))
		return err
	}

	return nil
}

func (s *indexDb) MarkDeletedByCollectionID(tenantID string, collID typeutil.UniqueID) error {
	err := s.db.Model(&dbmodel.Index{}).Where("tenant_id = ? AND collection_id = ?", tenantID, collID).Updates(dbmodel.Index{
		IsDeleted: true,
	}).Error

	if err != nil {
		log.Error("update indexes is_deleted=true failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collID), zap.Error(err))
		return err
	}

	return nil
}

func (s *indexDb) MarkDeletedByIndexID(tenantID string, indexID typeutil.UniqueID) error {
	err := s.db.Model(&dbmodel.Index{}).Where("tenant_id = ? AND index_id = ?", tenantID, indexID).Updates(dbmodel.Index{
		IsDeleted: true,
	}).Error

	if err != nil {
		log.Error("update indexes is_deleted=true failed", zap.String("tenant", tenantID), zap.Int64("indexID", indexID), zap.Error(err))
		return err
	}

	return nil
}
