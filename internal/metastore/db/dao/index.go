package dao

import (
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type indexDb struct {
	db *gorm.DB
}

func (s *indexDb) Get(tenantID string, collectionID typeutil.UniqueID) ([]*dbmodel.Index, error) {
	tx := s.db.Model(&dbmodel.Index{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var r []*dbmodel.Index
	err := tx.Where("collection_id = ? AND is_deleted = false", collectionID).Find(&r).Error
	if err != nil {
		log.Error("get indexes by collection_id failed", zap.Int64("collID", collectionID), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *indexDb) List(tenantID string) ([]*dbmodel.IndexResult, error) {
	tx := s.db.Table("indexes").
		Select("indexes.field_id AS field_id, indexes.collection_id AS collection_id, indexes.index_id AS index_id, indexes.index_name AS index_name, indexes.index_params AS index_params, segment_indexes.segment_id AS segment_id, segment_indexes.partition_id AS partition_id, segment_indexes.enable_index AS enable_index, segment_indexes.index_build_id AS index_build_id, segment_indexes.index_size AS index_size, segment_indexes.index_file_paths AS index_file_paths").
		Joins("LEFT JOIN segment_indexes ON indexes.index_id = segment_indexes.index_id AND segment_indexes.is_deleted = false").
		Where("indexes.is_deleted = false")

	// different sql clause if tenantID exists
	if tenantID != "" {
		tx = s.db.Table("indexes").
			Select("indexes.field_id AS field_id, indexes.collection_id AS collection_id, indexes.index_id AS index_id, indexes.index_name AS index_name, indexes.index_params AS index_params, segment_indexes.segment_id AS segment_id, segment_indexes.partition_id AS partition_id, segment_indexes.enable_index AS enable_index, segment_indexes.index_build_id AS index_build_id, segment_indexes.index_size AS index_size, segment_indexes.index_file_paths AS index_file_paths").
			Joins("LEFT JOIN segment_indexes ON indexes.index_id = segment_indexes.index_id AND indexes.tenant_id = segment_indexes.tenant_id AND segment_indexes.tenant_id = ? AND segment_indexes.is_deleted = false", tenantID).
			Where("indexes.is_deleted = false").Where("indexes.tenant_id = ?", tenantID)
	}

	var rs []*dbmodel.IndexResult
	err := tx.Scan(&rs).Error
	if err != nil {
		log.Error("list indexes by join failed", zap.Error(err))
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

func (s *indexDb) MarkDeletedByCollID(tenantID string, collID typeutil.UniqueID) error {
	tx := s.db.Model(&dbmodel.Index{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("collection_id = ?", collID).Updates(dbmodel.Index{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error

	if err != nil {
		log.Error("update indexes is_deleted=true failed", zap.Int64("collID", collID), zap.Error(err))
		return err
	}

	return nil
}

func (s *indexDb) MarkDeletedByIdxID(tenantID string, idxID typeutil.UniqueID) error {
	tx := s.db.Model(&dbmodel.Index{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("index_id = ?", idxID).Updates(dbmodel.Index{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error
	
	if err != nil {
		log.Error("update indexes is_deleted=true failed", zap.Int64("idxID", idxID), zap.Error(err))
		return err
	}

	return nil
}
