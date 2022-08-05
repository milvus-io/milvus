package dao

import (
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type partitionDb struct {
	db *gorm.DB
}

func (s *partitionDb) GetByCollID(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*dbmodel.Partition, error) {
	tx := s.db.Model(&dbmodel.Partition{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var r []*dbmodel.Partition
	err := tx.Where("collection_id = ? AND ts = ? AND is_deleted = false", collectionID, ts).Find(&r).Error
	if err != nil {
		log.Error("get partitions by collection_id and ts failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *partitionDb) Insert(in []*dbmodel.Partition) error {
	err := s.db.CreateInBatches(in, 100).Error
	if err != nil {
		log.Error("insert partition failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *partitionDb) MarkDeleted(tenantID string, collID typeutil.UniqueID) error {
	tx := s.db.Model(&dbmodel.Partition{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("collection_id = ? AND ts = 0", collID).Updates(dbmodel.Partition{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error

	if err != nil {
		log.Error("update partitions is_deleted=true failed", zap.Int64("collID", collID), zap.Error(err))
		return err
	}

	return nil
}
