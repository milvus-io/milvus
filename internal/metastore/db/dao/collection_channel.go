package dao

import (
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type collChannelDb struct {
	db *gorm.DB
}

func (s *collChannelDb) GetByCollID(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*dbmodel.CollectionChannel, error) {
	tx := s.db.Model(&dbmodel.CollectionChannel{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var r []*dbmodel.CollectionChannel

	err := tx.Where("collection_id = ? AND ts = ? AND is_deleted = false", collectionID, ts).Find(&r).Error
	if err != nil {
		log.Error("get channels by collection_id and ts failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *collChannelDb) Insert(in []*dbmodel.CollectionChannel) error {
	err := s.db.CreateInBatches(in, 100).Error
	if err != nil {
		log.Error("insert channel failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *collChannelDb) MarkDeleted(tenantID string, collID typeutil.UniqueID) error {
	tx := s.db.Model(&dbmodel.CollectionChannel{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("collection_id = ? AND ts = 0", collID).Updates(dbmodel.CollectionChannel{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error

	if err != nil {
		log.Error("update is_deleted=true failed", zap.Int64("collID", collID), zap.Error(err))
		return err
	}

	return nil
}
