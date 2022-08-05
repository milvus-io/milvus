package dao

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type fieldDb struct {
	db *gorm.DB
}

func (s *fieldDb) GetByCollID(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*dbmodel.Field, error) {
	tx := s.db.Model(&dbmodel.Field{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var r []*dbmodel.Field

	err := tx.Where("collection_id = ? AND ts = ? AND is_deleted = false", collectionID, ts).Find(&r).Error
	if err != nil {
		log.Error("get fields by collection_id and ts failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *fieldDb) Insert(in []*dbmodel.Field) error {
	err := s.db.CreateInBatches(in, 100).Error
	if err != nil {
		log.Error("insert field failed", zap.Error(err))
		return err
	}

	return nil
}
