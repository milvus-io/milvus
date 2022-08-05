package dao

import (
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type collectionDb struct {
	db *gorm.DB
}

func (s *collectionDb) GetCidTs(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*dbmodel.Collection, error) {
	if ts > 0 {
		tx := s.db.Model(&dbmodel.Collection{})

		if tenantID != "" {
			tx = tx.Where("tenant_id = ?", tenantID)
		}

		var col dbmodel.Collection

		err := tx.Where("collection_id = ? AND ts > 0 and ts <= ?", collectionID, ts).Order("ts desc").Limit(1).Find(&col).Error
		if err != nil {
			log.Error("get collection ts failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
			return nil, err
		}

		return &col, nil
	}

	return &dbmodel.Collection{CollectionID: collectionID, Ts: 0}, nil
}

func (s *collectionDb) ListCidTs(ts typeutil.Timestamp) ([]*dbmodel.Collection, error) {
	var r []*dbmodel.Collection
	err := s.db.Model(&dbmodel.Collection{}).Select("collection_id, MAX(ts) ts").Where("ts > 0 and ts <= ?", ts).Group("collection_id").Find(&r).Error
	if err != nil {
		log.Error("get latest ts <= paramTs failed", zap.Uint64("paramTs", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *collectionDb) Get(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*dbmodel.Collection, error) {
	tx := s.db.Model(&dbmodel.Collection{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var r dbmodel.Collection

	err := tx.Where("collection_id = ? AND ts = ? AND is_deleted = false", collectionID, ts).Find(&r).Error
	if err != nil {
		log.Error("get collection by collection_id and ts failed", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return &r, nil
}

func (s *collectionDb) GetIDByName(tenantID string, collectionName string) (typeutil.UniqueID, error) {
	tx := s.db.Model(&dbmodel.Collection{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var r dbmodel.Collection

	err := tx.Where("collection_name = ?", collectionName).Limit(1).Find(&r).Error
	if err != nil {
		log.Error("get collection_id by collection_name failed", zap.String("collName", collectionName), zap.Error(err))
		return 0, err
	}

	return r.CollectionID, nil
}

func (s *collectionDb) Insert(in *dbmodel.Collection) error {
	err := s.db.Create(in).Error
	if err != nil {
		log.Error("insert collection failed", zap.Int64("collID", in.CollectionID), zap.Uint64("ts", in.Ts), zap.Error(err))
		return err
	}

	return nil
}

func (s *collectionDb) MarkDeleted(tenantID string, collID typeutil.UniqueID) error {
	tx := s.db.Model(&dbmodel.Collection{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("collection_id = ? AND ts = 0", collID).Updates(dbmodel.Collection{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error

	if err != nil {
		log.Error("update is_deleted=true failed", zap.Int64("collID", collID), zap.Error(err))
		return err
	}

	return nil
}
