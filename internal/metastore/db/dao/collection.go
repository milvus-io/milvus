package dao

import (
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type collectionDb struct {
	db *gorm.DB
}

func (s *collectionDb) GetCollectionIDTs(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*dbmodel.Collection, error) {
	var col dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Select("collection_id, ts").Where("tenant_id = ? AND collection_id = ? AND ts <= ?", tenantID, collectionID, ts).Order("ts desc").Take(&col).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		log.Warn("record not found", zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, fmt.Errorf("record not found, collID=%d, ts=%d", collectionID, ts)
	}
	if err != nil {
		log.Error("get collection ts failed", zap.String("tenant", tenantID), zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return &col, nil
}

func (s *collectionDb) ListCollectionIDTs(tenantID string, ts typeutil.Timestamp) ([]*dbmodel.Collection, error) {
	var r []*dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Select("collection_id, MAX(ts) ts").Where("tenant_id = ? AND ts <= ?", tenantID, ts).Group("collection_id").Find(&r).Error
	if err != nil {
		log.Error("list collection_id & latest ts pairs in collections failed", zap.String("tenant", tenantID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *collectionDb) Get(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*dbmodel.Collection, error) {
	var r dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Where("tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false", tenantID, collectionID, ts).Take(&r).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("collection not found, collID=%d, ts=%d", collectionID, ts)
	}
	if err != nil {
		log.Error("get collection by collection_id and ts failed", zap.String("tenant", tenantID), zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return &r, nil
}

func (s *collectionDb) GetCollectionIDByName(tenantID string, collectionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	var r dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Select("collection_id").Where("tenant_id = ? AND collection_name = ? AND ts <= ?", tenantID, collectionName, ts).Order("ts desc").Take(&r).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, fmt.Errorf("get collection_id by collection_name not found, collName=%s, ts=%d", collectionName, ts)
	}
	if err != nil {
		log.Error("get collection_id by collection_name failed", zap.String("tenant", tenantID), zap.String("collName", collectionName), zap.Uint64("ts", ts), zap.Error(err))
		return 0, err
	}

	return r.CollectionID, nil
}

// Insert used in create & drop collection, needs be an idempotent operation, so we use DoNothing strategy here so it will not throw exception for retry, equivalent to kv catalog
func (s *collectionDb) Insert(in *dbmodel.Collection) error {
	err := s.db.Clauses(clause.OnConflict{
		// constraint UNIQUE (tenant_id, collection_id, ts)
		DoNothing: true,
	}).Create(&in).Error

	if err != nil {
		log.Error("insert collection failed", zap.String("tenant", in.TenantID), zap.Int64("collID", in.CollectionID), zap.Uint64("ts", in.Ts), zap.Error(err))
		return err
	}

	return nil
}
