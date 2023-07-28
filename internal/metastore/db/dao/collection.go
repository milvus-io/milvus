package dao

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type collectionDb struct {
	db *gorm.DB
}

func (s *collectionDb) GetCollectionIDTs(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*dbmodel.Collection, error) {
	var col dbmodel.Collection

	err := s.db.Model(&dbmodel.Collection{}).Select("collection_id, ts").Where("tenant_id = ? AND collection_id = ? AND ts <= ?", tenantID, collectionID, ts).Order("ts desc").Take(&col).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		log.Warn("record not found", zap.Int64("collectionID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, fmt.Errorf("record not found, collID=%d, ts=%d", collectionID, ts)
	}
	if err != nil {
		log.Error("get collection ts failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
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
		log.Error("get collection by collection_id and ts failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
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
		log.Error("insert collection failed", zap.String("tenant", in.TenantID), zap.Int64("collectionID", in.CollectionID), zap.Uint64("ts", in.Ts), zap.Error(err))
		return err
	}

	return nil
}

func generateCollectionUpdatesWithoutID(in *dbmodel.Collection) map[string]interface{} {
	ret := map[string]interface{}{
		"tenant_id":         in.TenantID,
		"collection_id":     in.CollectionID,
		"collection_name":   in.CollectionName,
		"description":       in.Description,
		"auto_id":           in.AutoID,
		"shards_num":        in.ShardsNum,
		"start_position":    in.StartPosition,
		"consistency_level": in.ConsistencyLevel,
		"status":            in.Status,
		"properties":        in.Properties,
		"ts":                in.Ts,
		"is_deleted":        in.IsDeleted,
		"created_at":        in.CreatedAt,
		"updated_at":        in.UpdatedAt,
	}
	return ret
}

func (s *collectionDb) Update(in *dbmodel.Collection) error {
	updates := generateCollectionUpdatesWithoutID(in)
	return s.db.Model(&dbmodel.Collection{}).Where("id = ?", in.ID).Updates(updates).Error
}
