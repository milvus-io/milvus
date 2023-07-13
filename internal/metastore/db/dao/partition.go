package dao

import (
	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type partitionDb struct {
	db *gorm.DB
}

func (s *partitionDb) GetByCollectionID(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*dbmodel.Partition, error) {
	var r []*dbmodel.Partition

	err := s.db.Model(&dbmodel.Partition{}).Where("tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false", tenantID, collectionID, ts).Find(&r).Error
	if err != nil {
		log.Error("get partitions by collection_id and ts failed", zap.String("tenant", tenantID), zap.Int64("collectionID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
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

func generatePartitionUpdatesWithoutID(in *dbmodel.Partition) map[string]interface{} {
	ret := map[string]interface{}{
		"tenant_id":                   in.TenantID,
		"partition_id":                in.PartitionID,
		"partition_name":              in.PartitionName,
		"partition_created_timestamp": in.PartitionCreatedTimestamp,
		"collection_id":               in.CollectionID,
		"status":                      in.Status,
		"ts":                          in.Ts,
		"is_deleted":                  in.IsDeleted,
		"created_at":                  in.CreatedAt,
		"updated_at":                  in.UpdatedAt,
	}
	return ret
}

func (s *partitionDb) Update(in *dbmodel.Partition) error {
	updates := generatePartitionUpdatesWithoutID(in)
	return s.db.Model(&dbmodel.Partition{}).Where("id = ?", in.ID).Updates(updates).Error
}
