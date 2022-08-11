package dao

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type collChannelDb struct {
	db *gorm.DB
}

func (s *collChannelDb) GetByCollectionID(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*dbmodel.CollectionChannel, error) {
	var r []*dbmodel.CollectionChannel

	err := s.db.Model(&dbmodel.CollectionChannel{}).Where("tenant_id = ? AND collection_id = ? AND ts = ? AND is_deleted = false", tenantID, collectionID, ts).Find(&r).Error
	if err != nil {
		log.Error("get channels by collection_id and ts failed", zap.String("tenant", tenantID), zap.Int64("collID", collectionID), zap.Uint64("ts", ts), zap.Error(err))
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
