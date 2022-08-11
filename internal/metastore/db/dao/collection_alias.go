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

type collAliasDb struct {
	db *gorm.DB
}

func (s *collAliasDb) Insert(in []*dbmodel.CollectionAlias) error {
	err := s.db.Clauses(clause.OnConflict{
		// constraint UNIQUE (tenant_id, collection_alias, ts)
		DoNothing: true,
	}).Create(&in).Error

	if err != nil {
		log.Error("insert collection alias failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *collAliasDb) GetCollectionIDByAlias(tenantID string, alias string, ts typeutil.Timestamp) (typeutil.UniqueID, error) {
	var r dbmodel.CollectionAlias

	err := s.db.Model(&dbmodel.CollectionAlias{}).Select("collection_id").Where("tenant_id = ? AND collection_alias = ? AND ts <= ?", tenantID, alias, ts).Order("ts desc").Take(&r).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, fmt.Errorf("get collection_id by alias not found, alias=%s, ts=%d", alias, ts)
	}
	if err != nil {
		log.Error("get collection_id by alias failed", zap.String("tenant", tenantID), zap.String("alias", alias), zap.Uint64("ts", ts), zap.Error(err))
		return 0, err
	}

	return r.CollectionID, nil
}

func (s *collAliasDb) ListCollectionIDTs(tenantID string, ts typeutil.Timestamp) ([]*dbmodel.CollectionAlias, error) {
	var r []*dbmodel.CollectionAlias

	err := s.db.Model(&dbmodel.CollectionAlias{}).Select("collection_id, MAX(ts) ts").Where("tenant_id = ? AND ts <= ?", tenantID, ts).Group("collection_id").Find(&r).Error
	if err != nil {
		log.Error("list collection_id & latest ts pairs in collection_aliases failed", zap.String("tenant", tenantID), zap.Uint64("ts", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *collAliasDb) List(tenantID string, cidTsPairs []*dbmodel.CollectionAlias) ([]*dbmodel.CollectionAlias, error) {
	var collAliases []*dbmodel.CollectionAlias

	inValues := make([][]interface{}, 0, len(cidTsPairs))
	for _, pair := range cidTsPairs {
		in := []interface{}{pair.CollectionID, pair.Ts}
		inValues = append(inValues, in)
	}

	err := s.db.Model(&dbmodel.CollectionAlias{}).Select("collection_id, collection_alias").
		Where("tenant_id = ? AND is_deleted = false AND (collection_id, ts) IN ?", tenantID, inValues).Find(&collAliases).Error
	if err != nil {
		log.Error("list alias by collection_id and alias pairs failed", zap.String("tenant", tenantID), zap.Any("collIdTs", inValues), zap.Error(err))
		return nil, err
	}

	return collAliases, nil
}
