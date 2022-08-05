package dao

import (
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type collAliasDb struct {
	db *gorm.DB
}

func (s *collAliasDb) Insert(in []*dbmodel.CollectionAlias) error {
	err := s.db.CreateInBatches(in, 100).Error
	if err != nil {
		log.Error("insert collection alias failed", zap.Error(err))
		return err
	}

	return nil
}

func (s *collAliasDb) Update(tenantID string, collID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error {
	tx := s.db.Model(&dbmodel.CollectionAlias{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("collection_alias = ? AND ts = ?", collID, ts).Updates(dbmodel.CollectionAlias{
		CollectionID: collID,
		UpdatedAt:    time.Now(),
	}).Error

	if err != nil {
		log.Error("update collection_aliases failed", zap.Int64("collID", collID), zap.String("alias", alias), zap.Uint64("ts", ts), zap.Error(err))
		return err
	}

	return nil
}

func (s *collAliasDb) MarkDeleted(tenantID string, collID typeutil.UniqueID, aliases []string) error {
	tx := s.db.Model(&dbmodel.CollectionAlias{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("collection_id = ? AND ts = 0", collID).Where("collection_alias in (?)", aliases).Updates(dbmodel.CollectionAlias{
		IsDeleted: true,
		UpdatedAt: time.Now(),
	}).Error

	if err != nil {
		log.Error("update collection_aliases is_deleted=true failed", zap.Int64("collID", collID), zap.Strings("aliases", aliases), zap.Error(err))
		return err
	}

	return nil
}

func (s *collAliasDb) ListCidTs(tenantID string, ts typeutil.Timestamp) ([]*dbmodel.CollectionAlias, error) {
	tx := s.db.Model(&dbmodel.CollectionAlias{}).Select("collection_id, MAX(ts) ts")

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var r []*dbmodel.CollectionAlias
	err := tx.Where("ts > 0 and ts <= ?", ts).Group("collection_id").Find(&r).Error
	if err != nil {
		log.Error("get latest ts <= paramTs failed", zap.Uint64("paramTs", ts), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *collAliasDb) List(tenantID string, cidTsPairs []*dbmodel.CollectionAlias) ([]*dbmodel.CollectionAlias, error) {
	tx := s.db.Model(&dbmodel.CollectionAlias{}).Select("collection_id, collection_alias")

	inValues := make([][]interface{}, 0, len(cidTsPairs))
	for _, pair := range cidTsPairs {
		in := []interface{}{pair.CollectionID, pair.Ts}
		inValues = append(inValues, in)
	}

	var collAliases []*dbmodel.CollectionAlias

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	err := tx.Where("is_deleted = false").Where("(collection_id, ts) IN ?", inValues).Find(&collAliases).Error
	if err != nil {
		log.Error("list collection_id and alias failed", zap.Error(err))
		return nil, err
	}

	return collAliases, nil
}
