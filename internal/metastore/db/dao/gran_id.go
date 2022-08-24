package dao

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type grantIDDb struct {
	db *gorm.DB
}

func (g *grantIDDb) GetGrantIDs(tenantID string, grantID int64, privilege string, preloadGrant bool, preloadGrantor bool) ([]*dbmodel.GrantID, error) {
	var (
		grantIDs []*dbmodel.GrantID
		db       *gorm.DB
		err      error
	)
	db = g.db.Model(&dbmodel.GrantID{}).
		Where(&dbmodel.GrantID{GrantID: grantID, Privilege: privilege}).
		Where(dbmodel.GetCommonCondition(tenantID, false))
	if preloadGrant {
		db = db.Preload("Grant")
	}
	if preloadGrantor {
		db = db.Preload("Grantor")
	}
	err = db.Find(&grantIDs).Error
	if err != nil {
		log.Error("fail to get grant ids", zap.String("tenant_id", tenantID), zap.Int64("grantID", grantID), zap.String("privilege", privilege), zap.Error(err))
		return nil, err
	}
	return grantIDs, err
}

func (g *grantIDDb) Insert(in *dbmodel.GrantID) error {
	err := g.db.Create(in).Error
	if err != nil {
		log.Error("fail to insert the grant-id", zap.Any("in", in), zap.Error(err))
	}
	return err
}

func (g *grantIDDb) Delete(tenantID string, grantID int64, privilege string) error {
	err := g.db.Model(dbmodel.GrantID{}).
		Where(&dbmodel.GrantID{GrantID: grantID, Privilege: privilege}).
		Where(dbmodel.GetCommonCondition(tenantID, false)).
		Update("is_deleted", true).Error
	if err != nil {
		log.Error("fail to delete the user-role", zap.String("tenant_id", tenantID), zap.Int64("grantID", grantID), zap.String("privilege", privilege), zap.Error(err))
	}
	return err
}
