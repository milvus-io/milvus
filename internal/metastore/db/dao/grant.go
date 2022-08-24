package dao

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type grantDb struct {
	db *gorm.DB
}

func (g *grantDb) GetGrants(tenantID string, roleID int64, object string, objectName string) ([]*dbmodel.Grant, error) {
	var (
		grants []*dbmodel.Grant
		err    error
	)
	err = g.db.Model(&dbmodel.Grant{}).
		Where(&dbmodel.Grant{RoleID: roleID, Object: object, ObjectName: objectName}).
		Where(dbmodel.GetCommonCondition(tenantID, false)).
		Preload("Role").
		Find(&grants).Error
	if err != nil {
		log.Error("fail to get grants", zap.String("tenant_id", tenantID), zap.Int64("roleID", roleID), zap.String("object", object), zap.String("object_name", objectName), zap.Error(err))
		return nil, err
	}
	return grants, nil
}

func (g *grantDb) Insert(in *dbmodel.Grant) error {
	err := g.db.Create(in).Error
	if err != nil {
		log.Error("fail to insert the grant", zap.Any("in", in), zap.Error(err))
	}
	return err
}

func (g *grantDb) Delete(tenantID string, roleID int64, object string, objectName string) error {
	err := g.db.Model(dbmodel.Grant{}).
		Where(&dbmodel.Grant{RoleID: roleID, Object: object, ObjectName: objectName}).
		Where(dbmodel.GetCommonCondition(tenantID, false)).
		Update("is_deleted", true).Error
	if err != nil {
		log.Error("fail to delete the grant", zap.String("tenant_id", tenantID), zap.Int64("roleID", roleID), zap.String("object", object), zap.String("object_name", objectName), zap.Error(err))
	}
	return err
}
