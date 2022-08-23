package dao

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type roleDb struct {
	db *gorm.DB
}

func (r *roleDb) GetRoles(tenantID string, name string) ([]*dbmodel.Role, error) {
	var (
		roles []*dbmodel.Role
		err   error
	)
	err = r.db.Model(&dbmodel.Role{}).Where(&dbmodel.Role{Name: name}).Where(dbmodel.GetCommonCondition(tenantID, false)).Find(&roles).Error
	if err != nil {
		log.Error("fail to get roles", zap.String("tenant_id", tenantID), zap.String("name", name), zap.Error(err))
		return nil, err
	}
	return roles, nil
}

func (r *roleDb) Insert(in *dbmodel.Role) error {
	err := r.db.Create(in).Error
	if err != nil {
		log.Error("fail to insert the role", zap.Any("in", in), zap.Error(err))
	}
	return err
}

func (r *roleDb) Delete(tenantID string, name string) error {
	err := r.db.Model(dbmodel.Role{}).Where(&dbmodel.Role{Name: name}).Where(dbmodel.GetCommonCondition(tenantID, false)).Update("is_deleted", true).Error
	if err != nil {
		log.Error("fail to delete the role", zap.String("tenant_id", tenantID), zap.String("name", name), zap.Error(err))
	}
	return err
}
