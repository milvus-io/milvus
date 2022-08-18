package dao

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type userRoleDb struct {
	db *gorm.DB
}

func (u *userRoleDb) GetUserRoles(tenantID string, userID int64, roleID int64) ([]*dbmodel.UserRole, error) {
	var (
		userRoles []*dbmodel.UserRole
		err       error
	)
	err = u.db.Model(&dbmodel.UserRole{}).Where(&dbmodel.UserRole{UserID: userID, RoleID: roleID}).Where(dbmodel.GetCommonCondition(tenantID, false)).Preload("User").Preload("Role").Find(&userRoles).Error
	if err != nil {
		log.Error("fail to get user-roles", zap.String("tenant_id", tenantID), zap.Int64("userID", userID), zap.Int64("roleID", roleID), zap.Error(err))
		return nil, err
	}
	return userRoles, nil
}

func (u *userRoleDb) Insert(in *dbmodel.UserRole) error {
	err := u.db.Create(in).Error
	if err != nil {
		log.Error("fail to insert the user-role", zap.Any("in", in), zap.Error(err))
	}
	return err
}

func (u *userRoleDb) Delete(tenantID string, userID int64, roleID int64) error {
	err := u.db.Model(dbmodel.UserRole{}).Where(&dbmodel.UserRole{UserID: userID, RoleID: roleID}).Where(dbmodel.GetCommonCondition(tenantID, false)).Update("is_deleted", true).Error
	if err != nil {
		log.Error("fail to delete the user-role", zap.String("tenant_id", tenantID), zap.Int64("userID", userID), zap.Int64("roleID", roleID), zap.Error(err))
	}
	return err
}
