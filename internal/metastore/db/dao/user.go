package dao

import (
	"errors"
	"fmt"

	"github.com/milvus-io/milvus/internal/common"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type userDb struct {
	db *gorm.DB
}

func (s *userDb) GetByUsername(tenantID string, username string) (*dbmodel.User, error) {
	var r *dbmodel.User

	err := s.db.Model(&dbmodel.User{}).Where("tenant_id = ? AND username = ? AND is_deleted = false", tenantID, username).Take(&r).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, common.NewKeyNotExistError(fmt.Sprintf("%s/%s", tenantID, username))
	}
	if err != nil {
		log.Error("get user by username failed", zap.String("tenant", tenantID), zap.String("username", username), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *userDb) ListUser(tenantID string) ([]*dbmodel.User, error) {
	var users []*dbmodel.User

	err := s.db.Model(&dbmodel.User{}).Where("tenant_id = ? AND is_deleted = false", tenantID).Find(&users).Error
	if err != nil {
		log.Error("list user failed", zap.String("tenant", tenantID), zap.Error(err))
		return nil, err
	}

	return users, nil
}

func (s *userDb) Insert(in *dbmodel.User) error {
	err := s.db.Create(in).Error
	if err != nil {
		log.Error("insert credential_users failed", zap.String("tenant", in.TenantID), zap.String("username", in.Username), zap.Error(err))
		return err
	}

	return nil
}

func (s *userDb) MarkDeletedByUsername(tenantID string, username string) error {
	err := s.db.Model(&dbmodel.User{}).Where("tenant_id = ? AND username = ?", tenantID, username).Update("is_deleted", true).Error
	if err != nil {
		log.Error("update credential_users is_deleted=true failed", zap.String("tenant", tenantID), zap.String("username", username), zap.Error(err))
		return err
	}

	return nil
}

func (s *userDb) UpdatePassword(tenantID string, username string, encryptedPassword string) error {
	err := s.db.Model(&dbmodel.User{}).Where("tenant_id = ? AND username = ?", tenantID, username).Update("encrypted_password", encryptedPassword).Error
	if err != nil {
		log.Error("update password by username failed", zap.String("tenant", tenantID), zap.String("username", username), zap.Error(err))
		return err
	}

	return nil
}
