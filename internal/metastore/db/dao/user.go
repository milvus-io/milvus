package dao

import (
	"errors"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/db/dbmodel"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type userDb struct {
	db *gorm.DB
}

func (s *userDb) GetByUsername(tenantID string, username string) (*dbmodel.User, error) {
	tx := s.db.Model(&dbmodel.User{})

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var r *dbmodel.User

	err := tx.Where("username = ? AND is_deleted = false", username).Take(&r).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		log.Warn("cannot find any user by username", zap.String("username", username))
		return nil, nil
	} else if err != nil {
		log.Error("get user by username failed", zap.String("username", username), zap.Error(err))
		return nil, err
	}

	return r, nil
}

func (s *userDb) ListUsername(tenantID string) ([]string, error) {
	tx := s.db.Model(&dbmodel.User{}).Select("username")

	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}

	var usernames []string

	err := tx.Where("is_deleted = false").Find(&usernames).Error
	if err != nil {
		log.Error("list usernames failed", zap.Error(err))
		return nil, err
	}

	return usernames, nil
}

func (s *userDb) Insert(in *dbmodel.User) error {
	err := s.db.Create(in).Error
	if err != nil {
		log.Error("insert credential_users failed", zap.String("username", in.Username), zap.Error(err))
		return err
	}

	return nil
}

func (s *userDb) MarkDeletedByUsername(tenantID string, username string) error {
	tx := s.db.Model(&dbmodel.Index{})
	if tenantID != "" {
		tx = tx.Where("tenant_id = ?", tenantID)
	}
	err := tx.Where("username = ?", username).Update("is_deleted", true).Error
	if err != nil {
		log.Error("update credential_users is_deleted=true failed", zap.String("username", username), zap.Error(err))
		return err
	}

	return nil
}
