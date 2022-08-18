package dbmodel

import (
	"time"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

type User struct {
	ID                int64     `gorm:"id"`
	TenantID          string    `gorm:"tenant_id"`
	Username          string    `gorm:"username"`
	EncryptedPassword string    `gorm:"encrypted_password"`
	IsSuper           bool      `gorm:"is_super"`
	IsDeleted         bool      `gorm:"is_deleted"`
	CreatedAt         time.Time `gorm:"created_at"`
	UpdatedAt         time.Time `gorm:"updated_at"`
}

func (v User) TableName() string {
	return "credential_users"
}

//go:generate mockery --name=IUserDb
type IUserDb interface {
	GetByUsername(tenantID string, username string) (*User, error)
	ListUser(tenantID string) ([]*User, error)
	Insert(in *User) error
	MarkDeletedByUsername(tenantID string, username string) error
}

// model <---> db

func UnmarshalUserModel(user *User) *model.Credential {
	if user == nil {
		return nil
	}

	return &model.Credential{
		Username:          user.Username,
		EncryptedPassword: user.EncryptedPassword,
	}
}
