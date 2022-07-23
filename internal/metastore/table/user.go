package table

import (
	"time"

	"github.com/milvus-io/milvus/internal/metastore/model"
)

type User struct {
	TenantID          *string   `db:"tenant_id"`
	Username          string    `db:"username"`
	EncryptedPassword string    `db:"encrypted_password"`
	IsSuper           bool      `db:"is_super"`
	IsDeleted         bool      `db:"is_deleted"`
	CreatedAt         time.Time `db:"created_at"`
	UpdatedAt         time.Time `db:"updated_at"`
}

// model <---> db

func ConvertUserDBToModel(user *User) *model.Credential {
	return &model.Credential{
		Username:          user.Username,
		EncryptedPassword: user.EncryptedPassword,
	}
}
