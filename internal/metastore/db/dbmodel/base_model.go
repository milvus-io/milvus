package dbmodel

import "time"

type Base struct {
	ID        int64     `gorm:"id"`
	TenantID  string    `gorm:"tenant_id"`
	IsDeleted bool      `gorm:"is_deleted"`
	CreatedAt time.Time `gorm:"created_at"`
	UpdatedAt time.Time `gorm:"updated_at"`
}
