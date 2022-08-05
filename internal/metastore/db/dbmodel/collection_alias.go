package dbmodel

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type CollectionAlias struct {
	ID              int64              `gorm:"id"`
	TenantID        *string            `gorm:"tenant_id"`
	CollectionID    int64              `gorm:"collection_id"`
	CollectionAlias string             `gorm:"collection_alias"`
	Ts              typeutil.Timestamp `gorm:"ts"`
	IsDeleted       bool               `gorm:"is_deleted"`
	CreatedAt       time.Time          `gorm:"created_at"`
	UpdatedAt       time.Time          `gorm:"updated_at"`
}

func (v CollectionAlias) TableName() string {
	return "collection_aliases"
}

type ICollAliasDb interface {
	Insert(in []*CollectionAlias) error
	Update(tenantID string, collID typeutil.UniqueID, alias string, ts typeutil.Timestamp) error
	MarkDeleted(tenantID string, collID typeutil.UniqueID, aliases []string) error
	ListCidTs(tenantID string, ts typeutil.Timestamp) ([]*CollectionAlias, error)
	List(tenantID string, cidTsPairs []*CollectionAlias) ([]*CollectionAlias, error)
}
