package dbmodel

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type CollectionAlias struct {
	ID              int64              `gorm:"id"`
	TenantID        string             `gorm:"tenant_id"`
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

//go:generate mockery --name=ICollAliasDb
type ICollAliasDb interface {
	Insert(in []*CollectionAlias) error
	GetCollectionIDByAlias(tenantID string, alias string, ts typeutil.Timestamp) (typeutil.UniqueID, error)
	ListCollectionIDTs(tenantID string, ts typeutil.Timestamp) ([]*CollectionAlias, error)
	List(tenantID string, cidTsPairs []*CollectionAlias) ([]*CollectionAlias, error)
}
