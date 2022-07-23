package table

import (
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type CollectionAlias struct {
	ID              int64              `db:"id"`
	TenantID        *string            `db:"tenant_id"`
	CollectionID    int64              `db:"collection_id"`
	CollectionAlias string             `db:"collection_alias"`
	Ts              typeutil.Timestamp `db:"ts"`
	IsDeleted       bool               `db:"is_deleted"`
	CreatedAt       time.Time          `db:"created_at"`
	UpdatedAt       time.Time          `db:"updated_at"`
}
