package table

import (
	"time"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Partition struct {
	ID                        int64              `db:"id"`
	TenantID                  *string            `db:"tenant_id"`
	PartitionID               int64              `db:"partition_id"`
	PartitionName             string             `db:"partition_name"`
	PartitionCreatedTimestamp uint64             `db:"partition_created_timestamp"`
	CollectionID              int64              `db:"collection_id"`
	Ts                        typeutil.Timestamp `db:"ts"`
	IsDeleted                 bool               `db:"is_deleted"`
	CreatedAt                 time.Time          `db:"created_at"`
	UpdatedAt                 time.Time          `db:"updated_at"`
}

// model <---> db

func ConvertPartitionDBToModel(partiton *Partition) *model.Partition {
	return &model.Partition{
		PartitionID:               partiton.PartitionID,
		PartitionName:             partiton.PartitionName,
		PartitionCreatedTimestamp: partiton.PartitionCreatedTimestamp,
	}
}
