package dbmodel

import (
	"time"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type Partition struct {
	ID                        int64              `gorm:"id"`
	TenantID                  string             `gorm:"tenant_id"`
	PartitionID               int64              `gorm:"partition_id"`
	PartitionName             string             `gorm:"partition_name"`
	PartitionCreatedTimestamp uint64             `gorm:"partition_created_timestamp"`
	CollectionID              int64              `gorm:"collection_id"`
	Ts                        typeutil.Timestamp `gorm:"ts"`
	IsDeleted                 bool               `gorm:"is_deleted"`
	CreatedAt                 time.Time          `gorm:"created_at"`
	UpdatedAt                 time.Time          `gorm:"updated_at"`
}

func (v Partition) TableName() string {
	return "partitions"
}

//go:generate mockery --name=IPartitionDb
type IPartitionDb interface {
	GetByCollectionID(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*Partition, error)
	Insert(in []*Partition) error
	//MarkDeleted(tenantID string, collID typeutil.UniqueID) error
}

// model <---> db

func UnmarshalPartitionModel(partitons []*Partition) []*model.Partition {
	r := make([]*model.Partition, 0, len(partitons))
	for _, p := range partitons {
		partition := ConvertPartitionDBToModel(p)
		r = append(r, partition)
	}

	return r
}

func ConvertPartitionDBToModel(partiton *Partition) *model.Partition {
	return &model.Partition{
		PartitionID:               partiton.PartitionID,
		PartitionName:             partiton.PartitionName,
		PartitionCreatedTimestamp: partiton.PartitionCreatedTimestamp,
	}
}
