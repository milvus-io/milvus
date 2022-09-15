package dbmodel

import (
	"encoding/json"
	"time"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type Collection struct {
	ID               int64              `gorm:"id"`
	TenantID         string             `gorm:"tenant_id"`
	CollectionID     int64              `gorm:"collection_id"`
	CollectionName   string             `gorm:"collection_name"`
	Description      string             `gorm:"description"`
	AutoID           bool               `gorm:"auto_id"`
	ShardsNum        int32              `gorm:"shards_num"`
	StartPosition    string             `gorm:"start_position"`
	ConsistencyLevel int32              `gorm:"consistency_level"`
	Status           int32              `gorm:"status"`
	Ts               typeutil.Timestamp `gorm:"ts"`
	IsDeleted        bool               `gorm:"is_deleted"`
	CreatedAt        time.Time          `gorm:"created_at"`
	UpdatedAt        time.Time          `gorm:"updated_at"`
}

func (v Collection) TableName() string {
	return "collections"
}

//go:generate mockery --name=ICollectionDb
type ICollectionDb interface {
	// GetCollectionIdTs get the largest timestamp that less than or equal to param ts, no matter is_deleted is true or false.
	GetCollectionIDTs(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*Collection, error)
	ListCollectionIDTs(tenantID string, ts typeutil.Timestamp) ([]*Collection, error)
	Get(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) (*Collection, error)
	GetCollectionIDByName(tenantID string, collectionName string, ts typeutil.Timestamp) (typeutil.UniqueID, error)
	Insert(in *Collection) error
	Update(in *Collection) error
}

// model <---> db

func UnmarshalCollectionModel(coll *Collection) (*model.Collection, error) {
	var startPositions []*commonpb.KeyDataPair
	if coll.StartPosition != "" {
		err := json.Unmarshal([]byte(coll.StartPosition), &startPositions)
		if err != nil {
			log.Error("unmarshal collection start positions error", zap.Int64("collID", coll.CollectionID), zap.Uint64("ts", coll.Ts), zap.Error(err))
			return nil, err
		}
	}

	return &model.Collection{
		TenantID:         coll.TenantID,
		CollectionID:     coll.CollectionID,
		Name:             coll.CollectionName,
		Description:      coll.Description,
		AutoID:           coll.AutoID,
		ShardsNum:        coll.ShardsNum,
		StartPositions:   startPositions,
		ConsistencyLevel: commonpb.ConsistencyLevel(coll.ConsistencyLevel),
		CreateTime:       coll.Ts,
	}, nil
}
