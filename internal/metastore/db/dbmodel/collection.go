package dbmodel

import (
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
	Properties       string             `gorm:"properties"`
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
			log.Error("unmarshal collection start positions error", zap.Int64("collectionID", coll.CollectionID), zap.Uint64("ts", coll.Ts), zap.Error(err))
			return nil, err
		}
	}

	properties, err := UnmarshalProperties(coll.Properties)
	if err != nil {
		log.Error("unmarshal collection properties error", zap.Int64("collectionID", coll.CollectionID),
			zap.String("properties", coll.Properties), zap.Error(err))
		return nil, err
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
		Properties:       properties,
	}, nil
}

func UnmarshalProperties(propertiesStr string) ([]*commonpb.KeyValuePair, error) {
	if propertiesStr == "" {
		return nil, nil
	}

	var properties []*commonpb.KeyValuePair
	if propertiesStr != "" {
		if err := json.Unmarshal([]byte(propertiesStr), &properties); err != nil {
			return nil, fmt.Errorf("failed to unmarshal properties: %s", err.Error())
		}
	}

	return properties, nil
}

func MarshalProperties(properties []*commonpb.KeyValuePair) (string, error) {
	if properties == nil {
		return "", nil
	}

	propertiesBytes, err := json.Marshal(properties)
	if err != nil {
		return "", fmt.Errorf("failed to marshal properties: %s", err.Error())
	}
	return string(propertiesBytes), nil
}
