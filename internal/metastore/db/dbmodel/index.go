package dbmodel

import (
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Index struct {
	ID              int64     `gorm:"id"`
	TenantID        string    `gorm:"tenant_id"`
	FieldID         int64     `gorm:"field_id"`
	CollectionID    int64     `gorm:"collection_id"`
	IndexID         int64     `gorm:"index_id"`
	IndexName       string    `gorm:"index_name"`
	IndexParams     string    `gorm:"index_params"`
	TypeParams      string    `gorm:"type_params"`
	UserIndexParams string    `gorm:"user_index_params"`
	IsAutoIndex     bool      `gorm:"is_auto_index"`
	CreateTime      uint64    `gorm:"create_time"`
	IsDeleted       bool      `gorm:"is_deleted"`
	CreatedAt       time.Time `gorm:"created_at"`
	UpdatedAt       time.Time `gorm:"updated_at"`
}

func (v Index) TableName() string {
	return "indexes"
}

// ------------- search result -------------

type IndexResult struct {
	FieldID         int64
	CollectionID    int64
	IndexID         int64
	IndexName       string
	TypeParams      string
	IndexParams     string
	CreateTime      uint64
	IsDeleted       bool
	IsAutoIndex     bool
	UserIndexParams string
}

//go:generate mockery --name=IIndexDb
type IIndexDb interface {
	Get(tenantID string, collectionID typeutil.UniqueID) ([]*Index, error)
	List(tenantID string) ([]*IndexResult, error)
	Insert(in []*Index) error
	Update(in *Index) error
	MarkDeletedByCollectionID(tenantID string, collID typeutil.UniqueID) error
	MarkDeletedByIndexID(tenantID string, idxID typeutil.UniqueID) error
}

// model <---> db

func UnmarshalIndexModel(inputs []*IndexResult) ([]*model.Index, error) {
	result := make([]*model.Index, 0, len(inputs))
	for _, ir := range inputs {
		var indexParams []commonpb.KeyValuePair
		if ir.IndexParams != "" {
			err := json.Unmarshal([]byte(ir.IndexParams), &indexParams)
			if err != nil {
				log.Error("unmarshal IndexParams of index failed", zap.Int64("collectionID", ir.CollectionID),
					zap.Int64("indexID", ir.IndexID), zap.String("indexName", ir.IndexName), zap.Error(err))
				return nil, err
			}
		}
		var userIndexParams []commonpb.KeyValuePair
		if ir.UserIndexParams != "" {
			err := json.Unmarshal([]byte(ir.UserIndexParams), &userIndexParams)
			if err != nil {
				log.Error("unmarshal UserIndexParams of index failed", zap.Int64("collectionID", ir.CollectionID),
					zap.Int64("indexID", ir.IndexID), zap.String("indexName", ir.IndexName), zap.Error(err))
				return nil, err
			}
		}

		var typeParams []commonpb.KeyValuePair
		if ir.TypeParams != "" {
			err := json.Unmarshal([]byte(ir.TypeParams), &typeParams)
			if err != nil {
				log.Error("unmarshal TypeParams of index failed", zap.Int64("collectionID", ir.CollectionID),
					zap.Int64("indexID", ir.IndexID), zap.String("indexName", ir.IndexName), zap.Error(err))
				return nil, err
			}
		}

		idx := &model.Index{
			CollectionID:    ir.CollectionID,
			FieldID:         ir.FieldID,
			IndexID:         ir.IndexID,
			IndexName:       ir.IndexName,
			IndexParams:     funcutil.ConvertToKeyValuePairPointer(indexParams),
			TypeParams:      funcutil.ConvertToKeyValuePairPointer(typeParams),
			CreateTime:      ir.CreateTime,
			IsDeleted:       ir.IsDeleted,
			IsAutoIndex:     ir.IsAutoIndex,
			UserIndexParams: funcutil.ConvertToKeyValuePairPointer(userIndexParams),
		}
		result = append(result, idx)
	}

	return result, nil
}

func ConvertIndexDBToModel(indexes []*Index) []common.Int64Tuple {
	r := make([]common.Int64Tuple, 0, len(indexes))

	for _, idx := range indexes {
		tuple := common.Int64Tuple{
			Key:   idx.FieldID,
			Value: idx.IndexID,
		}
		r = append(r, tuple)
	}

	return r
}
