package dbmodel

import (
	"encoding/json"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Field struct {
	ID           int64              `gorm:"id"`
	TenantID     string             `gorm:"tenant_id"`
	FieldID      int64              `gorm:"field_id"`
	FieldName    string             `gorm:"field_name"`
	IsPrimaryKey bool               `gorm:"is_primary_key"`
	Description  string             `gorm:"description"`
	DataType     schemapb.DataType  `gorm:"data_type"`
	TypeParams   string             `gorm:"type_params"`
	IndexParams  string             `gorm:"index_params"`
	AutoID       bool               `gorm:"auto_id"`
	CollectionID int64              `gorm:"collection_id"`
	Ts           typeutil.Timestamp `gorm:"ts"`
	IsDeleted    bool               `gorm:"is_deleted"`
	CreatedAt    time.Time          `gorm:"created_at"`
	UpdatedAt    time.Time          `gorm:"updated_at"`
}

func (v Field) TableName() string {
	return "field_schemas"
}

//go:generate mockery --name=IFieldDb
type IFieldDb interface {
	GetByCollectionID(tenantID string, collectionID typeutil.UniqueID, ts typeutil.Timestamp) ([]*Field, error)
	Insert(in []*Field) error
}

// model <---> db

func UnmarshalFieldModel(fields []*Field) ([]*model.Field, error) {
	r := make([]*model.Field, 0, len(fields))
	for _, f := range fields {
		fd, err := ConvertFieldDBToModel(f)
		if err != nil {
			return nil, err
		}
		r = append(r, fd)
	}

	return r, nil
}

func ConvertFieldDBToModel(field *Field) (*model.Field, error) {
	var typeParams []commonpb.KeyValuePair
	if field.TypeParams != "" {
		err := json.Unmarshal([]byte(field.TypeParams), &typeParams)
		if err != nil {
			log.Error("unmarshal TypeParams of field failed", zap.Int64("collectionID", field.CollectionID),
				zap.Int64("fieldID", field.FieldID), zap.String("fieldName", field.FieldName), zap.Error(err))
			return nil, err
		}
	}

	var indexParams []commonpb.KeyValuePair
	if field.IndexParams != "" {
		err := json.Unmarshal([]byte(field.IndexParams), &indexParams)
		if err != nil {
			log.Error("unmarshal IndexParams of field failed", zap.Int64("collectionID", field.CollectionID),
				zap.Int64("fieldID", field.FieldID), zap.String("fieldName", field.FieldName), zap.Error(err))
			return nil, err
		}
	}

	return &model.Field{
		FieldID:      field.FieldID,
		Name:         field.FieldName,
		IsPrimaryKey: field.IsPrimaryKey,
		Description:  field.Description,
		DataType:     field.DataType,
		TypeParams:   funcutil.ConvertToKeyValuePairPointer(typeParams),
		IndexParams:  funcutil.ConvertToKeyValuePairPointer(indexParams),
		AutoID:       field.AutoID,
	}, nil
}
