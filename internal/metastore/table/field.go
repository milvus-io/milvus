package table

import (
	"encoding/json"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type Field struct {
	ID           int64              `db:"id"`
	TenantID     *string            `db:"tenant_id"`
	FieldID      int64              `db:"field_id"`
	FieldName    string             `db:"field_name"`
	IsPrimaryKey bool               `db:"is_primary_key"`
	Description  *string            `db:"description"`
	DataType     schemapb.DataType  `db:"data_type"`
	TypeParams   *string            `db:"type_params"`
	IndexParams  *string            `db:"index_params"`
	AutoID       bool               `db:"auto_id"`
	CollectionID int64              `db:"collection_id"`
	Ts           typeutil.Timestamp `db:"ts"`
	IsDeleted    bool               `db:"is_deleted"`
	CreatedAt    time.Time          `db:"created_at"`
	UpdatedAt    time.Time          `db:"updated_at"`
}

// model <---> db

func ConvertFieldDBToModel(field *Field) *model.Field {
	var retDescription string
	if field.Description != nil {
		retDescription = *field.Description
	}
	var typeParams []commonpb.KeyValuePair
	if field.TypeParams != nil {
		err := json.Unmarshal([]byte(*field.TypeParams), &typeParams)
		if err != nil {
			log.Error("unmarshal TypeParams of field failed", zap.Error(err))
		}
	}
	var indexParams []commonpb.KeyValuePair
	if field.IndexParams != nil {
		err := json.Unmarshal([]byte(*field.IndexParams), &indexParams)
		if err != nil {
			log.Error("unmarshal IndexParams of field failed", zap.Error(err))
		}
	}
	return &model.Field{
		FieldID:      field.FieldID,
		Name:         field.FieldName,
		IsPrimaryKey: field.IsPrimaryKey,
		Description:  retDescription,
		DataType:     field.DataType,
		TypeParams:   funcutil.ConvertToKeyValuePairPointer(typeParams),
		IndexParams:  funcutil.ConvertToKeyValuePairPointer(indexParams),
		AutoID:       field.AutoID,
	}
}
