package model

import (
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

type Field struct {
	FieldID      int64
	Name         string
	IsPrimaryKey bool
	Description  string
	DataType     schemapb.DataType
	TypeParams   []*commonpb.KeyValuePair
	IndexParams  []*commonpb.KeyValuePair
	AutoID       bool
}
