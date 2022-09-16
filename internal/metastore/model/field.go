package model

import (
	"github.com/milvus-io/milvus/internal/common"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/api/schemapb"
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
	State        schemapb.FieldState
}

func (f Field) Available() bool {
	return f.State == schemapb.FieldState_FieldCreated
}

func (f Field) Clone() *Field {
	return &Field{
		FieldID:      f.FieldID,
		Name:         f.Name,
		IsPrimaryKey: f.IsPrimaryKey,
		Description:  f.Description,
		DataType:     f.DataType,
		TypeParams:   common.CloneKeyValuePairs(f.TypeParams),
		IndexParams:  common.CloneKeyValuePairs(f.IndexParams),
		AutoID:       f.AutoID,
		State:        f.State,
	}
}

func CloneFields(fields []*Field) []*Field {
	clone := make([]*Field, 0, len(fields))
	for _, field := range fields {
		clone = append(clone, field.Clone())
	}
	return clone
}

func checkParamsEqual(paramsA, paramsB []*commonpb.KeyValuePair) bool {
	var A common.KeyValuePairs = paramsA
	return A.Equal(paramsB)
}

func (f Field) Equal(other Field) bool {
	return f.FieldID == other.FieldID &&
		f.Name == other.Name &&
		f.IsPrimaryKey == other.IsPrimaryKey &&
		f.Description == other.Description &&
		f.DataType == other.DataType &&
		checkParamsEqual(f.TypeParams, f.TypeParams) &&
		checkParamsEqual(f.IndexParams, other.IndexParams) &&
		f.AutoID == other.AutoID
}

func CheckFieldsEqual(fieldsA, fieldsB []*Field) bool {
	if len(fieldsA) != len(fieldsB) {
		return false
	}
	l := len(fieldsA)
	for i := 0; i < l; i++ {
		if !fieldsA[i].Equal(*fieldsB[i]) {
			return false
		}
	}
	return true
}

func MarshalFieldModel(field *Field) *schemapb.FieldSchema {
	if field == nil {
		return nil
	}

	return &schemapb.FieldSchema{
		FieldID:      field.FieldID,
		Name:         field.Name,
		IsPrimaryKey: field.IsPrimaryKey,
		Description:  field.Description,
		DataType:     field.DataType,
		TypeParams:   field.TypeParams,
		IndexParams:  field.IndexParams,
		AutoID:       field.AutoID,
	}
}

func MarshalFieldModels(fields []*Field) []*schemapb.FieldSchema {
	if fields == nil {
		return nil
	}

	fieldSchemas := make([]*schemapb.FieldSchema, len(fields))
	for idx, field := range fields {
		fieldSchemas[idx] = MarshalFieldModel(field)
	}
	return fieldSchemas
}

func UnmarshalFieldModel(fieldSchema *schemapb.FieldSchema) *Field {
	if fieldSchema == nil {
		return nil
	}

	return &Field{
		FieldID:      fieldSchema.FieldID,
		Name:         fieldSchema.Name,
		IsPrimaryKey: fieldSchema.IsPrimaryKey,
		Description:  fieldSchema.Description,
		DataType:     fieldSchema.DataType,
		TypeParams:   fieldSchema.TypeParams,
		IndexParams:  fieldSchema.IndexParams,
		AutoID:       fieldSchema.AutoID,
	}
}

func UnmarshalFieldModels(fieldSchemas []*schemapb.FieldSchema) []*Field {
	if fieldSchemas == nil {
		return nil
	}

	fields := make([]*Field, len(fieldSchemas))
	for idx, fieldSchema := range fieldSchemas {
		fields[idx] = UnmarshalFieldModel(fieldSchema)
	}
	return fields
}
