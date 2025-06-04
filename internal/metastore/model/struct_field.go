package model

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type StructArrayField struct {
	FieldID     int64
	Name        string
	Description string
	Fields      []*Field
}

func (s *StructArrayField) Clone() *StructArrayField {
	return &StructArrayField{
		FieldID:     s.FieldID,
		Name:        s.Name,
		Description: s.Description,
		Fields:      CloneFields(s.Fields),
	}
}

func CloneStructArrayFields(structArrayFields []*StructArrayField) []*StructArrayField {
	clone := make([]*StructArrayField, len(structArrayFields))
	for i, structArrayField := range structArrayFields {
		clone[i] = structArrayField.Clone()
	}
	return clone
}

func (s *StructArrayField) Equal(other StructArrayField) bool {
	return s.FieldID == other.FieldID &&
		s.Name == other.Name &&
		s.Description == other.Description &&
		CheckFieldsEqual(s.Fields, other.Fields)
}

func CheckStructArrayFieldsEqual(structArrayFieldsA, structArrayFieldsB []*StructArrayField) bool {
	if len(structArrayFieldsA) != len(structArrayFieldsB) {
		return false
	}

	mapA := make(map[int64]*StructArrayField)
	for _, f := range structArrayFieldsA {
		mapA[f.FieldID] = f
	}

	for _, f := range structArrayFieldsB {
		if other, exists := mapA[f.FieldID]; !exists || !f.Equal(*other) {
			return false
		}
	}
	return true
}

func MarshalStructArrayFieldModel(structArrayField *StructArrayField) *schemapb.StructArrayFieldSchema {
	if structArrayField == nil {
		return nil
	}

	return &schemapb.StructArrayFieldSchema{
		FieldID:     structArrayField.FieldID,
		Name:        structArrayField.Name,
		Description: structArrayField.Description,
		Fields:      MarshalFieldModels(structArrayField.Fields),
	}
}

func MarshalStructArrayFieldModels(fieldSchemas []*StructArrayField) []*schemapb.StructArrayFieldSchema {
	if fieldSchemas == nil {
		return nil
	}

	structArrayFields := make([]*schemapb.StructArrayFieldSchema, len(fieldSchemas))
	for idx, structArrayField := range fieldSchemas {
		structArrayFields[idx] = MarshalStructArrayFieldModel(structArrayField)
	}
	return structArrayFields
}

func UnmarshalStructArrayFieldModel(fieldSchema *schemapb.StructArrayFieldSchema) *StructArrayField {
	if fieldSchema == nil {
		return nil
	}

	return &StructArrayField{
		FieldID:     fieldSchema.FieldID,
		Name:        fieldSchema.Name,
		Description: fieldSchema.Description,
		Fields:      UnmarshalFieldModels(fieldSchema.Fields),
	}
}

func UnmarshalStructArrayFieldModels(fieldSchemas []*schemapb.StructArrayFieldSchema) []*StructArrayField {
	if fieldSchemas == nil {
		return nil
	}

	structArrayFields := make([]*StructArrayField, len(fieldSchemas))
	for idx, StructArrayFieldSchema := range fieldSchemas {
		structArrayFields[idx] = UnmarshalStructArrayFieldModel(StructArrayFieldSchema)
	}
	return structArrayFields
}
