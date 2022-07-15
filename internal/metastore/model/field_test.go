package model

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
)

var (
	filedSchemaPb = &schemapb.FieldSchema{
		FieldID:      fieldID,
		Name:         fieldName,
		IsPrimaryKey: false,
		Description:  "none",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   typeParams,
		IndexParams:  indexParams,
		AutoID:       false,
	}

	fieldModel = &Field{
		FieldID:      fieldID,
		Name:         fieldName,
		IsPrimaryKey: false,
		Description:  "none",
		AutoID:       false,
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   typeParams,
		IndexParams:  indexParams,
	}
)

func TestMarshalFieldModel(t *testing.T) {
	ret := MarshalFieldModel(fieldModel)
	assert.Equal(t, filedSchemaPb, ret)
	assert.Nil(t, MarshalFieldModel(nil))
}

func TestMarshalFieldModels(t *testing.T) {
	ret := MarshalFieldModels([]*Field{fieldModel})
	assert.Equal(t, []*schemapb.FieldSchema{filedSchemaPb}, ret)
	assert.Nil(t, MarshalFieldModels(nil))
}

func TestUnmarshalFieldModel(t *testing.T) {
	ret := UnmarshalFieldModel(filedSchemaPb)
	assert.Equal(t, fieldModel, ret)
	assert.Nil(t, UnmarshalFieldModel(nil))
}

func TestUnmarshalFieldModels(t *testing.T) {
	ret := UnmarshalFieldModels([]*schemapb.FieldSchema{filedSchemaPb})
	assert.Equal(t, []*Field{fieldModel}, ret)
	assert.Nil(t, UnmarshalFieldModels(nil))
}
