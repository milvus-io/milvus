package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

var (
	subVectorArrayField = &schemapb.FieldSchema{
		FieldID:      fieldID,
		Name:         fieldName,
		IsPrimaryKey: false,
		Description:  "none",
		DataType:     schemapb.DataType_ArrayOfVector,
		ElementType:  schemapb.DataType_FloatVector,
		TypeParams:   typeParams,
		IndexParams:  indexParams,
		AutoID:       false,
	}

	subVectorArrayFieldModel = &Field{
		FieldID:      fieldID,
		Name:         fieldName,
		IsPrimaryKey: false,
		Description:  "none",
		DataType:     schemapb.DataType_ArrayOfVector,
		ElementType:  schemapb.DataType_FloatVector,
		TypeParams:   typeParams,
		IndexParams:  indexParams,
		AutoID:       false,
	}

	subVarcharArrayField = &schemapb.FieldSchema{
		FieldID:      fieldID,
		Name:         fieldName,
		IsPrimaryKey: false,
		Description:  "none",
		AutoID:       false,
		DataType:     schemapb.DataType_Array,
		ElementType:  schemapb.DataType_VarChar,
		TypeParams:   typeParams,
		IndexParams:  indexParams,
	}

	subVarcharArrayFieldModel = &Field{
		FieldID:      fieldID,
		Name:         fieldName,
		IsPrimaryKey: false,
		Description:  "none",
		AutoID:       false,
		DataType:     schemapb.DataType_Array,
		ElementType:  schemapb.DataType_VarChar,
		TypeParams:   typeParams,
		IndexParams:  indexParams,
	}

	structFieldPb = &schemapb.StructArrayFieldSchema{
		FieldID:     fieldID,
		Name:        fieldName,
		Description: "none",
		Fields:      []*schemapb.FieldSchema{subVarcharArrayField, subVectorArrayField},
	}

	structFieldModel = &StructArrayField{
		FieldID:     fieldID,
		Name:        fieldName,
		Description: "none",
		Fields:      []*Field{subVarcharArrayFieldModel, subVectorArrayFieldModel},
	}
)

func TestMarshalStructArrayFieldModel(t *testing.T) {
	ret := MarshalStructArrayFieldModel(structFieldModel)
	assert.Equal(t, structFieldPb, ret)
	assert.Nil(t, MarshalStructArrayFieldModel(nil))
}

func TestMarshalStructArrayFieldModels(t *testing.T) {
	ret := MarshalStructArrayFieldModels([]*StructArrayField{structFieldModel})
	assert.Equal(t, []*schemapb.StructArrayFieldSchema{structFieldPb}, ret)
	assert.Nil(t, MarshalStructArrayFieldModels(nil))
}

func TestUnmarshalStructArrayFieldModel(t *testing.T) {
	ret := UnmarshalStructArrayFieldModel(structFieldPb)
	assert.Equal(t, structFieldModel, ret)
	assert.Nil(t, UnmarshalStructArrayFieldModel(nil))
}

func TestUnmarshalStructArrayFieldModels(t *testing.T) {
	ret := UnmarshalStructArrayFieldModels([]*schemapb.StructArrayFieldSchema{structFieldPb})
	assert.Equal(t, []*StructArrayField{structFieldModel}, ret)
	assert.Nil(t, UnmarshalStructArrayFieldModels(nil))
}

func TestStructArrayField_Equal(t *testing.T) {
	field1 := &Field{
		FieldID:     1,
		Name:        "field1",
		DataType:    schemapb.DataType_VarChar,
		Description: "test field 1",
	}

	field2 := &Field{
		FieldID:     2,
		Name:        "field2",
		DataType:    schemapb.DataType_Int64,
		Description: "test field 2",
	}

	type args struct {
		structArrayFieldA StructArrayField
		structArrayFieldB StructArrayField
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "equal fields",
			args: args{
				structArrayFieldA: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1, field2},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1, field2},
				},
			},
			want: true,
		},
		{
			name: "different FieldID",
			args: args{
				structArrayFieldA: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     2,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1},
				},
			},
			want: false,
		},
		{
			name: "different Name",
			args: args{
				structArrayFieldA: StructArrayField{
					FieldID:     1,
					Name:        "struct_field_1",
					Description: "test struct field",
					Fields:      []*Field{field1},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field_2",
					Description: "test struct field",
					Fields:      []*Field{field1},
				},
			},
			want: false,
		},
		{
			name: "different Description",
			args: args{
				structArrayFieldA: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field 1",
					Fields:      []*Field{field1},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field 2",
					Fields:      []*Field{field1},
				},
			},
			want: false,
		},
		{
			name: "different Fields",
			args: args{
				structArrayFieldA: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field2},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.args.structArrayFieldA.Equal(tt.args.structArrayFieldB); got != tt.want {
				t.Errorf("StructArrayField.Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}
