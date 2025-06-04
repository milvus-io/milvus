package model

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
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
	function1 := &Function{
		Name:             "func1",
		ID:               1,
		Description:      "test function 1",
		Type:             schemapb.FunctionType_BM25,
		InputFieldIDs:    []int64{1, 2},
		InputFieldNames:  []string{"input1", "input2"},
		OutputFieldIDs:   []int64{3},
		OutputFieldNames: []string{"output1"},
		Params: []*commonpb.KeyValuePair{
			{Key: "param1", Value: "value1"},
		},
	}

	function2 := &Function{
		Name:             "func2",
		ID:               2,
		Description:      "test function 2",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldIDs:    []int64{4, 5},
		InputFieldNames:  []string{"input3", "input4"},
		OutputFieldIDs:   []int64{6},
		OutputFieldNames: []string{"output2"},
		Params: []*commonpb.KeyValuePair{
			{Key: "param2", Value: "value2"},
		},
	}

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
					Functions:   []*Function{function1},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1, field2},
					Functions:   []*Function{function1},
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
					Functions:   []*Function{},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     2,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1},
					Functions:   []*Function{},
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
					Functions:   []*Function{},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field_2",
					Description: "test struct field",
					Fields:      []*Field{field1},
					Functions:   []*Function{},
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
					Functions:   []*Function{},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field 2",
					Fields:      []*Field{field1},
					Functions:   []*Function{},
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
					Functions:   []*Function{},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field2},
					Functions:   []*Function{},
				},
			},
			want: false,
		},
		{
			name: "different Functions",
			args: args{
				structArrayFieldA: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1},
					Functions:   []*Function{function1},
				},
				structArrayFieldB: StructArrayField{
					FieldID:     1,
					Name:        "struct_field",
					Description: "test struct field",
					Fields:      []*Field{field1},
					Functions:   []*Function{function2},
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
