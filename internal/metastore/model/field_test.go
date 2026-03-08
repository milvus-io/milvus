package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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

func TestCheckFieldsEqual(t *testing.T) {
	type args struct {
		fieldsA []*Field
		fieldsB []*Field
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			// length not match.
			args: args{
				fieldsA: []*Field{{FieldID: 1, Name: "f1"}},
				fieldsB: []*Field{},
			},
			want: false,
		},
		{
			args: args{
				fieldsA: []*Field{{FieldID: 1, Name: "f1"}},
				fieldsB: []*Field{{FieldID: 2, Name: "f2"}},
			},
			want: false,
		},
		{
			args: args{
				fieldsA: []*Field{{FieldID: 1, Name: "f1"}, {FieldID: 2, Name: "f2"}},
				fieldsB: []*Field{{FieldID: 1, Name: "f1"}, {FieldID: 2, Name: "f2"}},
			},
			want: true,
		},
		{
			args: args{
				fieldsA: []*Field{{FieldID: 1, Name: "f1", TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				}}},
				fieldsB: []*Field{{FieldID: 1, Name: "f1", TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "256"},
				}}},
			},
			want: false,
		},
		{
			args: args{
				fieldsA: []*Field{{FieldID: 1, Name: "f1", TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "65536"},
				}}},
				fieldsB: []*Field{{FieldID: 1, Name: "f1", TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "32768"},
				}}},
			},
			want: false,
		},
		{
			args: args{
				fieldsA: []*Field{{FieldID: 1, Name: "f1"}, {FieldID: 2, Name: "f2"}},
				fieldsB: []*Field{{FieldID: 2, Name: "f2"}, {FieldID: 1, Name: "f1"}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckFieldsEqual(tt.args.fieldsA, tt.args.fieldsB); got != tt.want {
				t.Errorf("CheckFieldsEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestField_Available(t *testing.T) {
	type fields struct {
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
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{fields: fields{State: schemapb.FieldState_FieldCreated}, want: true},
		{fields: fields{State: schemapb.FieldState_FieldCreating}, want: false},
		{fields: fields{State: schemapb.FieldState_FieldDropping}, want: false},
		{fields: fields{State: schemapb.FieldState_FieldDropped}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := Field{
				FieldID:      tt.fields.FieldID,
				Name:         tt.fields.Name,
				IsPrimaryKey: tt.fields.IsPrimaryKey,
				Description:  tt.fields.Description,
				DataType:     tt.fields.DataType,
				TypeParams:   tt.fields.TypeParams,
				IndexParams:  tt.fields.IndexParams,
				AutoID:       tt.fields.AutoID,
				State:        tt.fields.State,
			}
			assert.Equalf(t, tt.want, f.Available(), "Available()")
		})
	}
}
