package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

var (
	fieldTypeSchema = &schemapb.TypeSchema{
		Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_FloatVector},
	}

	filedSchemaPb = &schemapb.FieldSchema{
		FieldID:      fieldID,
		Name:         fieldName,
		IsPrimaryKey: false,
		Description:  "none",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   typeParams,
		IndexParams:  indexParams,
		AutoID:       false,
		TypeSchema:   fieldTypeSchema,
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
		TypeSchema:   fieldTypeSchema,
	}
)

func TestCloneFields(t *testing.T) {
	ret := CloneFields([]*Field{fieldModel})
	assert.Len(t, ret, 1)
	assert.True(t, proto.Equal(filedSchemaPb, ret[0]))
	assert.NotSame(t, fieldModel, ret[0])
	assert.Nil(t, CloneFields(nil))
	assert.Empty(t, CloneFields([]*Field{}))
	assert.NotNil(t, CloneFields([]*Field{}))
}

func fieldConversionCases() []struct {
	name string
	pb   *schemapb.FieldSchema
} {
	return []struct {
		name string
		pb   *schemapb.FieldSchema
	}{
		{
			name: "plain scalar",
			pb: &schemapb.FieldSchema{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       true,
				TypeParams:   []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
			},
		},
		{
			name: "vector with type params",
			pb: &schemapb.FieldSchema{
				FieldID:     101,
				Name:        "vec",
				DataType:    schemapb.DataType_FloatVector,
				TypeParams:  []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}},
				IndexParams: []*commonpb.KeyValuePair{{Key: "metric_type", Value: "L2"}},
			},
		},
		{
			name: "default value",
			pb: &schemapb.FieldSchema{
				FieldID:    102,
				Name:       "age",
				DataType:   schemapb.DataType_Int32,
				TypeParams: []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{IntData: 18},
				},
				Nullable: true,
			},
		},
		{
			name: "struct sub field with type schema",
			pb: &schemapb.FieldSchema{
				FieldID:         103,
				Name:            "items[vec]",
				DataType:        schemapb.DataType_ArrayOfVector,
				ElementType:     schemapb.DataType_FloatVector,
				ElementNullable: true,
				TypeParams:      []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}},
				TypeSchema: &schemapb.TypeSchema{
					Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_FloatVector},
				},
			},
		},
		{
			name: "function output",
			pb: &schemapb.FieldSchema{
				FieldID:          104,
				Name:             "sparse",
				DataType:         schemapb.DataType_SparseFloatVector,
				IsFunctionOutput: true,
				TypeParams:       []*commonpb.KeyValuePair{{Key: "k", Value: "v"}},
				ExternalField:    "ext",
			},
		},
		{
			name: "dropping state",
			pb: &schemapb.FieldSchema{
				FieldID:    105,
				Name:       "old",
				DataType:   schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}},
				State:      schemapb.FieldState_FieldDropping,
			},
		},
	}
}

func TestCloneField_KeepsAllFields(t *testing.T) {
	for _, tc := range fieldConversionCases() {
		t.Run(tc.name, func(t *testing.T) {
			got := CloneField(tc.pb)
			assert.True(t, proto.Equal(tc.pb, got), "got %v, want %v", got, tc.pb)
			assert.Equal(t, tc.pb.GetState(), got.GetState())
		})
	}
}

func TestCloneField_Independent(t *testing.T) {
	for _, tc := range fieldConversionCases() {
		t.Run(tc.name, func(t *testing.T) {
			want := proto.Clone(tc.pb).(*schemapb.FieldSchema)
			model := CloneField(tc.pb)

			tc.pb.TypeParams[0].Value = "mutated"
			// Mutate the contents in place: replacing the pointer would
			// not catch a shared DefaultValue.
			if dv := tc.pb.GetDefaultValue(); dv != nil {
				dv.Data = &schemapb.ValueField_StringData{StringData: "mutated"}
			}
			tc.pb.Nullable = !tc.pb.Nullable

			assert.True(t, proto.Equal(want, model), "got %v, want %v", model, want)
		})
	}
}

func TestFieldEqual(t *testing.T) {
	base := func() *Field {
		return &Field{
			FieldID:         100,
			Name:            "array",
			DataType:        schemapb.DataType_Array,
			ElementType:     schemapb.DataType_Int64,
			ElementNullable: true,
			TypeParams:      []*commonpb.KeyValuePair{{Key: "max_capacity", Value: "8"}},
			DefaultValue: &schemapb.ValueField{
				Data: &schemapb.ValueField_IntData{IntData: 1},
			},
			TypeSchema: fieldTypeSchema,
		}
	}
	tests := []struct {
		name   string
		mutate func(f *Field)
		want   bool
	}{
		{name: "identical", mutate: func(f *Field) {}, want: true},
		{name: "state ignored", mutate: func(f *Field) { f.State = schemapb.FieldState_FieldDropping }, want: true},
		{name: "element nullable", mutate: func(f *Field) { f.ElementNullable = false }, want: false},
		{name: "name", mutate: func(f *Field) { f.Name = "other" }, want: false},
		{name: "type params", mutate: func(f *Field) { f.TypeParams[0].Value = "16" }, want: false},
		{name: "default value", mutate: func(f *Field) { f.DefaultValue = nil }, want: false},
		{name: "type schema", mutate: func(f *Field) {
			f.TypeSchema = &schemapb.TypeSchema{Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}}
		}, want: false},
		{name: "function output", mutate: func(f *Field) { f.IsFunctionOutput = true }, want: false},
		{name: "nullable", mutate: func(f *Field) { f.Nullable = true }, want: false},
		{name: "external field", mutate: func(f *Field) { f.ExternalField = "ext" }, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := base()
			b := CloneField(a)
			tt.mutate(b)
			assert.Equal(t, tt.want, FieldEqual(a, b))
			assert.Equal(t, tt.want, FieldEqual(b, a))
		})
	}
}

func TestCloneField(t *testing.T) {
	assert.Nil(t, CloneField(nil))
	cloned := CloneField(fieldModel)
	assert.True(t, proto.Equal(fieldModel, cloned))
	assert.NotSame(t, fieldModel.TypeSchema, cloned.TypeSchema)
	cloned.TypeSchema.Kind = &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int64}
	cloned.TypeParams[0].Value = "mutated"
	assert.Equal(t, schemapb.DataType_FloatVector, fieldModel.TypeSchema.GetLeafType())
	assert.NotEqual(t, "mutated", fieldModel.TypeParams[0].Value)
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
