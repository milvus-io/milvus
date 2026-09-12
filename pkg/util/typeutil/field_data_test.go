package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestFieldDataValidData(t *testing.T) {
	legacy := []bool{true, false}
	scalarValid := []bool{false, true}
	vectorValid := []bool{true, true}

	t.Run("scalar field-specific", func(t *testing.T) {
		field := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{ValidData: scalarValid},
			},
		}
		assert.Equal(t, scalarValid, GetFieldDataValidData(field))
		assert.True(t, ValidateAndNormalizeFieldDataValidData(field))
	})

	t.Run("vector field-specific", func(t *testing.T) {
		field := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{ValidData: vectorValid},
			},
		}
		assert.Equal(t, vectorValid, GetFieldDataValidData(field))
		assert.True(t, ValidateAndNormalizeFieldDataValidData(field))
	})

	t.Run("legacy fallback", func(t *testing.T) {
		field := &schemapb.FieldData{
			ValidData: legacy,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{},
			},
		}
		assert.Equal(t, legacy, GetFieldDataValidData(field))
		assert.True(t, ValidateAndNormalizeFieldDataValidData(field))
	})

	t.Run("field-specific empty slice", func(t *testing.T) {
		field := &schemapb.FieldData{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{ValidData: []bool{}},
			},
		}
		assert.NotNil(t, GetFieldDataValidData(field))
		assert.Empty(t, GetFieldDataValidData(field))
		assert.True(t, ValidateAndNormalizeFieldDataValidData(field))
	})

	t.Run("matching dual sources", func(t *testing.T) {
		field := &schemapb.FieldData{
			ValidData: scalarValid,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{ValidData: scalarValid},
			},
		}
		assert.True(t, ValidateAndNormalizeFieldDataValidData(field))
		assert.Nil(t, field.GetValidData())
		assert.Equal(t, scalarValid, field.GetScalars().GetValidData())
	})

	t.Run("normalize legacy source", func(t *testing.T) {
		field := &schemapb.FieldData{
			ValidData: legacy,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{},
			},
		}
		assert.True(t, ValidateAndNormalizeFieldDataValidData(field))
		assert.Nil(t, field.GetValidData())
		assert.Equal(t, legacy, field.GetScalars().GetValidData())
	})

	t.Run("reject mismatched dual sources without normalization", func(t *testing.T) {
		field := &schemapb.FieldData{
			ValidData: legacy,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{ValidData: scalarValid},
			},
		}
		assert.False(t, ValidateAndNormalizeFieldDataValidData(field))
		assert.Equal(t, legacy, field.GetValidData())
		assert.Equal(t, scalarValid, field.GetScalars().GetValidData())
	})

	t.Run("reject nested mismatched dual sources without normalization", func(t *testing.T) {
		subField := &schemapb.FieldData{
			ValidData: legacy,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{ValidData: scalarValid},
			},
		}
		field := &schemapb.FieldData{
			Field: &schemapb.FieldData_StructArrays{
				StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{subField}},
			},
		}

		assert.False(t, ValidateAndNormalizeFieldDataValidData(field))
		assert.Equal(t, legacy, subField.GetValidData())
		assert.Equal(t, scalarValid, subField.GetScalars().GetValidData())
	})

	t.Run("project current source for legacy readers", func(t *testing.T) {
		subField := &schemapb.FieldData{
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{ValidData: vectorValid},
			},
		}
		field := &schemapb.FieldData{
			Field: &schemapb.FieldData_StructArrays{
				StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{subField}},
			},
		}

		ProjectFieldDataValidDataForLegacy(field)
		assert.Equal(t, vectorValid, subField.GetValidData())
		assert.Equal(t, vectorValid, subField.GetVectors().GetValidData())
	})

	t.Run("set scalar validity", func(t *testing.T) {
		field := &schemapb.FieldData{
			ValidData: legacy,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{},
			},
		}
		SetFieldDataValidData(field, scalarValid)
		assert.Nil(t, field.GetValidData())
		assert.Equal(t, scalarValid, field.GetScalars().GetValidData())
	})

	t.Run("set vector validity", func(t *testing.T) {
		field := &schemapb.FieldData{
			ValidData: legacy,
			Field: &schemapb.FieldData_Vectors{
				Vectors: &schemapb.VectorField{},
			},
		}
		SetFieldDataValidData(field, vectorValid)
		assert.Nil(t, field.GetValidData())
		assert.Equal(t, vectorValid, field.GetVectors().GetValidData())
	})

	t.Run("set preserves empty scalar validity", func(t *testing.T) {
		field := &schemapb.FieldData{
			Type: schemapb.DataType_Int64,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{},
			},
		}
		SetFieldDataValidData(field, []bool{})
		assert.Nil(t, field.GetValidData())
		assert.NotNil(t, field.GetScalars().ValidData)
		assert.Empty(t, field.GetScalars().GetValidData())
	})

	t.Run("set does not write struct validity", func(t *testing.T) {
		field := &schemapb.FieldData{
			Type:      schemapb.DataType_ArrayOfStruct,
			ValidData: legacy,
			Field: &schemapb.FieldData_StructArrays{
				StructArrays: &schemapb.StructArrayField{},
			},
		}
		SetFieldDataValidData(field, scalarValid)
		assert.Equal(t, legacy, field.GetValidData())
		assert.Equal(t, legacy, GetFieldDataValidData(field))
	})

	t.Run("set does not initialize missing scalar field", func(t *testing.T) {
		field := &schemapb.FieldData{
			ValidData: legacy,
			Field:     &schemapb.FieldData_Scalars{},
		}
		SetFieldDataValidData(field, scalarValid)
		assert.Equal(t, legacy, field.GetValidData())
		assert.Nil(t, field.GetScalars())
	})

	t.Run("set does not initialize missing vector field", func(t *testing.T) {
		field := &schemapb.FieldData{
			ValidData: legacy,
			Field:     &schemapb.FieldData_Vectors{},
		}
		SetFieldDataValidData(field, vectorValid)
		assert.Equal(t, legacy, field.GetValidData())
		assert.Nil(t, field.GetVectors())
	})
}

func TestNewFieldDataBuilder(t *testing.T) {
	tests := []struct {
		name     string
		dt       schemapb.DataType
		fillZero bool
		capacity int
		wantErr  bool
	}{
		{
			name:     "valid bool type",
			dt:       schemapb.DataType_Bool,
			fillZero: true,
			capacity: 10,
			wantErr:  false,
		},
		{
			name:     "valid int32 type",
			dt:       schemapb.DataType_Int32,
			fillZero: false,
			capacity: 5,
			wantErr:  false,
		},
		{
			name:     "valid varchar type",
			dt:       schemapb.DataType_VarChar,
			fillZero: true,
			capacity: 3,
			wantErr:  false,
		},
		{
			name:     "invalid type",
			dt:       schemapb.DataType_FloatVector,
			fillZero: true,
			capacity: 10,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, err := NewFieldDataBuilder(tt.dt, tt.fillZero, tt.capacity)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, builder)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, builder)
				assert.Equal(t, tt.dt, builder.dt)
				assert.Equal(t, tt.fillZero, builder.fillZero)
				assert.Equal(t, 0, len(builder.data))
				assert.Equal(t, 0, len(builder.valid))
			}
		})
	}
}

func TestFieldDataBuilder_Add(t *testing.T) {
	tests := []struct {
		name     string
		dt       schemapb.DataType
		fillZero bool
		inputs   []any
		want     *FieldDataBuilder
	}{
		{
			name:     "add bool values",
			dt:       schemapb.DataType_Bool,
			fillZero: true,
			inputs:   []any{true, nil, false},
			want: &FieldDataBuilder{
				dt:         schemapb.DataType_Bool,
				data:       []any{true, false},
				valid:      []bool{true, false, true},
				hasInvalid: true,
				fillZero:   true,
			},
		},
		{
			name:     "add int32 values",
			dt:       schemapb.DataType_Int32,
			fillZero: false,
			inputs:   []any{int32(1), int32(2), nil},
			want: &FieldDataBuilder{
				dt:         schemapb.DataType_Int32,
				data:       []any{int32(1), int32(2)},
				valid:      []bool{true, true, false},
				hasInvalid: true,
				fillZero:   false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, err := NewFieldDataBuilder(tt.dt, tt.fillZero, len(tt.inputs))
			assert.NoError(t, err)

			for _, input := range tt.inputs {
				builder = builder.Add(input)
			}

			assert.Equal(t, tt.want.dt, builder.dt)
			assert.Equal(t, tt.want.data, builder.data)
			assert.Equal(t, tt.want.valid, builder.valid)
			assert.Equal(t, tt.want.hasInvalid, builder.hasInvalid)
			assert.Equal(t, tt.want.fillZero, builder.fillZero)
		})
	}
}

func TestFieldDataBuilder_Build(t *testing.T) {
	tests := []struct {
		name     string
		dt       schemapb.DataType
		fillZero bool
		inputs   []any
		want     *schemapb.FieldData
	}{
		{
			name:     "build bool field with fillZero",
			dt:       schemapb.DataType_Bool,
			fillZero: true,
			inputs:   []any{true, nil, false},
			want: &schemapb.FieldData{
				Type: schemapb.DataType_Bool,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						ValidData: []bool{true, false, true},
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{
								Data: []bool{true, false, false},
							},
						},
					},
				},
			},
		},
		{
			name:     "build int32 field without fillZero",
			dt:       schemapb.DataType_Int32,
			fillZero: false,
			inputs:   []any{int32(1), int32(2), nil},
			want: &schemapb.FieldData{
				Type: schemapb.DataType_Int32,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						ValidData: []bool{true, true, false},
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{
								Data: []int32{1, 2},
							},
						},
					},
				},
			},
		},
		{
			name:     "build varchar field with fillZero",
			dt:       schemapb.DataType_VarChar,
			fillZero: true,
			inputs:   []any{"hello", nil, "world"},
			want: &schemapb.FieldData{
				Type: schemapb.DataType_VarChar,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						ValidData: []bool{true, false, true},
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{
								Data: []string{"hello", "", "world"},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, err := NewFieldDataBuilder(tt.dt, tt.fillZero, len(tt.inputs))
			assert.NoError(t, err)

			for _, input := range tt.inputs {
				builder = builder.Add(input)
			}

			got, err := builder.Build()
			assert.NoError(t, err)
			assert.Equal(t, tt.want.Type, got.Type)
			assert.Equal(t, GetFieldDataValidData(tt.want), got.GetScalars().GetValidData())
			assert.True(t, ValidateAndNormalizeFieldDataValidData(got))

			switch tt.dt {
			case schemapb.DataType_Bool:
				assert.Equal(t, tt.want.GetScalars().GetBoolData().GetData(), got.GetScalars().GetBoolData().GetData())
			case schemapb.DataType_Int32:
				assert.Equal(t, tt.want.GetScalars().GetIntData().GetData(), got.GetScalars().GetIntData().GetData())
			case schemapb.DataType_VarChar:
				assert.Equal(t, tt.want.GetScalars().GetStringData().GetData(), got.GetScalars().GetStringData().GetData())
			}
		})
	}
}
