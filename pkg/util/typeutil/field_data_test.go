package typeutil

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

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
				Type:      schemapb.DataType_Bool,
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
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
				Type:      schemapb.DataType_Int32,
				ValidData: []bool{true, true, false},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
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
				Type:      schemapb.DataType_VarChar,
				ValidData: []bool{true, false, true},
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
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

			got := builder.Build()
			assert.Equal(t, tt.want.Type, got.Type)
			assert.Equal(t, tt.want.ValidData, got.ValidData)

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
