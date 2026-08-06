package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetFieldDataValidData returns the validity of the immediate values carried by
// FieldData. New payloads store it on ScalarField or VectorField; FieldData is
// retained as a legacy fallback for older payloads.
func GetFieldDataValidData(fieldData *schemapb.FieldData) []bool {
	if validData := fieldData.GetValidData(); len(validData) > 0 {
		return validData
	}
	if scalars := fieldData.GetScalars(); scalars != nil {
		return scalars.GetValidData()
	}
	return fieldData.GetVectors().GetValidData()
}

// SetFieldDataValidData writes validity to the current field-specific location
// and clears the legacy FieldData.valid_data source.
func SetFieldDataValidData(fieldData *schemapb.FieldData, validData []bool) {
	if fieldData == nil {
		return
	}
	fieldData.ValidData = nil
	switch field := fieldData.Field.(type) {
	case *schemapb.FieldData_Scalars:
		if field.Scalars == nil {
			field.Scalars = &schemapb.ScalarField{}
		}
		field.Scalars.ValidData = validData
	case *schemapb.FieldData_Vectors:
		if field.Vectors == nil {
			field.Vectors = &schemapb.VectorField{}
		}
		field.Vectors.ValidData = validData
	}
}

// HasFieldDataValidDataConflict reports whether both the legacy and current
// validity locations are populated for the same FieldData.
func HasFieldDataValidDataConflict(fieldData *schemapb.FieldData) bool {
	if fieldData == nil || len(fieldData.GetValidData()) == 0 {
		return false
	}
	return len(fieldData.GetScalars().GetValidData()) > 0 ||
		len(fieldData.GetVectors().GetValidData()) > 0
}

type FieldDataBuilder struct {
	dt         schemapb.DataType
	data       []any
	valid      []bool
	hasInvalid bool

	fillZero bool // if true, fill zero value in returned field data for invalid rows
}

func NewFieldDataBuilder(dt schemapb.DataType, fillZero bool, capacity int) (*FieldDataBuilder, error) {
	switch dt {
	case schemapb.DataType_Bool,
		schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64,
		// DataType_String is deprecated; string scalar fields should arrive as VarChar.
		schemapb.DataType_Timestamptz, schemapb.DataType_VarChar:
		return &FieldDataBuilder{
			dt:       dt,
			data:     make([]any, 0, capacity),
			valid:    make([]bool, 0, capacity),
			fillZero: fillZero,
		}, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("not supported field type: %s", dt.String())
	}
}

func (b *FieldDataBuilder) Add(data any) *FieldDataBuilder {
	if data == nil {
		b.hasInvalid = true
		b.valid = append(b.valid, false)
	} else {
		b.data = append(b.data, data)
		b.valid = append(b.valid, true)
	}
	return b
}

func (b *FieldDataBuilder) Build() *schemapb.FieldData {
	field := &schemapb.FieldData{
		Type: b.dt,
	}

	switch b.dt {
	case schemapb.DataType_Bool:
		val := make([]bool, 0, len(b.valid))
		validIdx := 0
		for _, v := range b.valid {
			if v {
				val = append(val, b.data[validIdx].(bool))
				validIdx++
			} else if b.fillZero {
				val = append(val, false)
			}
		}
		field.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: val,
					},
				},
			},
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		val := make([]int32, 0, len(b.valid))
		validIdx := 0
		for _, v := range b.valid {
			if v {
				val = append(val, b.data[validIdx].(int32))
				validIdx++
			} else if b.fillZero {
				val = append(val, 0)
			}
		}
		field.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: val,
					},
				},
			},
		}
	case schemapb.DataType_Int64:
		val := make([]int64, 0, len(b.valid))
		validIdx := 0
		for _, v := range b.valid {
			if v {
				val = append(val, b.data[validIdx].(int64))
				validIdx++
			} else if b.fillZero {
				val = append(val, 0)
			}
		}
		field.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: val,
					},
				},
			},
		}
	case schemapb.DataType_Timestamptz:
		val := make([]int64, 0, len(b.valid))
		validIdx := 0
		for _, v := range b.valid {
			if v {
				val = append(val, b.data[validIdx].(int64))
				validIdx++
			} else if b.fillZero {
				val = append(val, 0)
			}
		}
		field.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_TimestamptzData{
					TimestamptzData: &schemapb.TimestamptzArray{
						Data: val,
					},
				},
			},
		}
	case schemapb.DataType_VarChar:
		val := make([]string, 0, len(b.valid))
		validIdx := 0
		for _, v := range b.valid {
			if v {
				val = append(val, b.data[validIdx].(string))
				validIdx++
			} else if b.fillZero {
				val = append(val, "")
			}
		}
		field.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: val,
					},
				},
			},
		}
	default:
		return nil
	}
	if b.hasInvalid {
		SetFieldDataValidData(field, b.valid)
	}
	return field
}
