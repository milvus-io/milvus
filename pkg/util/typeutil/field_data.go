package typeutil

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FieldData valid_data is handled in three stages:
//   - At an input boundary, ValidateAndNormalizeFieldDataValidData validates
//     that the legacy and field-specific values do not conflict, then keeps
//     only the field-specific representation.
//   - Internal code uses GetFieldDataValidData and SetFieldDataValidData to
//     process validity without depending on where it is physically stored.
//   - At a user-facing response boundary,
//     ProjectFieldDataValidDataForLegacy restores the legacy representation
//     while retaining the field-specific value for SDK compatibility.

// GetFieldDataValidData returns the validity of the immediate values carried by
// FieldData. New payloads store it on ScalarField or VectorField; FieldData is
// retained as a legacy fallback for older payloads.
func GetFieldDataValidData(fieldData *schemapb.FieldData) []bool {
	if legacy := fieldData.GetValidData(); len(legacy) > 0 {
		return legacy
	}
	return getFieldSpecificValidData(fieldData)
}

// GetArrayElementValidData returns element validity carried by one scalar
// Array row. Unlike GetFieldDataValidData, this bitmap is element-level.
func GetArrayElementValidData(row *schemapb.ScalarField) []bool {
	return row.GetValidData()
}

// GetVectorArrayElementValidData returns element validity carried by one
// ArrayOfVector row. Unlike GetFieldDataValidData, this bitmap is element-level.
func GetVectorArrayElementValidData(row *schemapb.VectorField) []bool {
	return row.GetValidData()
}

// SetVectorArrayElementValidData writes element validity carried by one
// ArrayOfVector row.
func SetVectorArrayElementValidData(row *schemapb.VectorField, validData []bool) {
	if row != nil {
		row.ValidData = validData
	}
}

// SetFieldDataValidData writes validity to the current field-specific location
// and clears the legacy FieldData.valid_data source.
func SetFieldDataValidData(fieldData *schemapb.FieldData, validData []bool) {
	if fieldData == nil {
		return
	}

	if scalars := fieldData.GetScalars(); scalars != nil {
		scalars.ValidData = validData
	} else if vectors := fieldData.GetVectors(); vectors != nil {
		vectors.ValidData = validData
	} else {
		return
	}

	fieldData.ValidData = nil
}

// ValidateAndNormalizeFieldDataValidData checks the legacy and current
// validity locations once at an input boundary. Matching values are accepted
// and normalized to the current field-specific location. It returns false if
// any immediate or nested FieldData carries different values in both places.
func ValidateAndNormalizeFieldDataValidData(fieldData *schemapb.FieldData) bool {
	if !fieldDataValidDataConsistent(fieldData) {
		return false
	}
	normalizeFieldDataValidData(fieldData)
	return true
}

func fieldDataValidDataConsistent(fieldData *schemapb.FieldData) bool {
	if fieldData == nil {
		return true
	}

	legacy := fieldData.GetValidData()
	current := getFieldSpecificValidData(fieldData)
	if len(legacy) > 0 && len(current) > 0 && !slices.Equal(legacy, current) {
		return false
	}

	for _, subField := range fieldData.GetStructArrays().GetFields() {
		if !fieldDataValidDataConsistent(subField) {
			return false
		}
	}
	return true
}

func normalizeFieldDataValidData(fieldData *schemapb.FieldData) {
	if fieldData == nil {
		return
	}

	switch fieldData.Field.(type) {
	case *schemapb.FieldData_Scalars, *schemapb.FieldData_Vectors:
		if validData := GetFieldDataValidData(fieldData); len(validData) > 0 {
			SetFieldDataValidData(fieldData, validData)
		} else {
			fieldData.ValidData = nil
		}
	case *schemapb.FieldData_StructArrays:
		fieldData.ValidData = nil
		for _, subField := range fieldData.GetStructArrays().GetFields() {
			normalizeFieldDataValidData(subField)
		}
	default:
		fieldData.ValidData = nil
	}
}

// ProjectFieldDataValidDataForLegacy copies current top-level validity to the
// legacy location without clearing the current value. This is intended only
// for user-facing response boundaries so old SDKs can read the validity mask.
func ProjectFieldDataValidDataForLegacy(fieldData *schemapb.FieldData) {
	if fieldData == nil {
		return
	}
	if validData := getFieldSpecificValidData(fieldData); len(validData) > 0 {
		fieldData.ValidData = slices.Clone(validData)
	}
	for _, subField := range fieldData.GetStructArrays().GetFields() {
		ProjectFieldDataValidDataForLegacy(subField)
	}
}

func getFieldSpecificValidData(fieldData *schemapb.FieldData) []bool {
	if scalars := fieldData.GetScalars(); scalars != nil {
		return scalars.GetValidData()
	}
	return fieldData.GetVectors().GetValidData()
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
