package typeutil

import (
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Row validity of a FieldData has two wire locations: the legacy
// FieldData.valid_data and the field-specific ScalarField.valid_data or
// VectorField.valid_data. Components built before the field-specific location
// existed read and update only the legacy location, and forward the
// field-specific one untouched as an unknown field. While such components can
// still share a cluster, WAL, or replication stream with this version:
//   - Writers store the same mask in both locations (SetFieldDataValidData), so
//     older readers keep seeing row validity.
//   - Readers treat the legacy location as authoritative whenever it is set
//     (GetFieldDataValidData). Every version writes and updates it, while an
//     older component may rewrite it and leave a stale field-specific copy.
//   - A user payload entering the proxy is rejected if the two locations
//     disagree (ValidateAndNormalizeFieldDataValidData). Payloads produced
//     inside Milvus, such as WAL messages written by an older proxy, are
//     reconciled to the legacy value instead (NormalizeFieldDataValidData).

// GetFieldDataValidData returns the row validity of the immediate values
// carried by FieldData. The legacy location wins when it is set; see the
// comment at the top of this file.
func GetFieldDataValidData(fieldData *schemapb.FieldData) []bool {
	if legacy := fieldData.GetValidData(); len(legacy) > 0 {
		return legacy
	}
	return getFieldSpecificValidData(fieldData)
}

// SetFieldDataValidData writes row validity to the legacy location, and to the
// field-specific location when the ScalarField or VectorField wrapper exists.
// Both locations share validData.
func SetFieldDataValidData(fieldData *schemapb.FieldData, validData []bool) {
	if fieldData == nil {
		return
	}

	fieldData.ValidData = validData
	if scalars := fieldData.GetScalars(); scalars != nil {
		scalars.ValidData = validData
	} else if vectors := fieldData.GetVectors(); vectors != nil {
		vectors.ValidData = validData
	}
}

// ValidateAndNormalizeFieldDataValidData checks a user payload once when it
// enters the proxy. It returns false if any immediate or nested FieldData
// carries different values in the two locations; otherwise it writes the
// validity to both locations.
func ValidateAndNormalizeFieldDataValidData(fieldData *schemapb.FieldData) bool {
	if !fieldDataValidDataConsistent(fieldData) {
		return false
	}
	normalizeFieldDataValidData(fieldData)
	return true
}

// NormalizeFieldDataValidData reconciles a payload produced inside Milvus. When
// the two locations disagree, the legacy value wins, because an older component
// may have rewritten it without updating the field-specific copy. The result is
// written to both locations.
func NormalizeFieldDataValidData(fieldData *schemapb.FieldData) {
	normalizeFieldDataValidData(fieldData)
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

	// Older SDKs omit the sparse payload for an all-null batch. Materialize
	// its empty container before moving legacy validity into VectorField,
	// otherwise the default branch below discards the only null information.
	// Keep the bitmap unchanged: row alignment still rejects missing vectors
	// when any bit is true.
	if fieldData.Field == nil && fieldData.GetType() == schemapb.DataType_SparseFloatVector && len(fieldData.GetValidData()) > 0 {
		fieldData.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{},
				},
			},
		}
	}

	switch fieldData.Field.(type) {
	case *schemapb.FieldData_Scalars, *schemapb.FieldData_Vectors:
		SetFieldDataValidData(fieldData, GetFieldDataValidData(fieldData))
	case *schemapb.FieldData_StructArrays:
		fieldData.ValidData = nil
		for _, subField := range fieldData.GetStructArrays().GetFields() {
			normalizeFieldDataValidData(subField)
		}
	default:
		fieldData.ValidData = nil
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
