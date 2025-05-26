package typeutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

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
		schemapb.DataType_VarChar:
		return &FieldDataBuilder{
			dt:       dt,
			data:     make([]any, 0, capacity),
			valid:    make([]bool, 0, capacity),
			fillZero: fillZero,
		}, nil
	default:
		return nil, fmt.Errorf("not supported field type: %s", dt.String())
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
	if b.hasInvalid {
		field.ValidData = b.valid
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
	return field
}
