package typeutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func genEmptyBoolFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: nil}},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}
}

func genEmptyIntFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: nil}},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}
}

func genEmptyLongFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: nil}},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}
}

func genEmptyFloatFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: nil}},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}
}

func genEmptyDoubleFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: nil}},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}
}

func genEmptyVarCharFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: nil}},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}
}

func genEmptyArrayFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_ArrayData{
					ArrayData: &schemapb.ArrayArray{
						Data:        nil,
						ElementType: field.GetElementType(),
					},
				},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}
}

func genEmptyJSONFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: nil}},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}
}

func genEmptyBinaryVectorFieldData(field *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	dim, err := GetDim(field)
	if err != nil {
		return nil, err
	}
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: nil,
				},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}, nil
}

func genEmptyFloatVectorFieldData(field *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	dim, err := GetDim(field)
	if err != nil {
		return nil, err
	}
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{Data: nil},
				},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}, nil
}

func genEmptyFloat16VectorFieldData(field *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	dim, err := GetDim(field)
	if err != nil {
		return nil, err
	}
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: nil,
				},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}, nil
}

func genEmptyBFloat16VectorFieldData(field *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	dim, err := GetDim(field)
	if err != nil {
		return nil, err
	}
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_Bfloat16Vector{
					Bfloat16Vector: nil,
				},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}, nil
}

func genEmptySparseFloatVectorFieldData(field *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 0,
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Dim:      0,
						Contents: make([][]byte, 0),
					},
				},
			},
		},
		FieldId:   field.GetFieldID(),
		IsDynamic: field.GetIsDynamic(),
	}, nil
}

func GenEmptyFieldData(field *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	dataType := field.GetDataType()
	switch dataType {
	case schemapb.DataType_Bool:
		return genEmptyBoolFieldData(field), nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		return genEmptyIntFieldData(field), nil
	case schemapb.DataType_Int64:
		return genEmptyLongFieldData(field), nil
	case schemapb.DataType_Float:
		return genEmptyFloatFieldData(field), nil
	case schemapb.DataType_Double:
		return genEmptyDoubleFieldData(field), nil
	case schemapb.DataType_VarChar:
		return genEmptyVarCharFieldData(field), nil
	case schemapb.DataType_Array:
		return genEmptyArrayFieldData(field), nil
	case schemapb.DataType_JSON:
		return genEmptyJSONFieldData(field), nil
	case schemapb.DataType_BinaryVector:
		return genEmptyBinaryVectorFieldData(field)
	case schemapb.DataType_FloatVector:
		return genEmptyFloatVectorFieldData(field)
	case schemapb.DataType_Float16Vector:
		return genEmptyFloat16VectorFieldData(field)
	case schemapb.DataType_BFloat16Vector:
		return genEmptyBFloat16VectorFieldData(field)
	case schemapb.DataType_SparseFloatVector:
		return genEmptySparseFloatVectorFieldData(field)
	default:
		return nil, fmt.Errorf("unsupported data type: %s", dataType.String())
	}
}
