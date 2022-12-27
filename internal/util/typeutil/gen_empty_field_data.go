package typeutil

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

func fieldDataEmpty(data *schemapb.FieldData) bool {
	if data == nil {
		return true
	}
	switch realData := data.Field.(type) {
	case *schemapb.FieldData_Scalars:
		switch realScalars := realData.Scalars.Data.(type) {
		case *schemapb.ScalarField_BoolData:
			return len(realScalars.BoolData.GetData()) <= 0
		case *schemapb.ScalarField_LongData:
			return len(realScalars.LongData.GetData()) <= 0
		case *schemapb.ScalarField_FloatData:
			return len(realScalars.FloatData.GetData()) <= 0
		case *schemapb.ScalarField_DoubleData:
			return len(realScalars.DoubleData.GetData()) <= 0
		case *schemapb.ScalarField_StringData:
			return len(realScalars.StringData.GetData()) <= 0
		}
	case *schemapb.FieldData_Vectors:
		switch realVectors := realData.Vectors.Data.(type) {
		case *schemapb.VectorField_BinaryVector:
			return len(realVectors.BinaryVector) <= 0
		case *schemapb.VectorField_FloatVector:
			return len(realVectors.FloatVector.Data) <= 0
		}
	}
	return true
}

func genEmptyBoolFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: nil}},
			},
		},
		FieldId: field.GetFieldID(),
	}
}

func genEmptyIntFieldData(field *schemapb.FieldSchema) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: nil}},
			},
		},
		FieldId: field.GetFieldID(),
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
		FieldId: field.GetFieldID(),
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
		FieldId: field.GetFieldID(),
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
		FieldId: field.GetFieldID(),
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
		FieldId: field.GetFieldID(),
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
		FieldId: field.GetFieldID(),
	}, nil
}

func GenEmptyFieldData(field *schemapb.FieldSchema) (*schemapb.FieldData, error) {
	dataType := field.GetDataType()
	switch dataType {
	case schemapb.DataType_Bool:
		return genEmptyBoolFieldData(field), nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32, schemapb.DataType_Int64:
		return genEmptyIntFieldData(field), nil
	case schemapb.DataType_Float:
		return genEmptyFloatFieldData(field), nil
	case schemapb.DataType_Double:
		return genEmptyDoubleFieldData(field), nil
	case schemapb.DataType_VarChar:
		return genEmptyVarCharFieldData(field), nil
	case schemapb.DataType_BinaryVector:
		return genEmptyBinaryVectorFieldData(field)
	case schemapb.DataType_FloatVector:
		return genEmptyFloatVectorFieldData(field)
	default:
		return nil, fmt.Errorf("unsupported data type: %s", dataType.String())
	}
}
