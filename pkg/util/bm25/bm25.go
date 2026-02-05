package bm25

import "github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

func BuildSparseFieldData(field *schemapb.FieldSchema, sparseArray *schemapb.SparseFloatArray) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:      field.GetDataType(),
		FieldName: field.GetName(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: sparseArray.GetDim(),
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: sparseArray,
				},
			},
		},
		FieldId: field.GetFieldID(),
	}
}
