package typeutil

import "github.com/milvus-io/milvus-proto/go-api/schemapb"

func GetRowCount(fieldData *schemapb.FieldData) int {
	switch field := fieldData.Field.(type) {
	case *schemapb.FieldData_Scalars:
		switch scalars := field.Scalars.Data.(type) {
		case *schemapb.ScalarField_BoolData:
			return len(scalars.BoolData.GetData())
		case *schemapb.ScalarField_IntData:
			return len(scalars.IntData.GetData())
		case *schemapb.ScalarField_LongData:
			return len(scalars.LongData.GetData())
		case *schemapb.ScalarField_FloatData:
			return len(scalars.FloatData.GetData())
		case *schemapb.ScalarField_DoubleData:
			return len(scalars.DoubleData.GetData())
		case *schemapb.ScalarField_StringData:
			return len(scalars.StringData.GetData())
		case *schemapb.ScalarField_BytesData:
			return len(scalars.BytesData.GetData())
		}
	case *schemapb.FieldData_Vectors:
		switch vectors := field.Vectors.Data.(type) {
		case *schemapb.VectorField_FloatVector:
			return int(int64(len(vectors.FloatVector.GetData())) / field.Vectors.GetDim())
		case *schemapb.VectorField_BinaryVector:
			return int(int64(len(vectors.BinaryVector)*8) / field.Vectors.GetDim())
		}
	}
	return 0
}
