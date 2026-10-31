package tasks

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/distance"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// reservedFieldID is the FieldId used for the synthetic distance column appended
// to retrieve results. It is chosen to never collide with schema field ids.
const reservedFieldID int64 = -10000

// calcDistanceColumn computes per-row distance between the reference vector and
// the target vector field of a reduced retrieve result, appends it as a
// synthetic float32 column, and drops the target vector column when the user
// did not request it. It is a no-op when the plan does not carry the
// calc_distance spec.
//
// refVector is the canonical byte encoding for refVectorType (see the proxy's
// resolveCalcDistance): float32 LE bytes for FloatVector, half-precision bytes
// for Float16/BFloat16, dim bytes for Int8Vector, packed bits for BinaryVector,
// and a compact sparse row for SparseFloatVector. fieldDim must be the
// schema-fixed dimension for dense types and 0 for sparse.
func calcDistanceColumn(result *segcorepb.RetrieveResults, refVector []byte, refVectorType schemapb.DataType, metricType string, vectorFieldID int64, distanceOutputName string, userOutputFieldIDs []int64, fieldDim int64) error {
	// locate the target vector field data
	var vectorFieldIdx = -1
	var vectorData *schemapb.VectorField
	for i, fd := range result.GetFieldsData() {
		if fd.GetFieldId() == vectorFieldID {
			vectorFieldIdx = i
			vectorData = fd.GetVectors()
			break
		}
	}
	if vectorFieldIdx == -1 {
		return merr.WrapErrServiceInternalMsg("calc_distance: vector field %d not found in retrieve result", vectorFieldID)
	}

	distances, err := calcDistances(refVector, refVectorType, metricType, vectorData, fieldDim)
	if err != nil {
		return err
	}

	// append synthetic distance column at the end
	result.FieldsData = append(result.FieldsData, &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: distanceOutputName,
		FieldId:   reservedFieldID,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: distances},
				},
			},
		},
	})

	// drop the vector column unless the user requested it
	userRequested := false
	for _, id := range userOutputFieldIDs {
		if id == vectorFieldID {
			userRequested = true
			break
		}
	}
	if !userRequested {
		result.FieldsData = append(result.FieldsData[:vectorFieldIdx], result.FieldsData[vectorFieldIdx+1:]...)
	}
	return nil
}

// calcDistances dispatches the per-row distance computation on the target
// vector field's data type. The reference vector is always a single vector;
// the result is one distance per row of vectorData.
func calcDistances(refVector []byte, refVectorType schemapb.DataType, metricType string, vectorData *schemapb.VectorField, fieldDim int64) ([]float32, error) {
	switch refVectorType {
	case schemapb.DataType_FloatVector:
		refVec := typeutil.BytesToFloat32Vector(refVector)
		rowData := vectorData.GetFloatVector().GetData()
		if int64(len(rowData))%fieldDim != 0 {
			return nil, merr.WrapErrServiceInternalMsg("calc_distance: float vector field %d data length %d is not a multiple of dim %d", vectorData.GetDim(), len(rowData), fieldDim)
		}
		return distance.CalcFloatDistance(fieldDim, refVec, rowData, metricType)
	case schemapb.DataType_Int8Vector:
		refVec := int8BytesToFloat32Vector(refVector)
		rowData := int8BytesToFloat32Vector(vectorData.GetInt8Vector())
		return distance.CalcFloatDistance(fieldDim, refVec, rowData, metricType)
	case schemapb.DataType_Float16Vector:
		refVec := typeutil.Float16BytesToFloat32Vector(refVector)
		rowData := typeutil.Float16BytesToFloat32Vector(vectorData.GetFloat16Vector())
		return distance.CalcFloatDistance(fieldDim, refVec, rowData, metricType)
	case schemapb.DataType_BFloat16Vector:
		refVec := typeutil.BFloat16BytesToFloat32Vector(refVector)
		rowData := typeutil.BFloat16BytesToFloat32Vector(vectorData.GetBfloat16Vector())
		return distance.CalcFloatDistance(fieldDim, refVec, rowData, metricType)
	case schemapb.DataType_BinaryVector:
		return distance.CalcBinaryDistance(fieldDim, refVector, vectorData.GetBinaryVector(), metricType)
	case schemapb.DataType_SparseFloatVector:
		return distance.CalcSparseDistance(refVector, vectorData.GetSparseFloatVector().GetContents(), metricType)
	default:
		return nil, merr.WrapErrServiceInternalMsg("calc_distance: unsupported vector field type %s", refVectorType.String())
	}
}

// int8BytesToFloat32Vector decodes a signed int8 byte vector into float32.
func int8BytesToFloat32Vector(b []byte) []float32 {
	vec := make([]float32, len(b))
	for i, v := range b {
		vec[i] = float32(int8(v))
	}
	return vec
}
