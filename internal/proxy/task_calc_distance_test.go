package proxy

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/stretchr/testify/assert"
)

func TestCalcDistanceTask_arrangeVectorsByStrID(t *testing.T) {
	task := &calcDistanceTask{}

	inputIds := make([]string, 0)
	inputIds = append(inputIds, "c")
	inputIds = append(inputIds, "b")
	inputIds = append(inputIds, "a")

	sequence := make(map[string]int)
	sequence["a"] = 0
	sequence["b"] = 1
	sequence["c"] = 2

	dim := 16

	// float vector
	floatValue := make([]float32, 0)
	for i := 0; i < dim*3; i++ {
		floatValue = append(floatValue, float32(i))
	}
	retrievedVectors := &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{
				Data: floatValue,
			},
		},
	}

	result, err := task.arrangeVectorsByStrID(inputIds, sequence, retrievedVectors)
	assert.NoError(t, err)

	floatResult := result.GetFloatVector().GetData()
	for i := 0; i < 3; i++ {
		for j := 0; j < dim; j++ {
			assert.Equal(t, floatValue[dim*sequence[inputIds[i]]+j], floatResult[i*dim+j])
		}
	}

	// binary vector
	binaryValue := make([]byte, 0)
	for i := 0; i < 3*dim/8; i++ {
		binaryValue = append(binaryValue, byte(i))
	}
	retrievedVectors = &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_BinaryVector{
			BinaryVector: binaryValue,
		},
	}

	result, err = task.arrangeVectorsByStrID(inputIds, sequence, retrievedVectors)
	assert.NoError(t, err)

	binaryResult := result.GetBinaryVector()
	numBytes := dim / 8
	for i := 0; i < 3; i++ {
		for j := 0; j < numBytes; j++ {
			assert.Equal(t, binaryValue[sequence[inputIds[i]]*numBytes+j], binaryResult[i*numBytes+j])
		}
	}
}

func TestCalcDistanceTask_arrangeVectorsByIntID(t *testing.T) {
	task := &calcDistanceTask{}

	inputIds := make([]int64, 0)
	inputIds = append(inputIds, 2)
	inputIds = append(inputIds, 0)
	inputIds = append(inputIds, 1)

	sequence := make(map[int64]int)
	sequence[0] = 0
	sequence[1] = 1
	sequence[2] = 2

	dim := 16

	// float vector
	floatValue := make([]float32, 0)
	for i := 0; i < dim*3; i++ {
		floatValue = append(floatValue, float32(i))
	}
	retrievedVectors := &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{
				Data: floatValue,
			},
		},
	}

	result, err := task.arrangeVectorsByIntID(inputIds, sequence, retrievedVectors)
	assert.NoError(t, err)

	floatResult := result.GetFloatVector().GetData()
	for i := 0; i < 3; i++ {
		for j := 0; j < dim; j++ {
			assert.Equal(t, floatValue[dim*sequence[inputIds[i]]+j], floatResult[i*dim+j])
		}
	}

	// binary vector
	binaryValue := make([]byte, 0)
	for i := 0; i < dim*3; i++ {
		binaryValue = append(binaryValue, byte(i))
	}
	retrievedVectors = &schemapb.VectorField{
		Dim: int64(dim),
		Data: &schemapb.VectorField_BinaryVector{
			BinaryVector: binaryValue,
		},
	}

	result, err = task.arrangeVectorsByIntID(inputIds, sequence, retrievedVectors)
	assert.NoError(t, err)

	binaryResult := result.GetBinaryVector()
	numBytes := dim / 8
	for i := 0; i < 3; i++ {
		for j := 0; j < numBytes; j++ {
			assert.Equal(t, binaryValue[sequence[inputIds[i]]*numBytes+j], binaryResult[i*numBytes+j])
		}
	}
}

func TestCalcDistanceTask_ExecuteFloat(t *testing.T) {
	ctx := context.Background()
	queryFunc := func(ids *milvuspb.VectorIDs) (*milvuspb.QueryResults, error) {
		return nil, errors.New("unexpected error")
	}

	task := &calcDistanceTask{
		traceID:   "dummy",
		queryFunc: queryFunc,
	}

	request := &milvuspb.CalcDistanceRequest{
		OpLeft:  nil,
		OpRight: nil,
		Params: []*commonpb.KeyValuePair{
			{Key: "metric", Value: "L2"},
		},
	}

	// left-op empty
	calcResult, err := task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)

	request = &milvuspb.CalcDistanceRequest{
		OpLeft: &milvuspb.VectorsArray{
			Array: &milvuspb.VectorsArray_IdArray{
				IdArray: &milvuspb.VectorIDs{},
			},
		},
		OpRight: nil,
		Params: []*commonpb.KeyValuePair{
			{Key: "metric", Value: "L2"},
		},
	}

	// left-op query error
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)

	fieldIds := make([]int64, 0)
	fieldIds = append(fieldIds, 2)
	fieldIds = append(fieldIds, 0)
	fieldIds = append(fieldIds, 1)

	dim := 8
	floatValue := make([]float32, 0)
	for i := 0; i < dim*3; i++ {
		floatValue = append(floatValue, float32(i))
	}

	queryFunc = func(ids *milvuspb.VectorIDs) (*milvuspb.QueryResults, error) {
		if ids == nil {
			return &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "unexpected",
				},
			}, nil
		}

		return &milvuspb.QueryResults{
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "id",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: fieldIds,
								},
							},
						},
					},
				},
				{
					Type:      schemapb.DataType_FloatVector,
					FieldName: "vec",
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: int64(dim),
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{
									Data: floatValue,
								},
							},
						},
					},
				},
			},
		}, nil
	}

	task.queryFunc = queryFunc
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)

	idArray := &milvuspb.VectorsArray{
		Array: &milvuspb.VectorsArray_IdArray{
			IdArray: &milvuspb.VectorIDs{
				FieldName: "vec",
				IdArray: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: fieldIds,
						},
					},
				},
			},
		},
	}
	request = &milvuspb.CalcDistanceRequest{
		OpLeft:  idArray,
		OpRight: idArray,
		Params: []*commonpb.KeyValuePair{
			{Key: "metric", Value: "L2"},
		},
	}

	// success
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, calcResult.Status.ErrorCode)

	// right-op query error
	request.OpRight = nil
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)

	request.OpRight = &milvuspb.VectorsArray{
		Array: &milvuspb.VectorsArray_IdArray{
			IdArray: &milvuspb.VectorIDs{
				FieldName: "kkk",
				IdArray: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: fieldIds,
						},
					},
				},
			},
		},
	}

	// right-op arrange error
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)

	request.OpRight = &milvuspb.VectorsArray{
		Array: &milvuspb.VectorsArray_DataArray{
			DataArray: &schemapb.VectorField{
				Dim: 5,
			},
		},
	}

	// different dimension
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)

	request.OpRight = &milvuspb.VectorsArray{
		Array: &milvuspb.VectorsArray_DataArray{
			DataArray: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: make([]float32, 0),
					},
				},
			},
		},
	}

	// calcdistance return error
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)
}

func TestCalcDistanceTask_ExecuteBinary(t *testing.T) {
	ctx := context.Background()

	fieldIds := make([]int64, 0)
	fieldIds = append(fieldIds, 2)
	fieldIds = append(fieldIds, 0)
	fieldIds = append(fieldIds, 1)

	dim := 16
	binaryValue := make([]byte, 0)
	for i := 0; i < 3*dim/8; i++ {
		binaryValue = append(binaryValue, byte(i))
	}

	queryFunc := func(ids *milvuspb.VectorIDs) (*milvuspb.QueryResults, error) {
		if ids == nil {
			return &milvuspb.QueryResults{
				Status: &commonpb.Status{
					ErrorCode: commonpb.ErrorCode_UnexpectedError,
					Reason:    "unexpected",
				},
			}, nil
		}

		return &milvuspb.QueryResults{
			FieldsData: []*schemapb.FieldData{
				{
					Type:      schemapb.DataType_Int64,
					FieldName: "id",
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: fieldIds,
								},
							},
						},
					},
				},
				{
					Type:      schemapb.DataType_FloatVector,
					FieldName: "vec",
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: int64(dim),
							Data: &schemapb.VectorField_BinaryVector{
								BinaryVector: binaryValue,
							},
						},
					},
				},
			},
		}, nil
	}

	idArray := &milvuspb.VectorsArray{
		Array: &milvuspb.VectorsArray_IdArray{
			IdArray: &milvuspb.VectorIDs{
				FieldName: "vec",
				IdArray: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: fieldIds,
						},
					},
				},
			},
		},
	}
	request := &milvuspb.CalcDistanceRequest{
		OpLeft:  idArray,
		OpRight: idArray,
		Params: []*commonpb.KeyValuePair{
			{Key: "metric", Value: "HAMMING"},
		},
	}

	task := &calcDistanceTask{
		traceID:   "dummy",
		queryFunc: queryFunc,
	}

	// success
	calcResult, err := task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, calcResult.Status.ErrorCode)

	floatArray := &milvuspb.VectorsArray{
		Array: &milvuspb.VectorsArray_DataArray{
			DataArray: &schemapb.VectorField{
				Dim:  int64(dim),
				Data: &schemapb.VectorField_FloatVector{},
			},
		},
	}
	binaryArray := &milvuspb.VectorsArray{
		Array: &milvuspb.VectorsArray_DataArray{
			DataArray: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: binaryValue,
				},
			},
		},
	}
	request = &milvuspb.CalcDistanceRequest{
		OpLeft:  floatArray,
		OpRight: binaryArray,
		Params: []*commonpb.KeyValuePair{
			{Key: "metric", Value: "HAMMING"},
		},
	}

	// float vs binary
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)

	request = &milvuspb.CalcDistanceRequest{
		OpLeft:  binaryArray,
		OpRight: binaryArray,
		Params: []*commonpb.KeyValuePair{
			{Key: "metric", Value: "HAMMING"},
		},
	}

	// hamming
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, calcResult.Status.ErrorCode)

	request = &milvuspb.CalcDistanceRequest{
		OpLeft:  binaryArray,
		OpRight: binaryArray,
		Params: []*commonpb.KeyValuePair{
			{Key: "metric", Value: "TANIMOTO"},
		},
	}

	// tanimoto
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, calcResult.Status.ErrorCode)

	request = &milvuspb.CalcDistanceRequest{
		OpLeft: binaryArray,
		OpRight: &milvuspb.VectorsArray{
			Array: &milvuspb.VectorsArray_DataArray{
				DataArray: &schemapb.VectorField{
					Dim: int64(dim),
					Data: &schemapb.VectorField_BinaryVector{
						BinaryVector: make([]byte, 0),
					},
				},
			},
		},
		Params: []*commonpb.KeyValuePair{
			{Key: "metric", Value: "HAMMING"},
		},
	}

	// hamming error
	calcResult, err = task.Execute(ctx, request)
	assert.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, calcResult.Status.ErrorCode)
}
