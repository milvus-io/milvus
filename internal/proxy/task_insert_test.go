package proxy

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

func TestInsertTask_CheckAligned(t *testing.T) {
	var err error

	// passed NumRows is less than 0
	case1 := insertTask{
		insertMsg: &BaseInsertTask{
			InsertRequest: msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				NumRows: 0,
			},
		},
	}
	err = case1.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// checkLengthOfFieldsData was already checked by TestInsertTask_checkLengthOfFieldsData

	boolFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Bool}
	int8FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int8}
	int16FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int16}
	int32FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int32}
	int64FieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Int64}
	floatFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Float}
	doubleFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_Double}
	floatVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_FloatVector}
	binaryVectorFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_BinaryVector}
	varCharFieldSchema := &schemapb.FieldSchema{DataType: schemapb.DataType_VarChar}

	numRows := 20
	dim := 128
	case2 := insertTask{
		insertMsg: &BaseInsertTask{
			InsertRequest: msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				Version:    msgpb.InsertDataVersion_ColumnBased,
				RowIDs:     generateInt64Array(numRows),
				Timestamps: generateUint64Array(numRows),
			},
		},
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkRowNums",
			Description: "TestInsertTask_checkRowNums",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				boolFieldSchema,
				int8FieldSchema,
				int16FieldSchema,
				int32FieldSchema,
				int64FieldSchema,
				floatFieldSchema,
				doubleFieldSchema,
				floatVectorFieldSchema,
				binaryVectorFieldSchema,
				varCharFieldSchema,
			},
		},
	}

	// satisfied
	case2.insertMsg.NumRows = uint64(numRows)
	case2.insertMsg.FieldsData = []*schemapb.FieldData{
		newScalarFieldData(boolFieldSchema, "Bool", numRows),
		newScalarFieldData(int8FieldSchema, "Int8", numRows),
		newScalarFieldData(int16FieldSchema, "Int16", numRows),
		newScalarFieldData(int32FieldSchema, "Int32", numRows),
		newScalarFieldData(int64FieldSchema, "Int64", numRows),
		newScalarFieldData(floatFieldSchema, "Float", numRows),
		newScalarFieldData(doubleFieldSchema, "Double", numRows),
		newFloatVectorFieldData("FloatVector", numRows, dim),
		newBinaryVectorFieldData("BinaryVector", numRows, dim),
		newScalarFieldData(varCharFieldSchema, "VarChar", numRows),
	}
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less bool data
	case2.insertMsg.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more bool data
	case2.insertMsg.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[0] = newScalarFieldData(boolFieldSchema, "Bool", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int8 data
	case2.insertMsg.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more int8 data
	case2.insertMsg.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[1] = newScalarFieldData(int8FieldSchema, "Int8", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int16 data
	case2.insertMsg.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more int16 data
	case2.insertMsg.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[2] = newScalarFieldData(int16FieldSchema, "Int16", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int32 data
	case2.insertMsg.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more int32 data
	case2.insertMsg.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[3] = newScalarFieldData(int32FieldSchema, "Int32", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less int64 data
	case2.insertMsg.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more int64 data
	case2.insertMsg.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[4] = newScalarFieldData(int64FieldSchema, "Int64", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less float data
	case2.insertMsg.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more float data
	case2.insertMsg.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[5] = newScalarFieldData(floatFieldSchema, "Float", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less double data
	case2.insertMsg.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.insertMsg.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[6] = newScalarFieldData(doubleFieldSchema, "Double", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, nil, err)

	// less float vectors
	case2.insertMsg.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows/2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more float vectors
	case2.insertMsg.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows*2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows, dim)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less binary vectors
	case2.insertMsg.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows/2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more binary vectors
	case2.insertMsg.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows*2, dim)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows, dim)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)

	// less double data
	case2.insertMsg.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows/2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// more double data
	case2.insertMsg.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows*2)
	err = case2.insertMsg.CheckAligned()
	assert.Error(t, err)
	// revert
	case2.insertMsg.FieldsData[8] = newScalarFieldData(varCharFieldSchema, "VarChar", numRows)
	err = case2.insertMsg.CheckAligned()
	assert.NoError(t, err)
}

func TestInsertTask_CheckVectorFieldData(t *testing.T) {
	fieldName := "embeddings"
	numRows := 10
	dim := 32
	task := insertTask{
		insertMsg: &BaseInsertTask{
			InsertRequest: msgpb.InsertRequest{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
				},
				Version: msgpb.InsertDataVersion_ColumnBased,
				NumRows: uint64(numRows),
			},
		},
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_CheckVectorFieldData",
			Description: "TestInsertTask_CheckVectorFieldData",
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      100,
					Name:         fieldName,
					IsPrimaryKey: false,
					AutoID:       false,
					DataType:     schemapb.DataType_FloatVector,
					TypeParams:   []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(dim)}},
				},
			},
		},
	}

	// success case
	task.insertMsg.FieldsData = []*schemapb.FieldData{
		newFloatVectorFieldData(fieldName, numRows, dim),
	}
	err := task.checkVectorFieldData()
	assert.NoError(t, err)

	// field is nil
	task.insertMsg.FieldsData = []*schemapb.FieldData{
		{
			Type:      schemapb.DataType_FloatVector,
			FieldName: fieldName,
			Field: &schemapb.FieldData_Vectors{
				Vectors: nil,
			},
		},
	}
	err = task.checkVectorFieldData()
	assert.Error(t, err)

	// vector data is not a number
	values := generateFloatVectors(numRows, dim)
	values[5] = float32(math.NaN())
	task.insertMsg.FieldsData[0].Field = &schemapb.FieldData_Vectors{
		Vectors: &schemapb.VectorField{
			Dim: int64(dim),
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: values,
				},
			},
		},
	}
	err = task.checkVectorFieldData()
	assert.Error(t, err)

	// vector data is infinity
	values[5] = float32(math.Inf(1))
	task.insertMsg.FieldsData[0].Field = &schemapb.FieldData_Vectors{
		Vectors: &schemapb.VectorField{
			Dim: int64(dim),
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: values,
				},
			},
		},
	}
	err = task.checkVectorFieldData()
	assert.Error(t, err)

	// vector dim not match
	task.insertMsg.FieldsData = []*schemapb.FieldData{
		newFloatVectorFieldData(fieldName, numRows, dim+1),
	}
	err = task.checkVectorFieldData()
	assert.Error(t, err)
}
