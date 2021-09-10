package proxy

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"github.com/milvus-io/milvus/internal/proto/schemapb"

	"github.com/stretchr/testify/assert"
)

// TODO(dragondriver): add more test cases

func TestGetNumRowsOfScalarField(t *testing.T) {
	cases := []struct {
		datas interface{}
		want  uint32
	}{
		{[]bool{}, 0},
		{[]bool{true, false}, 2},
		{[]int32{}, 0},
		{[]int32{1, 2}, 2},
		{[]int64{}, 0},
		{[]int64{1, 2}, 2},
		{[]float32{}, 0},
		{[]float32{1.0, 2.0}, 2},
		{[]float64{}, 0},
		{[]float64{1.0, 2.0}, 2},
	}

	for _, test := range cases {
		if got := getNumRowsOfScalarField(test.datas); got != test.want {
			t.Errorf("getNumRowsOfScalarField(%v) = %v", test.datas, test.want)
		}
	}
}

func TestGetNumRowsOfFloatVectorField(t *testing.T) {
	cases := []struct {
		fDatas   []float32
		dim      int64
		want     uint32
		errIsNil bool
	}{
		{[]float32{}, -1, 0, false},     // dim <= 0
		{[]float32{}, 0, 0, false},      // dim <= 0
		{[]float32{1.0}, 128, 0, false}, // length % dim != 0
		{[]float32{}, 128, 0, true},
		{[]float32{1.0, 2.0}, 2, 1, true},
		{[]float32{1.0, 2.0, 3.0, 4.0}, 2, 2, true},
	}

	for _, test := range cases {
		got, err := getNumRowsOfFloatVectorField(test.fDatas, test.dim)
		if test.errIsNil {
			assert.Equal(t, nil, err)
			if got != test.want {
				t.Errorf("getNumRowsOfFloatVectorField(%v, %v) = %v, %v", test.fDatas, test.dim, test.want, nil)
			}
		} else {
			assert.NotEqual(t, nil, err)
		}
	}
}

func TestGetNumRowsOfBinaryVectorField(t *testing.T) {
	cases := []struct {
		bDatas   []byte
		dim      int64
		want     uint32
		errIsNil bool
	}{
		{[]byte{}, -1, 0, false},     // dim <= 0
		{[]byte{}, 0, 0, false},      // dim <= 0
		{[]byte{1.0}, 128, 0, false}, // length % dim != 0
		{[]byte{}, 128, 0, true},
		{[]byte{1.0}, 1, 0, false}, // dim % 8 != 0
		{[]byte{1.0}, 4, 0, false}, // dim % 8 != 0
		{[]byte{1.0, 2.0}, 8, 2, true},
		{[]byte{1.0, 2.0}, 16, 1, true},
		{[]byte{1.0, 2.0, 3.0, 4.0}, 8, 4, true},
		{[]byte{1.0, 2.0, 3.0, 4.0}, 16, 2, true},
		{[]byte{1.0}, 128, 0, false}, // (8*l) % dim != 0
	}

	for _, test := range cases {
		got, err := getNumRowsOfBinaryVectorField(test.bDatas, test.dim)
		if test.errIsNil {
			assert.Equal(t, nil, err)
			if got != test.want {
				t.Errorf("getNumRowsOfBinaryVectorField(%v, %v) = %v, %v", test.bDatas, test.dim, test.want, nil)
			}
		} else {
			assert.NotEqual(t, nil, err)
		}
	}
}

func TestInsertTask_checkLengthOfFieldsData(t *testing.T) {
	var err error

	// schema is empty, though won't happened in system
	case1 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields:      []*schemapb.FieldSchema{},
		},
		req: &milvuspb.InsertRequest{
			DbName:         "TestInsertTask_checkLengthOfFieldsData",
			CollectionName: "TestInsertTask_checkLengthOfFieldsData",
			PartitionName:  "TestInsertTask_checkLengthOfFieldsData",
			FieldsData:     nil,
		},
	}
	err = case1.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has two fields, neither of them are autoID
	case2 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	case2.req = &milvuspb.InsertRequest{}
	err = case2.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// the num of passed fields is less than needed
	case2.req = &milvuspb.InsertRequest{
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64,
			},
		},
	}
	err = case2.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// satisfied
	case2.req = &milvuspb.InsertRequest{
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64,
			},
			{
				Type: schemapb.DataType_Int64,
			},
		},
	}
	err = case2.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has two field, one of them are autoID
	case3 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   true,
					DataType: schemapb.DataType_Int64,
				},
				{
					AutoID:   false,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	case3.req = &milvuspb.InsertRequest{}
	err = case3.checkLengthOfFieldsData()
	assert.NotEqual(t, nil, err)
	// satisfied
	case3.req = &milvuspb.InsertRequest{
		FieldsData: []*schemapb.FieldData{
			{
				Type: schemapb.DataType_Int64,
			},
		},
	}
	err = case3.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)

	// schema has one field which is autoID
	case4 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkLengthOfFieldsData",
			Description: "TestInsertTask_checkLengthOfFieldsData",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{
					AutoID:   true,
					DataType: schemapb.DataType_Int64,
				},
			},
		},
	}
	// passed fields is empty
	// satisfied
	case4.req = &milvuspb.InsertRequest{}
	err = case4.checkLengthOfFieldsData()
	assert.Equal(t, nil, err)
}

func TestInsertTask_checkRowNums(t *testing.T) {
	var err error

	// passed NumRows is less than 0
	case1 := InsertTask{
		req: &milvuspb.InsertRequest{
			NumRows: 0,
		},
	}
	err = case1.checkRowNums()
	assert.NotEqual(t, nil, err)

	// checkLengthOfFieldsData was already checked by TestInsertTask_checkLengthOfFieldsData

	numRows := 20
	dim := 128
	case2 := InsertTask{
		schema: &schemapb.CollectionSchema{
			Name:        "TestInsertTask_checkRowNums",
			Description: "TestInsertTask_checkRowNums",
			AutoID:      false,
			Fields: []*schemapb.FieldSchema{
				{DataType: schemapb.DataType_Bool},
				{DataType: schemapb.DataType_Int8},
				{DataType: schemapb.DataType_Int16},
				{DataType: schemapb.DataType_Int32},
				{DataType: schemapb.DataType_Int64},
				{DataType: schemapb.DataType_Float},
				{DataType: schemapb.DataType_Double},
				{DataType: schemapb.DataType_FloatVector},
				{DataType: schemapb.DataType_BinaryVector},
			},
		},
	}

	// satisfied
	case2.req = &milvuspb.InsertRequest{
		NumRows: uint32(numRows),
		FieldsData: []*schemapb.FieldData{
			newScalarFieldData(schemapb.DataType_Bool, "Bool", numRows),
			newScalarFieldData(schemapb.DataType_Int8, "Int8", numRows),
			newScalarFieldData(schemapb.DataType_Int16, "Int16", numRows),
			newScalarFieldData(schemapb.DataType_Int32, "Int32", numRows),
			newScalarFieldData(schemapb.DataType_Int64, "Int64", numRows),
			newScalarFieldData(schemapb.DataType_Float, "Float", numRows),
			newScalarFieldData(schemapb.DataType_Double, "Double", numRows),
			newFloatVectorFieldData("FloatVector", numRows, dim),
			newBinaryVectorFieldData("BinaryVector", numRows, dim),
		},
	}
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less bool data
	case2.req.FieldsData[0] = newScalarFieldData(schemapb.DataType_Bool, "Bool", numRows/2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more bool data
	case2.req.FieldsData[0] = newScalarFieldData(schemapb.DataType_Bool, "Bool", numRows*2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[0] = newScalarFieldData(schemapb.DataType_Bool, "Bool", numRows)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less int8 data
	case2.req.FieldsData[1] = newScalarFieldData(schemapb.DataType_Int8, "Int8", numRows/2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more int8 data
	case2.req.FieldsData[1] = newScalarFieldData(schemapb.DataType_Int8, "Int8", numRows*2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[1] = newScalarFieldData(schemapb.DataType_Int8, "Int8", numRows)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less int16 data
	case2.req.FieldsData[2] = newScalarFieldData(schemapb.DataType_Int16, "Int16", numRows/2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more int16 data
	case2.req.FieldsData[2] = newScalarFieldData(schemapb.DataType_Int16, "Int16", numRows*2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[2] = newScalarFieldData(schemapb.DataType_Int16, "Int16", numRows)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less int32 data
	case2.req.FieldsData[3] = newScalarFieldData(schemapb.DataType_Int32, "Int32", numRows/2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more int32 data
	case2.req.FieldsData[3] = newScalarFieldData(schemapb.DataType_Int32, "Int32", numRows*2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[3] = newScalarFieldData(schemapb.DataType_Int32, "Int32", numRows)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less int64 data
	case2.req.FieldsData[4] = newScalarFieldData(schemapb.DataType_Int64, "Int64", numRows/2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more int64 data
	case2.req.FieldsData[4] = newScalarFieldData(schemapb.DataType_Int64, "Int64", numRows*2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[4] = newScalarFieldData(schemapb.DataType_Int64, "Int64", numRows)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less float data
	case2.req.FieldsData[5] = newScalarFieldData(schemapb.DataType_Float, "Float", numRows/2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more float data
	case2.req.FieldsData[5] = newScalarFieldData(schemapb.DataType_Float, "Float", numRows*2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[5] = newScalarFieldData(schemapb.DataType_Float, "Float", numRows)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less double data
	case2.req.FieldsData[6] = newScalarFieldData(schemapb.DataType_Double, "Double", numRows/2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more double data
	case2.req.FieldsData[6] = newScalarFieldData(schemapb.DataType_Double, "Double", numRows*2)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[6] = newScalarFieldData(schemapb.DataType_Double, "Double", numRows)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less float vectors
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows/2, dim)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more float vectors
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows*2, dim)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[7] = newFloatVectorFieldData("FloatVector", numRows, dim)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)

	// less binary vectors
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows/2, dim)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// more binary vectors
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows*2, dim)
	err = case2.checkRowNums()
	assert.NotEqual(t, nil, err)
	// revert
	case2.req.FieldsData[7] = newBinaryVectorFieldData("BinaryVector", numRows, dim)
	err = case2.checkRowNums()
	assert.Equal(t, nil, err)
}

func TestTranslateOutputFields(t *testing.T) {
	const (
		idFieldName           = "id"
		tsFieldName           = "timestamp"
		floatVectorFieldName  = "float_vector"
		binaryVectorFieldName = "binary_vector"
	)
	var outputFields []string
	var err error

	schema := &schemapb.CollectionSchema{
		Name:        "TestTranslateOutputFields",
		Description: "TestTranslateOutputFields",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{Name: idFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: tsFieldName, DataType: schemapb.DataType_Int64},
			{Name: floatVectorFieldName, DataType: schemapb.DataType_FloatVector},
			{Name: binaryVectorFieldName, DataType: schemapb.DataType_BinaryVector},
		},
	}

	outputFields, err = translateOutputFields([]string{}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{" * "}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{" % "}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", "%"}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", tsFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", floatVectorFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", idFieldName}, schema, false)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	//=========================================================================
	outputFields, err = translateOutputFields([]string{}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{idFieldName, tsFieldName, floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", "%"}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", tsFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"*", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, tsFieldName, floatVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", floatVectorFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)

	outputFields, err = translateOutputFields([]string{"%", idFieldName}, schema, true)
	assert.Equal(t, nil, err)
	assert.ElementsMatch(t, []string{idFieldName, floatVectorFieldName, binaryVectorFieldName}, outputFields)
}
