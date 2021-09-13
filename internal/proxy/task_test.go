package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/msgstream"

	"github.com/milvus-io/milvus/internal/util/distance"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/uniquegenerator"
	"github.com/stretchr/testify/assert"
)

// TODO(dragondriver): add more test cases

func constructCollectionSchema(
	int64Field, floatVecField string,
	dim int,
	collectionName string,
) *schemapb.CollectionSchema {

	pk := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       true,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      0,
		Name:         floatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(dim),
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	return &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			pk,
			fVec,
		},
	}
}

func constructPlaceholderGroup(
	nq, dim int,
) *milvuspb.PlaceholderGroup {
	values := make([][]byte, 0, nq)
	for i := 0; i < nq; i++ {
		bs := make([]byte, 0, dim*4)
		for j := 0; j < dim; j++ {
			var buffer bytes.Buffer
			f := rand.Float32()
			err := binary.Write(&buffer, binary.LittleEndian, f)
			if err != nil {
				panic(err)
			}
			bs = append(bs, buffer.Bytes()...)
		}
		values = append(values, bs)
	}

	return &milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   milvuspb.PlaceholderType_FloatVector,
				Values: values,
			},
		},
	}
}

func constructSearchRequest(
	dbName, collectionName string,
	expr string,
	floatVecField string,
	nq, dim, nprobe, topk int,
) *milvuspb.SearchRequest {
	params := make(map[string]string)
	params["nprobe"] = strconv.Itoa(nprobe)
	b, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	plg := constructPlaceholderGroup(nq, dim)
	plgBs, err := proto.Marshal(plg)
	if err != nil {
		panic(err)
	}

	return &milvuspb.SearchRequest{
		Base:             nil,
		DbName:           dbName,
		CollectionName:   collectionName,
		PartitionNames:   nil,
		Dsl:              expr,
		PlaceholderGroup: plgBs,
		DslType:          commonpb.DslType_BoolExprV1,
		OutputFields:     nil,
		SearchParams: []*commonpb.KeyValuePair{
			{
				Key:   MetricTypeKey,
				Value: distance.L2,
			},
			{
				Key:   SearchParamsKey,
				Value: string(b),
			},
			{
				Key:   AnnsFieldKey,
				Value: floatVecField,
			},
			{
				Key:   TopKKey,
				Value: strconv.Itoa(topk),
			},
		},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
	}
}

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
	case1 := insertTask{
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
	case2 := insertTask{
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
	case3 := insertTask{
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
	case4 := insertTask{
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
	case1 := insertTask{
		req: &milvuspb.InsertRequest{
			NumRows: 0,
		},
	}
	err = case1.checkRowNums()
	assert.NotEqual(t, nil, err)

	// checkLengthOfFieldsData was already checked by TestInsertTask_checkLengthOfFieldsData

	numRows := 20
	dim := 128
	case2 := insertTask{
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

func TestSearchTask(t *testing.T) {
	ctx := context.Background()
	ctxCancel, cancel := context.WithCancel(ctx)
	qt := &searchTask{
		ctx:       ctxCancel,
		Condition: NewTaskCondition(context.TODO()),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.SearchResults),
		query:     nil,
		chMgr:     nil,
		qc:        nil,
	}

	// no result
	go func() {
		qt.resultBuf <- []*internalpb.SearchResults{}
	}()
	err := qt.PostExecute(context.TODO())
	assert.NotNil(t, err)

	// test trace context done
	cancel()
	err = qt.PostExecute(context.TODO())
	assert.NotNil(t, err)

	// error result
	ctx = context.Background()
	qt = &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(context.TODO()),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.SearchResults),
		query:     nil,
		chMgr:     nil,
		qc:        nil,
	}

	// no result
	go func() {
		result := internalpb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    "test",
			},
		}
		results := make([]*internalpb.SearchResults, 1)
		results[0] = &result
		qt.resultBuf <- results
	}()
	err = qt.PostExecute(context.TODO())
	assert.NotNil(t, err)

	log.Debug("PostExecute failed" + err.Error())
	// check result SlicedBlob

	ctx = context.Background()
	qt = &searchTask{
		ctx:       ctx,
		Condition: NewTaskCondition(context.TODO()),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_Search,
				SourceID: Params.ProxyID,
			},
			ResultChannelID: strconv.FormatInt(Params.ProxyID, 10),
		},
		resultBuf: make(chan []*internalpb.SearchResults),
		query:     nil,
		chMgr:     nil,
		qc:        nil,
	}

	// no result
	go func() {
		result := internalpb.SearchResults{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
				Reason:    "test",
			},
			SlicedBlob: nil,
		}
		results := make([]*internalpb.SearchResults, 1)
		results[0] = &result
		qt.resultBuf <- results
	}()
	err = qt.PostExecute(context.TODO())
	assert.Nil(t, err)

	assert.Equal(t, qt.result.Status.ErrorCode, commonpb.ErrorCode_Success)

	// TODO, add decode result, reduce result test
}

func TestCreateCollectionTask(t *testing.T) {
	Params.Init()

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	shardsNum := int32(2)
	prefix := "TestCreateCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	var marshaledSchema []byte
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	task := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	t.Run("on enqueue", func(t *testing.T) {
		err := task.OnEnqueue()
		assert.NoError(t, err)
		assert.Equal(t, commonpb.MsgType_CreateCollection, task.Type())
	})

	t.Run("ctx", func(t *testing.T) {
		traceCtx := task.TraceCtx()
		assert.NotNil(t, traceCtx)
	})

	t.Run("id", func(t *testing.T) {
		id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
		task.SetID(id)
		assert.Equal(t, id, task.ID())
	})

	t.Run("name", func(t *testing.T) {
		assert.Equal(t, CreateCollectionTaskName, task.Name())
	})

	t.Run("ts", func(t *testing.T) {
		ts := Timestamp(time.Now().UnixNano())
		task.SetTs(ts)
		assert.Equal(t, ts, task.BeginTs())
		assert.Equal(t, ts, task.EndTs())
	})

	t.Run("process task", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.Equal(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		// recreate -> fail
		err = task.Execute(ctx)
		assert.NoError(t, err)
		assert.NotEqual(t, commonpb.ErrorCode_Success, task.result.ErrorCode)

		err = task.PostExecute(ctx)
		assert.NoError(t, err)
	})

	t.Run("PreExecute", func(t *testing.T) {
		var err error

		err = task.PreExecute(ctx)
		assert.NoError(t, err)

		task.Schema = []byte{0x1, 0x2, 0x3, 0x4}
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.Schema = marshaledSchema

		task.ShardsNum = Params.MaxShardNum + 1
		err = task.PreExecute(ctx)
		assert.Error(t, err)
		task.ShardsNum = shardsNum

		reqBackup := proto.Clone(task.CreateCollectionRequest).(*milvuspb.CreateCollectionRequest)
		schemaBackup := proto.Clone(schema).(*schemapb.CollectionSchema)

		schemaWithTooManyFields := &schemapb.CollectionSchema{
			Name:        collectionName,
			Description: "",
			AutoID:      false,
			Fields:      make([]*schemapb.FieldSchema, Params.MaxFieldNum+1),
		}
		marshaledSchemaWithTooManyFields, err := proto.Marshal(schemaWithTooManyFields)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = marshaledSchemaWithTooManyFields
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		task.CreateCollectionRequest = reqBackup

		// ValidateCollectionName

		schema.Name = " " // empty
		emptyNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = emptyNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema.Name = prefix
		for i := 0; i < int(Params.MaxNameLength); i++ {
			schema.Name += strconv.Itoa(i % 10)
		}
		tooLongNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooLongNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema.Name = "$" // invalid first char
		invalidFirstCharSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidFirstCharSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// ValidateDuplicatedFieldName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields = append(schema.Fields, schema.Fields[0])
		duplicatedFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = duplicatedFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// ValidatePrimaryKey
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			schema.Fields[idx].IsPrimaryKey = false
		}
		noPrimaryFieldsSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noPrimaryFieldsSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// ValidateFieldName
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			schema.Fields[idx].Name = "$"
		}
		invalidFieldNameSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = invalidFieldNameSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		// ValidateVectorField
		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = nil
			}
		}
		noDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = noDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: "not int",
					},
				}
			}
		}
		dimNotIntSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = dimNotIntSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		for idx := range schema.Fields {
			if schema.Fields[idx].DataType == schemapb.DataType_FloatVector ||
				schema.Fields[idx].DataType == schemapb.DataType_BinaryVector {
				schema.Fields[idx].TypeParams = []*commonpb.KeyValuePair{
					{
						Key:   "dim",
						Value: strconv.Itoa(int(Params.MaxDimension) + 1),
					},
				}
			}
		}
		tooLargeDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = tooLargeDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)

		schema = proto.Clone(schemaBackup).(*schemapb.CollectionSchema)
		schema.Fields[1].DataType = schemapb.DataType_BinaryVector
		schema.Fields[1].TypeParams = []*commonpb.KeyValuePair{
			{
				Key:   "dim",
				Value: strconv.Itoa(int(Params.MaxDimension) + 1),
			},
		}
		binaryTooLargeDimSchema, err := proto.Marshal(schema)
		assert.NoError(t, err)
		task.CreateCollectionRequest.Schema = binaryTooLargeDimSchema
		err = task.PreExecute(ctx)
		assert.Error(t, err)
	})
}

func TestDropCollectionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	InitMetaCache(rc)

	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	channelMgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer channelMgr.removeAllDMLStream()

	prefix := "TestDropCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	//CreateCollection
	task := &dropCollectionTask{
		Condition: NewTaskCondition(ctx),
		DropCollectionRequest: &milvuspb.DropCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		chMgr:     channelMgr,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyID, task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.NotNil(t, err)
	// createCollection in RootCood and fill GlobalMetaCache
	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	// success to drop collection
	err = task.Execute(ctx)
	assert.Nil(t, err)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.Nil(t, err)

}

func TestHasCollectionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	InitMetaCache(rc)
	prefix := "TestHasCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	shardsNum := int32(2)
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColReq := &milvuspb.CreateCollectionRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_DropCollection,
			MsgID:     100,
			Timestamp: 100,
		},
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      shardsNum,
	}

	//CreateCollection
	task := &hasCollectionTask{
		Condition: NewTaskCondition(ctx),
		HasCollectionRequest: &milvuspb.HasCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_HasCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyID, task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, false, task.result.Value)
	// createCollection in RootCood and fill GlobalMetaCache
	rc.CreateCollection(ctx, createColReq)
	globalMetaCache.GetCollectionID(ctx, collectionName)

	// success to drop collection
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, true, task.result.Value)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	rc.updateState(internalpb.StateCode_Abnormal)
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.Nil(t, err)
	err = task.Execute(ctx)
	assert.NotNil(t, err)

}

func TestDescribeCollectionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	InitMetaCache(rc)
	prefix := "TestDescribeCollectionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()

	//CreateCollection
	task := &describeCollectionTask{
		Condition: NewTaskCondition(ctx),
		DescribeCollectionRequest: &milvuspb.DescribeCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DescribeCollection,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DescribeCollection, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyID, task.GetBase().GetSourceID())
	// missing collectionID in globalMetaCache
	err := task.Execute(ctx)
	assert.Nil(t, err)

	// illegal name
	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	rc.Stop()
	task.CollectionName = collectionName
	err = task.PreExecute(ctx)
	assert.Nil(t, err)
	err = task.Execute(ctx)
	assert.Nil(t, err)
	assert.Equal(t, commonpb.ErrorCode_UnexpectedError, task.result.Status.ErrorCode)
}

func TestCreatePartitionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestCreatePartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &createPartitionTask{
		Condition: NewTaskCondition(ctx),
		CreatePartitionRequest: &milvuspb.CreatePartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_CreatePartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_CreatePartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyID, task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestDropPartitionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestDropPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &dropPartitionTask{
		Condition: NewTaskCondition(ctx),
		DropPartitionRequest: &milvuspb.DropPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_DropPartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_DropPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyID, task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestHasPartitionTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestHasPartitionTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &hasPartitionTask{
		Condition: NewTaskCondition(ctx),
		HasPartitionRequest: &milvuspb.HasPartitionRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_HasPartition,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionName:  partitionName,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_HasPartition, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyID, task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)
}

func TestShowPartitionsTask(t *testing.T) {
	Params.Init()
	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()
	ctx := context.Background()
	prefix := "TestShowPartitionsTask"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	partitionName := prefix + funcutil.GenRandomStr()

	task := &showPartitionsTask{
		Condition: NewTaskCondition(ctx),
		ShowPartitionsRequest: &milvuspb.ShowPartitionsRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_ShowPartitions,
				MsgID:     100,
				Timestamp: 100,
			},
			DbName:         dbName,
			CollectionName: collectionName,
			PartitionNames: []string{partitionName},
			Type:           milvuspb.ShowType_All,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
	}
	task.PreExecute(ctx)

	assert.Equal(t, commonpb.MsgType_ShowPartitions, task.Type())
	assert.Equal(t, UniqueID(100), task.ID())
	assert.Equal(t, Timestamp(100), task.BeginTs())
	assert.Equal(t, Timestamp(100), task.EndTs())
	assert.Equal(t, Params.ProxyID, task.GetBase().GetSourceID())
	err := task.Execute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = "#0xc0de"
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	task.PartitionNames = []string{"#0xc0de"}
	err = task.PreExecute(ctx)
	assert.NotNil(t, err)

	task.CollectionName = collectionName
	task.PartitionNames = []string{partitionName}
	task.ShowPartitionsRequest.Type = milvuspb.ShowType_InMemory
	err = task.Execute(ctx)
	assert.NotNil(t, err)

}

func TestSearchTask_all(t *testing.T) {
	var err error

	Params.Init()
	Params.SearchResultChannelNames = []string{funcutil.GenRandomStr()}

	rc := NewRootCoordMock()
	rc.Start()
	defer rc.Stop()

	ctx := context.Background()

	err = InitMetaCache(rc)
	assert.NoError(t, err)

	shardsNum := int32(2)
	prefix := "TestSearchTask_all"
	dbName := ""
	collectionName := prefix + funcutil.GenRandomStr()
	int64Field := "int64"
	floatVecField := "fvec"
	dim := 128
	expr := fmt.Sprintf("%s > 0", int64Field)
	nq := 10
	topk := 10
	nprobe := 10

	schema := constructCollectionSchema(int64Field, floatVecField, dim, collectionName)
	marshaledSchema, err := proto.Marshal(schema)
	assert.NoError(t, err)

	createColT := &createCollectionTask{
		Condition: NewTaskCondition(ctx),
		CreateCollectionRequest: &milvuspb.CreateCollectionRequest{
			Base:           nil,
			DbName:         dbName,
			CollectionName: collectionName,
			Schema:         marshaledSchema,
			ShardsNum:      shardsNum,
		},
		ctx:       ctx,
		rootCoord: rc,
		result:    nil,
		schema:    nil,
	}

	assert.NoError(t, createColT.OnEnqueue())
	assert.NoError(t, createColT.PreExecute(ctx))
	assert.NoError(t, createColT.Execute(ctx))
	assert.NoError(t, createColT.PostExecute(ctx))

	dmlChannelsFunc := getDmlChannelsFunc(ctx, rc)
	query := newMockGetChannelsService()
	factory := newSimpleMockMsgStreamFactory()
	chMgr := newChannelsMgrImpl(dmlChannelsFunc, nil, query.GetChannels, nil, factory)
	defer chMgr.removeAllDMLStream()
	defer chMgr.removeAllDQLStream()

	collectionID, err := globalMetaCache.GetCollectionID(ctx, collectionName)
	assert.NoError(t, err)

	qc := newMockQueryCoordShowCollectionsInterface()
	qc.addCollection(collectionID, 100)

	req := constructSearchRequest(dbName, collectionName,
		expr,
		floatVecField,
		nq, dim, nprobe, topk)

	task := &searchTask{
		Condition: NewTaskCondition(ctx),
		SearchRequest: &internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     0,
				Timestamp: 0,
				SourceID:  Params.ProxyID,
			},
			ResultChannelID:    strconv.FormatInt(Params.ProxyID, 10),
			DbID:               0,
			CollectionID:       0,
			PartitionIDs:       nil,
			Dsl:                "",
			PlaceholderGroup:   nil,
			DslType:            0,
			SerializedExprPlan: nil,
			OutputFieldsId:     nil,
			TravelTimestamp:    0,
			GuaranteeTimestamp: 0,
		},
		ctx:       ctx,
		resultBuf: make(chan []*internalpb.SearchResults),
		result:    nil,
		query:     req,
		chMgr:     chMgr,
		qc:        qc,
	}

	// simple mock for query node
	// TODO(dragondriver): should we replace this mock using RocksMq or MemMsgStream?

	err = chMgr.createDQLStream(collectionID)
	assert.NoError(t, err)
	stream, err := chMgr.getDQLStream(collectionID)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	consumeCtx, cancel := context.WithCancel(ctx)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-consumeCtx.Done():
				return
			case pack := <-stream.Chan():
				for _, msg := range pack.Msgs {
					_, ok := msg.(*msgstream.SearchMsg)
					assert.True(t, ok)
					// TODO(dragondriver): construct result according to the request

					constructSearchResulstData := func() *schemapb.SearchResultData {
						resultData := &schemapb.SearchResultData{
							NumQueries: int64(nq),
							TopK:       int64(topk),
							FieldsData: nil,
							Scores:     make([]float32, nq*topk),
							Ids: &schemapb.IDs{
								IdField: &schemapb.IDs_IntId{
									IntId: &schemapb.LongArray{
										Data: make([]int64, nq*topk),
									},
								},
							},
							Topks: make([]int64, nq),
						}

						// ids := make([]int64, topk)
						// for i := 0; i < topk; i++ {
						// 	ids[i] = int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
						// }

						for i := 0; i < nq; i++ {
							for j := 0; j < topk; j++ {
								offset := i*topk + j
								score := rand.Float32()
								id := int64(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
								resultData.Scores[offset] = score
								resultData.Ids.IdField.(*schemapb.IDs_IntId).IntId.Data[offset] = id
							}
							resultData.Topks[i] = int64(topk)
						}

						return resultData
					}

					result1 := &internalpb.SearchResults{
						Base: &commonpb.MsgBase{
							MsgType:   commonpb.MsgType_SearchResult,
							MsgID:     0,
							Timestamp: 0,
							SourceID:  0,
						},
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
							Reason:    "",
						},
						ResultChannelID:          "",
						MetricType:               distance.L2,
						NumQueries:               int64(nq),
						TopK:                     int64(topk),
						SealedSegmentIDsSearched: nil,
						ChannelIDsSearched:       nil,
						GlobalSealedSegmentIDs:   nil,
						SlicedBlob:               nil,
						SlicedNumCount:           1,
						SlicedOffset:             0,
					}
					resultData := constructSearchResulstData()
					sliceBlob, err := proto.Marshal(resultData)
					assert.NoError(t, err)
					result1.SlicedBlob = sliceBlob

					// send search result
					task.resultBuf <- []*internalpb.SearchResults{result1}
				}
			}
		}
	}()

	assert.NoError(t, task.OnEnqueue())
	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))

	cancel()
	wg.Wait()
}
