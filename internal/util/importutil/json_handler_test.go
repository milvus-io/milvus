package importutil

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/rootcoordpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

type mockIDAllocator struct {
}

func (tso *mockIDAllocator) AllocID(ctx context.Context, req *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
	return &rootcoordpb.AllocIDResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		ID:    int64(1),
		Count: req.Count,
	}, nil
}

func newIDAllocator(ctx context.Context, t *testing.T) *allocator.IDAllocator {
	mockIDAllocator := &mockIDAllocator{}

	idAllocator, err := allocator.NewIDAllocator(ctx, mockIDAllocator, int64(1))
	assert.Nil(t, err)
	err = idAllocator.Start()
	assert.Nil(t, err)

	return idAllocator
}

func Test_GetFieldDimension(t *testing.T) {
	schema := &schemapb.FieldSchema{
		FieldID:      111,
		Name:         "field_float_vector",
		IsPrimaryKey: false,
		Description:  "float_vector",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}

	dim, err := getFieldDimension(schema)
	assert.Nil(t, err)
	assert.Equal(t, 4, dim)

	schema.TypeParams = []*commonpb.KeyValuePair{
		{Key: "dim", Value: "abc"},
	}
	dim, err = getFieldDimension(schema)
	assert.NotNil(t, err)
	assert.Equal(t, 0, dim)

	schema.TypeParams = []*commonpb.KeyValuePair{}
	dim, err = getFieldDimension(schema)
	assert.NotNil(t, err)
	assert.Equal(t, 0, dim)
}

func Test_InitValidators(t *testing.T) {
	validators := make(map[storage.FieldID]*Validator)
	err := initValidators(nil, validators)
	assert.NotNil(t, err)

	schema := sampleSchema()
	// success case
	err = initValidators(schema, validators)
	assert.Nil(t, err)
	assert.Equal(t, len(schema.Fields), len(validators))
	name2ID := make(map[string]storage.FieldID)
	for _, field := range schema.Fields {
		name2ID[field.GetName()] = field.GetFieldID()
	}

	checkFunc := func(funcName string, validVal interface{}, invalidVal interface{}) {
		id := name2ID[funcName]
		v, ok := validators[id]
		assert.True(t, ok)
		err = v.validateFunc(validVal)
		assert.Nil(t, err)
		err = v.validateFunc(invalidVal)
		assert.NotNil(t, err)
	}

	// validate functions
	var validVal interface{} = true
	var invalidVal interface{} = "aa"
	checkFunc("field_bool", validVal, invalidVal)

	validVal = float64(100)
	invalidVal = "aa"
	checkFunc("field_int8", validVal, invalidVal)
	checkFunc("field_int16", validVal, invalidVal)
	checkFunc("field_int32", validVal, invalidVal)
	checkFunc("field_int64", validVal, invalidVal)
	checkFunc("field_float", validVal, invalidVal)
	checkFunc("field_double", validVal, invalidVal)

	validVal = "aa"
	invalidVal = 100
	checkFunc("field_string", validVal, invalidVal)

	validVal = []interface{}{float64(100), float64(101)}
	invalidVal = "aa"
	checkFunc("field_binary_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(100)}
	checkFunc("field_binary_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(100), float64(101), float64(102)}
	checkFunc("field_binary_vector", validVal, invalidVal)
	invalidVal = []interface{}{true, true}
	checkFunc("field_binary_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(255), float64(-1)}
	checkFunc("field_binary_vector", validVal, invalidVal)

	validVal = []interface{}{float64(1), float64(2), float64(3), float64(4)}
	invalidVal = true
	checkFunc("field_float_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(1), float64(2), float64(3)}
	checkFunc("field_float_vector", validVal, invalidVal)
	invalidVal = []interface{}{float64(1), float64(2), float64(3), float64(4), float64(5)}
	checkFunc("field_float_vector", validVal, invalidVal)
	invalidVal = []interface{}{"a", "b", "c", "d"}
	checkFunc("field_float_vector", validVal, invalidVal)

	// error cases
	schema = &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields:      make([]*schemapb.FieldSchema, 0),
	}
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      111,
		Name:         "field_float_vector",
		IsPrimaryKey: false,
		Description:  "float_vector",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "aa"},
		},
	})

	validators = make(map[storage.FieldID]*Validator)
	err = initValidators(schema, validators)
	assert.NotNil(t, err)

	schema.Fields = make([]*schemapb.FieldSchema, 0)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
		FieldID:      110,
		Name:         "field_binary_vector",
		IsPrimaryKey: false,
		Description:  "float_vector",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "aa"},
		},
	})

	err = initValidators(schema, validators)
	assert.NotNil(t, err)
}

func Test_JSONRowValidator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)

	// 0 row case
	reader := strings.NewReader(`{
		"rows":[]
	}`)

	validator := NewJSONRowValidator(schema, nil)
	err := parser.ParseRows(reader, validator)
	assert.Nil(t, err)
	assert.Equal(t, int64(0), validator.ValidateCount())

	// // missed some fields
	// reader = strings.NewReader(`{
	// 	"rows":[
	// 		{"field_bool": true, "field_int8": 10, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
	// 		{"field_bool": true, "field_int8": 10, "field_int16": 101, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]}
	// 	]
	// }`)
	// err = parser.ParseRows(reader, validator)
	// assert.NotNil(t, err)

	// invalid dimension
	reader = strings.NewReader(`{
		"rows":[
			{"field_bool": true, "field_int8": true, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0, 1, 66, 128, 0, 1, 66], "field_float_vector": [1.1, 1.2, 1.3, 1.4]}
		]
	}`)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	// invalid value type
	reader = strings.NewReader(`{
		"rows":[
			{"field_bool": true, "field_int8": true, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]}
		]
	}`)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	// init failed
	validator.validators = nil
	err = validator.Handle(nil)
	assert.NotNil(t, err)
}

func Test_JSONColumnValidator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)

	// 0 row case
	reader := strings.NewReader(`{
		"field_bool": [],
		"field_int8": [],
		"field_int16": [],
		"field_int32": [],
		"field_int64": [],
		"field_float": [],
		"field_double": [],
		"field_string": [],
		"field_binary_vector": [],
		"field_float_vector": []
	}`)

	validator := NewJSONColumnValidator(schema, nil)
	err := parser.ParseColumns(reader, validator)
	assert.Nil(t, err)
	for _, count := range validator.rowCounter {
		assert.Equal(t, int64(0), count)
	}

	// different row count
	reader = strings.NewReader(`{
		"field_bool": [true],
		"field_int8": [],
		"field_int16": [],
		"field_int32": [1, 2, 3],
		"field_int64": [],
		"field_float": [],
		"field_double": [],
		"field_string": [],
		"field_binary_vector": [],
		"field_float_vector": []
	}`)

	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)

	// invalid value type
	reader = strings.NewReader(`{
		"dummy": [],
		"field_bool": [true],
		"field_int8": [1],
		"field_int16": [2],
		"field_int32": [3],
		"field_int64": [4],
		"field_float": [1],
		"field_double": [1],
		"field_string": [9],
		"field_binary_vector": [[254, 1]],
		"field_float_vector": [[1.1, 1.2, 1.3, 1.4]]
	}`)

	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)

	// init failed
	validator.validators = nil
	err = validator.Handle(nil)
	assert.NotNil(t, err)
}

func Test_JSONRowConsumer(t *testing.T) {
	ctx := context.Background()
	idAllocator := newIDAllocator(ctx, t)

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)

	reader := strings.NewReader(`{
		"rows":[
			{"field_bool": true, "field_int8": 10, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
			{"field_bool": false, "field_int8": 11, "field_int16": 102, "field_int32": 1002, "field_int64": 10002, "field_float": 3.15, "field_double": 2.56, "field_string": "hello world", "field_binary_vector": [253, 0], "field_float_vector": [2.1, 2.2, 2.3, 2.4]},
			{"field_bool": true, "field_int8": 12, "field_int16": 103, "field_int32": 1003, "field_int64": 10003, "field_float": 3.16, "field_double": 3.56, "field_string": "hello world", "field_binary_vector": [252, 0], "field_float_vector": [3.1, 3.2, 3.3, 3.4]},
			{"field_bool": false, "field_int8": 13, "field_int16": 104, "field_int32": 1004, "field_int64": 10004, "field_float": 3.17, "field_double": 4.56, "field_string": "hello world", "field_binary_vector": [251, 0], "field_float_vector": [4.1, 4.2, 4.3, 4.4]},
			{"field_bool": true, "field_int8": 14, "field_int16": 105, "field_int32": 1005, "field_int64": 10005, "field_float": 3.18, "field_double": 5.56, "field_string": "hello world", "field_binary_vector": [250, 0], "field_float_vector": [5.1, 5.2, 5.3, 5.4]}
		]
	}`)

	var callTime int32
	var totalCount int
	consumeFunc := func(fields map[storage.FieldID]storage.FieldData) error {
		callTime++
		rowCount := 0
		for _, data := range fields {
			if rowCount == 0 {
				rowCount = data.RowNum()
			} else {
				assert.Equal(t, rowCount, data.RowNum())
			}
		}
		totalCount += rowCount
		return nil
	}

	var shardNum int32 = 2
	consumer := NewJSONRowConsumer(schema, idAllocator, shardNum, 1, consumeFunc)
	assert.NotNil(t, consumer)

	validator := NewJSONRowValidator(schema, consumer)
	err := parser.ParseRows(reader, validator)
	assert.Nil(t, err)
	assert.Equal(t, int64(5), validator.ValidateCount())

	assert.Equal(t, shardNum, callTime)
	assert.Equal(t, 5, totalCount)
}

func Test_JSONColumnConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)

	reader := strings.NewReader(`{
		"field_bool": [true, false, true, true, true],
		"field_int8": [10, 11, 12, 13, 14],
		"field_int16": [100, 101, 102, 103, 104],
		"field_int32": [1000, 1001, 1002, 1003, 1004],
		"field_int64": [10000, 10001, 10002, 10003, 10004],
		"field_float": [3.14, 3.15, 3.16, 3.17, 3.18],
		"field_double": [5.1, 5.2, 5.3, 5.4, 5.5],
		"field_string": ["a", "b", "c", "d", "e"],
		"field_binary_vector": [
			[254, 1],
			[253, 2],
			[252, 3],
			[251, 4],
			[250, 5]
		],
		"field_float_vector": [
			[1.1, 1.2, 1.3, 1.4],
			[2.1, 2.2, 2.3, 2.4],
			[3.1, 3.2, 3.3, 3.4],
			[4.1, 4.2, 4.3, 4.4],
			[5.1, 5.2, 5.3, 5.4]
		]
	}`)

	callTime := 0
	rowCount := 0
	consumeFunc := func(fields map[storage.FieldID]storage.FieldData) error {
		callTime++
		for _, data := range fields {
			if rowCount == 0 {
				rowCount = data.RowNum()
			} else {
				assert.Equal(t, rowCount, data.RowNum())
			}
		}
		return nil
	}

	consumer := NewJSONColumnConsumer(schema, consumeFunc)
	assert.NotNil(t, consumer)

	validator := NewJSONColumnValidator(schema, consumer)
	err := parser.ParseColumns(reader, validator)
	assert.Nil(t, err)
	for _, count := range validator.ValidateCount() {
		assert.Equal(t, int64(5), count)
	}

	assert.Equal(t, 1, callTime)
	assert.Equal(t, 5, rowCount)
}
