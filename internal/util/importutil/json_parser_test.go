package importutil

import (
	"context"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
)

func sampleSchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      102,
				Name:         "field_bool",
				IsPrimaryKey: false,
				Description:  "bool",
				DataType:     schemapb.DataType_Bool,
			},
			{
				FieldID:      103,
				Name:         "field_int8",
				IsPrimaryKey: false,
				Description:  "int8",
				DataType:     schemapb.DataType_Int8,
			},
			{
				FieldID:      104,
				Name:         "field_int16",
				IsPrimaryKey: false,
				Description:  "int16",
				DataType:     schemapb.DataType_Int16,
			},
			{
				FieldID:      105,
				Name:         "field_int32",
				IsPrimaryKey: false,
				Description:  "int32",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      106,
				Name:         "field_int64",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      107,
				Name:         "field_float",
				IsPrimaryKey: false,
				Description:  "float",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      108,
				Name:         "field_double",
				IsPrimaryKey: false,
				Description:  "double",
				DataType:     schemapb.DataType_Double,
			},
			{
				FieldID:      109,
				Name:         "field_string",
				IsPrimaryKey: false,
				Description:  "string",
				DataType:     schemapb.DataType_String,
			},
			{
				FieldID:      110,
				Name:         "field_binary_vector",
				IsPrimaryKey: false,
				Description:  "binary_vector",
				DataType:     schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "16"},
				},
			},
			{
				FieldID:      111,
				Name:         "field_float_vector",
				IsPrimaryKey: false,
				Description:  "float_vector",
				DataType:     schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
	return schema
}

func Test_ParserRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)
	parser.bufSize = 1

	reader := strings.NewReader(`{
		"rows":[
			{"field_bool": true, "field_int8": 10, "field_int16": 101, "field_int32": 1001, "field_int64": 10001, "field_float": 3.14, "field_double": 1.56, "field_string": "hello world", "field_binary_vector": [254, 0], "field_float_vector": [1.1, 1.2, 1.3, 1.4]},
			{"field_bool": false, "field_int8": 11, "field_int16": 102, "field_int32": 1002, "field_int64": 10002, "field_float": 3.15, "field_double": 2.56, "field_string": "hello world", "field_binary_vector": [253, 0], "field_float_vector": [2.1, 2.2, 2.3, 2.4]},
			{"field_bool": true, "field_int8": 12, "field_int16": 103, "field_int32": 1003, "field_int64": 10003, "field_float": 3.16, "field_double": 3.56, "field_string": "hello world", "field_binary_vector": [252, 0], "field_float_vector": [3.1, 3.2, 3.3, 3.4]},
			{"field_bool": false, "field_int8": 13, "field_int16": 104, "field_int32": 1004, "field_int64": 10004, "field_float": 3.17, "field_double": 4.56, "field_string": "hello world", "field_binary_vector": [251, 0], "field_float_vector": [4.1, 4.2, 4.3, 4.4]},
			{"field_bool": true, "field_int8": 14, "field_int16": 105, "field_int32": 1005, "field_int64": 10005, "field_float": 3.18, "field_double": 5.56, "field_string": "hello world", "field_binary_vector": [250, 0], "field_float_vector": [5.1, 5.2, 5.3, 5.4]}
		]
	}`)

	err := parser.ParseRows(reader, nil)
	assert.NotNil(t, err)

	validator := NewJSONRowValidator(schema, nil)
	err = parser.ParseRows(reader, validator)
	assert.Nil(t, err)
	assert.Equal(t, int64(5), validator.ValidateCount())

	reader = strings.NewReader(`{
		"dummy":[]
	}`)
	validator = NewJSONRowValidator(schema, nil)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(`{
		"rows":
	}`)
	validator = NewJSONRowValidator(schema, nil)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(`{
		"rows": [}
	}`)
	validator = NewJSONRowValidator(schema, nil)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(`{
		"rows": {}
	}`)
	validator = NewJSONRowValidator(schema, nil)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(`{
		"rows": [[]]
	}`)
	validator = NewJSONRowValidator(schema, nil)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(`[]`)
	validator = NewJSONRowValidator(schema, nil)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(``)
	validator = NewJSONRowValidator(schema, nil)
	err = parser.ParseRows(reader, validator)
	assert.NotNil(t, err)
}

func Test_ParserColumns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)
	parser.bufSize = 1

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

	err := parser.ParseColumns(reader, nil)
	assert.NotNil(t, err)

	validator := NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.Nil(t, err)
	counter := validator.ValidateCount()
	for _, v := range counter {
		assert.Equal(t, int64(5), v)
	}

	reader = strings.NewReader(`{
		"dummy":[1, 2, 3]
	}`)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.Nil(t, err)

	reader = strings.NewReader(`{
		"field_bool":
	}`)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(`{
		"field_bool":{}
	}`)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(`{
		"field_bool":[}
	}`)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(`[]`)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(``)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)
}
