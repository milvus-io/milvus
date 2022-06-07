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

func strKeySchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      101,
				Name:         "uid",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "uid",
				DataType:     schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "max_length", Value: "1024"},
				},
			},
			{
				FieldID:      102,
				Name:         "int_scalar",
				IsPrimaryKey: false,
				Description:  "int_scalar",
				DataType:     schemapb.DataType_Int32,
			},
			{
				FieldID:      103,
				Name:         "float_scalar",
				IsPrimaryKey: false,
				Description:  "float_scalar",
				DataType:     schemapb.DataType_Float,
			},
			{
				FieldID:      104,
				Name:         "string_scalar",
				IsPrimaryKey: false,
				Description:  "string_scalar",
				DataType:     schemapb.DataType_VarChar,
			},
			{
				FieldID:      105,
				Name:         "bool_scalar",
				IsPrimaryKey: false,
				Description:  "bool_scalar",
				DataType:     schemapb.DataType_Bool,
			},
			{
				FieldID:      106,
				Name:         "vectors",
				IsPrimaryKey: false,
				Description:  "vectors",
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

	reader = strings.NewReader(`{}`)
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
		"field_int8": [10, 11, 12, 13, 14],
		"dummy":[1, 2, 3]
	}`)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.Nil(t, err)

	reader = strings.NewReader(`{
		"dummy":[1, 2, 3]
	}`)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)

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

	reader = strings.NewReader(`{}`)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)

	reader = strings.NewReader(``)
	validator = NewJSONColumnValidator(schema, nil)
	err = parser.ParseColumns(reader, validator)
	assert.NotNil(t, err)
}

func Test_ParserRowsStringKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := strKeySchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)
	parser.bufSize = 1

	reader := strings.NewReader(`{
		"rows": [{
				"uid": "Dm4aWrbNzhmjwCTEnCJ9LDPO2N09sqysxgVfbH9Zmn3nBzmwsmk0eZN6x7wSAoPQ",
				"int_scalar": 9070353,
				"float_scalar": 0.9798043638085004,
				"string_scalar": "ShQ44OX0z8kGpRPhaXmfSsdH7JHq5DsZzu0e2umS1hrWG0uONH2RIIAdOECaaXir",
				"bool_scalar": true,
				"vectors": [0.5040062902126952, 0.8297619818664708, 0.20248342801564806, 0.12834786423659314]
			},
			{
				"uid": "RP50U0d2napRjXu94a8oGikWgklvVsXFurp8RR4tHGw7N0gk1b7opm59k3FCpyPb",
				"int_scalar": 8505288,
				"float_scalar": 0.937913432198687,
				"string_scalar": "Ld4b0avxathBdNvCrtm3QsWO1pYktUVR7WgAtrtozIwrA8vpeactNhJ85CFGQnK5",
				"bool_scalar": false,
				"vectors": [0.528232122836893, 0.6916116750653186, 0.41443762522548705, 0.26624344144792056]
			},
			{
				"uid": "oxhFkQitWPPw0Bjmj7UQcn4iwvS0CU7RLAC81uQFFQjWtOdiB329CPyWkfGSeYfE",
				"int_scalar": 4392660,
				"float_scalar": 0.32381232630490264,
				"string_scalar": "EmAlB0xdQcxeBtwlZJQnLgKodiuRinynoQtg0eXrjkq24dQohzSm7Bx3zquHd3kO",
				"bool_scalar": false,
				"vectors": [0.7978693027281338, 0.12394906726785092, 0.42431962903815285, 0.4098707807351914]
			},
			{
				"uid": "sxoEL4Mpk1LdsyXhbNm059UWJ3CvxURLCQczaVI5xtBD4QcVWTDFUW7dBdye6nbn",
				"int_scalar": 7927425,
				"float_scalar": 0.31074026464844895,
				"string_scalar": "fdY2beCvs1wSws0Gb9ySD92xwfEfJpX5DQgsWoISylBAoYOcXpRaqIJoXYS4g269",
				"bool_scalar": true,
				"vectors": [0.3716157812069954, 0.006981281113265229, 0.9007003458552365, 0.22492634316191004]
			},
			{
				"uid": "g33Rqq2UQSHPRHw5FvuXxf5uGEhIAetxE6UuXXCJj0hafG8WuJr1ueZftsySCqAd",
				"int_scalar": 9288807,
				"float_scalar": 0.4953578200336135,
				"string_scalar": "6f8Iv1zQAGksj5XxMbbI5evTrYrB8fSFQ58jl0oU7Z4BpA81VsD2tlWqkhfoBNa7",
				"bool_scalar": false,
				"vectors": [0.5921374209648096, 0.04234832587925662, 0.7803878096531548, 0.1964045837884633]
			},
			{
				"uid": "ACIJd7lTXkRgUNmlQk6AbnWIKEEV8Z6OS3vDcm0w9psmt9sH3z1JLg1fNVCqiX3d",
				"int_scalar": 1173595,
				"float_scalar": 0.9000745450802002,
				"string_scalar": "gpj9YctF2ig1l1APkvRzHbVE8PZVKRbk7nvW73qS2uQbY5l7MeIeTPwRBjasbY8z",
				"bool_scalar": true,
				"vectors": [0.4655121736168688, 0.6195496905333787, 0.5316616196326639, 0.3417492053890768]
			},
			{
				"uid": "f0wRVZZ9u1bEKrAjLeZj3oliEnUjBiUl6TiermeczceBmGe6M2RHONgz3qEogrd5",
				"int_scalar": 3722368,
				"float_scalar": 0.7212299175768438,
				"string_scalar": "xydiejGUlvS49BfBuy1EuYRKt3v2oKwC6pqy7Ga4dGWn3BnQigV4XAGawixDAGHN",
				"bool_scalar": false,
				"vectors": [0.6173164237304075, 0.374107748459483, 0.3686321416317251, 0.585725336391797]
			},
			{
				"uid": "uXq9q96vUqnDebcUISFkRFT27OjD89DWhok6urXIjTuLzaSWnCVTJkrJXxFctSg0",
				"int_scalar": 1940731,
				"float_scalar": 0.9524404085944204,
				"string_scalar": "ZXSNzR5V3t62fjop7b7DHK56ByAF0INYwycKsu6OxGP4p2j0Obs6l0NUqukypGXd",
				"bool_scalar": false,
				"vectors": [0.07178869784465443, 0.4208459174227864, 0.5882811425075762, 0.6867753592116734]
			},
			{
				"uid": "EXDDklLvQIfeCJN8cES3b9mdCYDQVhq2iLj8WWA3TPtZ1SZ4Jpidj7OXJidSD7Wn",
				"int_scalar": 2158426,
				"float_scalar": 0.23770219927963454,
				"string_scalar": "9TNeKVSMqTP8Zxs90kaAcB7n6JbIcvFWInzi9JxZQgmYxD5xLYwaCoeUzRiNAxAg",
				"bool_scalar": false,
				"vectors": [0.5659468293534021, 0.6275816433340369, 0.3978846871291008, 0.3571179679645908]
			},
			{
				"uid": "mlaXOgYvB88WWRpXNyWv6UqpmvIHrC6pRo03AtaPLMpVymu0L9ioO8GWa1XgGyj0",
				"int_scalar": 198279,
				"float_scalar": 0.020343767010139513,
				"string_scalar": "AblYGRZJiMAwDbMEkungG0yKTeuya4FgyliakWWqSOJ5TvQWB9Ki2WXbnvSsYIDF",
				"bool_scalar": true,
				"vectors": [0.5374636140212398, 0.7655373567912009, 0.05491796821609715, 0.349384366747262]
			}
		]
	}`)

	validator := NewJSONRowValidator(schema, nil)
	err := parser.ParseRows(reader, validator)
	assert.Nil(t, err)
	assert.Equal(t, int64(10), validator.ValidateCount())
}

func Test_ParserColumnsStrKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := strKeySchema()
	parser := NewJSONParser(ctx, schema)
	assert.NotNil(t, parser)
	parser.bufSize = 1

	reader := strings.NewReader(`{
		"uid": ["Dm4aWrbNzhmjwCTEnCJ9LDPO2N09sqysxgVfbH9Zmn3nBzmwsmk0eZN6x7wSAoPQ", "RP50U0d2napRjXu94a8oGikWgklvVsXFurp8RR4tHGw7N0gk1b7opm59k3FCpyPb", "oxhFkQitWPPw0Bjmj7UQcn4iwvS0CU7RLAC81uQFFQjWtOdiB329CPyWkfGSeYfE", "sxoEL4Mpk1LdsyXhbNm059UWJ3CvxURLCQczaVI5xtBD4QcVWTDFUW7dBdye6nbn", "g33Rqq2UQSHPRHw5FvuXxf5uGEhIAetxE6UuXXCJj0hafG8WuJr1ueZftsySCqAd"],
		"int_scalar": [9070353, 8505288, 4392660, 7927425, 9288807],
		"float_scalar": [0.9798043638085004, 0.937913432198687, 0.32381232630490264, 0.31074026464844895, 0.4953578200336135],
		"string_scalar": ["ShQ44OX0z8kGpRPhaXmfSsdH7JHq5DsZzu0e2umS1hrWG0uONH2RIIAdOECaaXir", "Ld4b0avxathBdNvCrtm3QsWO1pYktUVR7WgAtrtozIwrA8vpeactNhJ85CFGQnK5", "EmAlB0xdQcxeBtwlZJQnLgKodiuRinynoQtg0eXrjkq24dQohzSm7Bx3zquHd3kO", "fdY2beCvs1wSws0Gb9ySD92xwfEfJpX5DQgsWoISylBAoYOcXpRaqIJoXYS4g269", "6f8Iv1zQAGksj5XxMbbI5evTrYrB8fSFQ58jl0oU7Z4BpA81VsD2tlWqkhfoBNa7"],
		"bool_scalar": [true, false, true, false, false],
		"vectors": [
			[0.5040062902126952, 0.8297619818664708, 0.20248342801564806, 0.12834786423659314],
			[0.528232122836893, 0.6916116750653186, 0.41443762522548705, 0.26624344144792056],
			[0.7978693027281338, 0.12394906726785092, 0.42431962903815285, 0.4098707807351914],
			[0.3716157812069954, 0.006981281113265229, 0.9007003458552365, 0.22492634316191004],
			[0.5921374209648096, 0.04234832587925662, 0.7803878096531548, 0.1964045837884633]
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
}
