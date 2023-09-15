// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importutil

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/stretchr/testify/assert"
)

type mockCSVRowConsumer struct {
	handleErr   error
	rows        []map[storage.FieldID]string
	handleCount int
}

func (v *mockCSVRowConsumer) Handle(rows []map[storage.FieldID]string) error {
	if v.handleErr != nil {
		return v.handleErr
	}
	if rows != nil {
		v.rows = append(v.rows, rows...)
	}
	v.handleCount++
	return nil
}

func Test_CSVParserAdjustBufSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	parser, err := NewCSVParser(ctx, collectionInfo, nil)
	assert.NoError(t, err)
	assert.NotNil(t, parser)
	assert.Greater(t, parser.bufRowCount, 0)
	// huge row
	schema.Fields[9].TypeParams = []*commonpb.KeyValuePair{
		{Key: common.DimKey, Value: "32768"},
	}
	parser, err = NewCSVParser(ctx, collectionInfo, nil)
	assert.NoError(t, err)
	assert.NotNil(t, parser)
	assert.Greater(t, parser.bufRowCount, 0)
}

// func Test_CSVParserTypeInference(t *testing.T) {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	defer cancel()

// 	schema := sampleSchema()
// 	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
// 	assert.NoError(t, err)
// 	parser, err := NewCSVParser(ctx, collectionInfo, nil)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, parser)

// 	dynamicValues := map[string]string{
// 		"number": "1234",
// 		"string": "abcd",
// 		"array":  "[1,2,3,4]",
// 		"object": `{"x": 2}`,
// 	}
// 	// DynamicDataType is nil
// 	err = parser.typeInference(dynamicValues)
// 	assert.Error(t, err)

// 	parser.dynamicDataType = make(map[string]DynamicDataType)
// 	err = parser.typeInference(dynamicValues)
// 	assert.NoError(t, err)
// 	assert.Equal(t, parser.dynamicDataType["number"], NumberType)
// 	assert.Equal(t, parser.dynamicDataType["string"], StringType)
// 	assert.Equal(t, parser.dynamicDataType["array"], ArrayType)
// 	assert.Equal(t, parser.dynamicDataType["object"], JSONType)

// }

func Test_CSVParserParseRows_IntPK(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	parser, err := NewCSVParser(ctx, collectionInfo, nil)
	assert.NoError(t, err)
	assert.NotNil(t, parser)

	consumer := &mockCSVRowConsumer{
		handleErr:   nil,
		rows:        make([]map[int64]string, 0),
		handleCount: 0,
	}

	reader := strings.NewReader(
		`FieldBool,FieldInt8,FieldInt16,FieldInt32,FieldInt64,FieldFloat,FieldDouble,FieldString,FieldJSON,FieldBinaryVector,FieldFloatVector
		true,10,101,1001,10001,3.14,1.56,No.0,"{""x"": 0}","[200,0]","[0.1,0.2,0.3,0.4]"`)

	t.Run("parse success", func(t *testing.T) {
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NoError(t, err)

		// empty file
		reader = strings.NewReader(``)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(0)}, consumer)
		assert.NoError(t, err)

		// only have headers no value row
		reader = strings.NewReader(`FieldBool,FieldInt8,FieldInt16,FieldInt32,FieldInt64,FieldFloat,FieldDouble,FieldString,FieldJSON,FieldBinaryVector,FieldFloatVector`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NoError(t, err)
	})

	t.Run("error cases", func(t *testing.T) {
		// handler is nil

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(0)}, nil)
		assert.Error(t, err)

		// csv parse error, fields len error
		reader := strings.NewReader(
			`FieldBool,FieldInt8,FieldInt16,FieldInt32,FieldInt64,FieldFloat,FieldDouble,FieldString,FieldJSON,FieldBinaryVector,FieldFloatVector
		0,100,1000,99999999999999999,3,1,No.0,"{""x"": 0}","[200,0]","[0.1,0.2,0.3,0.4]"`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.Error(t, err)

		// redundant field
		reader = strings.NewReader(
			`dummy,FieldBool,FieldInt8,FieldInt16,FieldInt32,FieldInt64,FieldFloat,FieldDouble,FieldString,FieldJSON,FieldBinaryVector,FieldFloatVector
		1,true,0,100,1000,99999999999999999,3,1,No.0,"{""x"": 0}","[200,0]","[0.1,0.2,0.3,0.4]"`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.Error(t, err)

		// field missed
		reader = strings.NewReader(
			`FieldInt8,FieldInt16,FieldInt32,FieldInt64,FieldFloat,FieldDouble,FieldString,FieldJSON,FieldBinaryVector,FieldFloatVector
		0,100,1000,99999999999999999,3,1,No.0,"{""x"": 0}","[200,0]","[0.1,0.2,0.3,0.4]"`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.Error(t, err)

		// handle() error
		content := `FieldBool,FieldInt8,FieldInt16,FieldInt32,FieldInt64,FieldFloat,FieldDouble,FieldString,FieldJSON,FieldBinaryVector,FieldFloatVector
		true,0,100,1000,99999999999999999,3,1,No.0,"{""x"": 0}","[200,0]","[0.1,0.2,0.3,0.4]"`
		consumer.handleErr = errors.New("error")
		reader = strings.NewReader(content)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.Error(t, err)

		// canceled
		consumer.handleErr = nil
		cancel()
		reader = strings.NewReader(content)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.Error(t, err)

	})
}

func Test_CSVParserCombineDynamicRow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := &schemapb.CollectionSchema{
		Name:               "schema",
		Description:        "schema",
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      106,
				Name:         "FieldID",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      113,
				Name:         "FieldDynamic",
				IsPrimaryKey: false,
				IsDynamic:    true,
				Description:  "dynamic field",
				DataType:     schemapb.DataType_JSON,
			},
		},
	}

	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	parser, err := NewCSVParser(ctx, collectionInfo, nil)
	assert.NoError(t, err)
	assert.NotNil(t, parser)

	// valid input:
	//        id,vector,x,$meta        id,vector,$meta
	// case1: 1,"[]",8,"{""y"": 8}" ==>> 1,"[]","{""y"": 8, ""x"": 8}"
	// case2: 1,"[]",8,"{}"       ==>> 1,"[]","{""x"": 8}"
	// case3: 1,"[]",,"{""x"": 8}"
	// case4: 1,"[]",8,            ==>> 1,"[]","{""x"": 8}"
	// case5: 1,"[]",,

	t.Run("value combined for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]string{
			"x": "88",
		}
		row := map[storage.FieldID]string{
			106: "1",
			113: `{"y": 8}`,
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row[113], "x")
		assert.Contains(t, row[113], "y")

		row = map[storage.FieldID]string{
			106: "1",
			113: `{}`,
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row[113], "x")
	})

	t.Run("JSON format string/object for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]string{}
		row := map[storage.FieldID]string{
			106: "1",
			113: `{"x": 8}`,
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
	})

	t.Run("dynamic field is hidden", func(t *testing.T) {
		dynamicValues := map[string]string{
			"x": "8",
		}
		row := map[storage.FieldID]string{
			106: "1",
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row[113], "x")
	})

	t.Run("no values for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]string{}
		row := map[storage.FieldID]string{
			106: "1",
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
		assert.Equal(t, "{}", row[113])
	})

	t.Run("invalid input for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]string{
			"x": "8",
		}
		row := map[storage.FieldID]string{
			106: "1",
			113: "5",
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.Error(t, err)

		row = map[storage.FieldID]string{
			106: "1",
			113: "abc",
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.Error(t, err)
	})

	t.Run("conflict input for dynamic field", func(t *testing.T) {
		// parser.dynamicDataType = map[string]DynamicDataType{
		// 	"x": NumberType,
		// 	"y": StringType,
		// 	"z": ArrayType,
		// 	"k": BoolType,
		// }

		dynamicValues := map[string]string{
			"x": "[1,2,3]",
			"y": "qwe",
			"z": "[1, 2, 3]",
			"k": "true",
		}
		row := map[storage.FieldID]string{
			106: "1",
		}
		parser.combineDynamicRow(dynamicValues, row)

		dynamicValues = map[string]string{
			"x": "12",
			"y": "qwe",
			"z": "2",
			"k": "true",
		}
		parser.combineDynamicRow(dynamicValues, row)

		dynamicValues = map[string]string{
			"x": "12",
			"y": "234",
			"z": "[1, 2, 3]",
			"k": "3",
		}
		parser.combineDynamicRow(dynamicValues, row)

	})

	t.Run("not allow dynamic values if no dynamic field", func(t *testing.T) {
		parser.collectionInfo.DynamicField = nil
		dynamicValues := map[string]string{
			"x": "8",
		}
		row := map[storage.FieldID]string{
			106: "1",
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.NotContains(t, row, int64(113))
	})
}

func Test_CSVParserVerifyRow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := &schemapb.CollectionSchema{
		Name:               "schema",
		Description:        "schema",
		EnableDynamicField: true,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      106,
				Name:         "FieldID",
				IsPrimaryKey: true,
				AutoID:       false,
				Description:  "int64",
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      113,
				Name:         "FieldDynamic",
				IsPrimaryKey: false,
				IsDynamic:    true,
				Description:  "dynamic field",
				DataType:     schemapb.DataType_JSON,
			},
		},
	}

	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	parser, err := NewCSVParser(ctx, collectionInfo, nil)
	assert.NoError(t, err)
	assert.NotNil(t, parser)

	t.Run("not auto-id, dynamic field provided", func(t *testing.T) {
		parser.fieldsName = []string{"FieldID", "FieldDynamic", "y"}
		raw := []string{"1", `{"x": 8}`, "true"}
		row, err := parser.verifyRow(raw)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(106))
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row[113], "x")
		assert.Contains(t, row[113], "y")
	})

	t.Run("not auto-id, dynamic field not provided", func(t *testing.T) {
		parser.fieldsName = []string{"FieldID"}
		raw := []string{"1"}
		row, err := parser.verifyRow(raw)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(106))
		assert.Contains(t, row, int64(113))
		assert.Contains(t, "{}", row[113])
	})

	t.Run("not auto-id, invalid input dynamic field", func(t *testing.T) {
		parser.fieldsName = []string{"FieldID", "FieldDynamic", "y"}
		raw := []string{"1", "true", "true"}
		_, err = parser.verifyRow(raw)
		assert.Error(t, err)
	})

	schema.Fields[0].AutoID = true
	err = collectionInfo.resetSchema(schema)
	assert.NoError(t, err)
	t.Run("no need to provide value for auto-id", func(t *testing.T) {
		parser.fieldsName = []string{"FieldID", "FieldDynamic", "y"}
		raw := []string{"1", `{"x": 8}`, "true"}
		_, err := parser.verifyRow(raw)
		assert.Error(t, err)

		parser.fieldsName = []string{"FieldDynamic", "y"}
		raw = []string{`{"x": 8}`, "true"}
		row, err := parser.verifyRow(raw)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
	})

	schema.Fields[1].IsDynamic = false
	err = collectionInfo.resetSchema(schema)
	assert.NoError(t, err)
	t.Run("auto id, no dynamic field", func(t *testing.T) {
		parser.fieldsName = []string{"FieldDynamic", "y"}
		raw := []string{`{"x": 8}`, "true"}
		_, err := parser.verifyRow(raw)
		assert.Error(t, err)

		// miss FieldDynamic
		parser.fieldsName = []string{}
		raw = []string{}
		_, err = parser.verifyRow(raw)
		assert.Error(t, err)
	})
}
