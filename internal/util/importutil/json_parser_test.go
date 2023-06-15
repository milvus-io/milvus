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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/stretchr/testify/assert"
)

// mock class of JSONRowCounsumer
type mockJSONRowConsumer struct {
	handleErr   error
	rows        []map[storage.FieldID]interface{}
	handleCount int
}

func (v *mockJSONRowConsumer) Handle(rows []map[storage.FieldID]interface{}) error {
	if v.handleErr != nil {
		return v.handleErr
	}
	if rows != nil {
		v.rows = append(v.rows, rows...)
	}
	v.handleCount++
	return nil
}

func Test_AdjustBufSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// small row
	schema := sampleSchema()
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	parser := NewJSONParser(ctx, collectionInfo, nil)
	assert.NotNil(t, parser)
	assert.Greater(t, parser.bufRowCount, 0)

	// huge row
	schema.Fields[9].TypeParams = []*commonpb.KeyValuePair{
		{Key: common.DimKey, Value: "32768"},
	}
	parser = NewJSONParser(ctx, collectionInfo, nil)
	assert.NotNil(t, parser)
	assert.Greater(t, parser.bufRowCount, 0)

	// no change
	schema = &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{},
	}
	parser = NewJSONParser(ctx, collectionInfo, nil)
	assert.NotNil(t, parser)
	assert.Greater(t, parser.bufRowCount, 0)
	adjustBufSize(parser, schema)

}

func Test_JSONParserParseRows_IntPK(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	parser := NewJSONParser(ctx, collectionInfo, nil)
	assert.NotNil(t, parser)

	// prepare test data
	content := &sampleContent{
		Rows: make([]sampleRow, 0),
	}
	for i := 0; i < 10; i++ {
		row := sampleRow{
			FieldBool:         i%2 == 0,
			FieldInt8:         int8(i % math.MaxInt8),
			FieldInt16:        int16(100 + i),
			FieldInt32:        int32(1000 + i),
			FieldInt64:        int64(99999999999999999 + i),
			FieldFloat:        3 + float32(i)/11,
			FieldDouble:       1 + float64(i)/7,
			FieldString:       "No." + strconv.FormatInt(int64(i), 10),
			FieldJSON:         fmt.Sprintf("{\"x\": %d}", i),
			FieldBinaryVector: []int{(200 + i) % math.MaxUint8, 0},
			FieldFloatVector:  []float32{float32(i) + 0.1, float32(i) + 0.2, float32(i) + 0.3, float32(i) + 0.4},
		}
		content.Rows = append(content.Rows, row)
	}

	binContent, err := json.Marshal(content)
	assert.NoError(t, err)
	strContent := string(binContent)
	reader := strings.NewReader(strContent)

	consumer := &mockJSONRowConsumer{
		handleErr:   nil,
		rows:        make([]map[int64]interface{}, 0),
		handleCount: 0,
	}

	t.Run("parse success", func(t *testing.T) {
		// set bufRowCount = 4, means call handle() after reading 4 rows
		parser.bufRowCount = 4
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(len(strContent))}, consumer)
		assert.NoError(t, err)
		assert.Equal(t, len(content.Rows), len(consumer.rows))
		for i := 0; i < len(consumer.rows); i++ {
			contenctRow := content.Rows[i]
			parsedRow := consumer.rows[i]

			v1, ok := parsedRow[102].(bool)
			assert.True(t, ok)
			assert.Equal(t, contenctRow.FieldBool, v1)

			v2, ok := parsedRow[103].(json.Number)
			assert.True(t, ok)
			assert.Equal(t, strconv.FormatInt(int64(contenctRow.FieldInt8), 10), string(v2))

			v3, ok := parsedRow[104].(json.Number)
			assert.True(t, ok)
			assert.Equal(t, strconv.FormatInt(int64(contenctRow.FieldInt16), 10), string(v3))

			v4, ok := parsedRow[105].(json.Number)
			assert.True(t, ok)
			assert.Equal(t, strconv.FormatInt(int64(contenctRow.FieldInt32), 10), string(v4))

			v5, ok := parsedRow[106].(json.Number)
			assert.True(t, ok)
			assert.Equal(t, strconv.FormatInt(contenctRow.FieldInt64, 10), string(v5))

			v6, ok := parsedRow[107].(json.Number)
			assert.True(t, ok)
			f32, err := parseFloat(string(v6), 32, "")
			assert.NoError(t, err)
			assert.InDelta(t, contenctRow.FieldFloat, float32(f32), 10e-6)

			v7, ok := parsedRow[108].(json.Number)
			assert.True(t, ok)
			f64, err := parseFloat(string(v7), 64, "")
			assert.NoError(t, err)
			assert.InDelta(t, contenctRow.FieldDouble, f64, 10e-14)

			v8, ok := parsedRow[109].(string)
			assert.True(t, ok)
			assert.Equal(t, contenctRow.FieldString, v8)

			v9, ok := parsedRow[110].([]interface{})
			assert.True(t, ok)
			assert.Equal(t, len(contenctRow.FieldBinaryVector), len(v9))
			for k := 0; k < len(v9); k++ {
				val, ok := v9[k].(json.Number)
				assert.True(t, ok)
				assert.Equal(t, strconv.FormatInt(int64(contenctRow.FieldBinaryVector[k]), 10), string(val))
			}

			v10, ok := parsedRow[111].([]interface{})
			assert.True(t, ok)
			assert.Equal(t, len(contenctRow.FieldFloatVector), len(v10))
			for k := 0; k < len(v10); k++ {
				val, ok := v10[k].(json.Number)
				assert.True(t, ok)
				fval, err := parseFloat(string(val), 64, "")
				assert.NoError(t, err)
				assert.InDelta(t, contenctRow.FieldFloatVector[k], float32(fval), 10e-6)
			}
		}

		// empty content
		reader = strings.NewReader(`{}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(2)}, consumer)
		assert.NoError(t, err)

		// row count is 0
		reader = strings.NewReader(`{
					"rows":[]
				}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NoError(t, err)
	})

	t.Run("error cases", func(t *testing.T) {
		// handler is nil
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(0)}, nil)
		assert.Error(t, err)

		// not a valid JSON format
		reader = strings.NewReader(`{[]`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(10)}, consumer)
		assert.Error(t, err)

		// not a row-based format
		reader = strings.NewReader(`{
			"dummy":[]
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(10)}, consumer)
		assert.Error(t, err)

		// rows is not a list
		reader = strings.NewReader(`{
			"rows":
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(5)}, consumer)
		assert.Error(t, err)

		// typo
		reader = strings.NewReader(`{
			"rows": [}
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(6)}, consumer)
		assert.Error(t, err)

		// rows is not a list
		reader = strings.NewReader(`{
			"rows": {}
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(8)}, consumer)
		assert.Error(t, err)

		// rows is not a list of list
		reader = strings.NewReader(`{
			"rows": [[]]
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(10)}, consumer)
		assert.Error(t, err)

		// typo
		reader = strings.NewReader(`{
			"rows": ["]
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(10)}, consumer)
		assert.Error(t, err)

		// not valid json format
		reader = strings.NewReader(`[]`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(2)}, consumer)
		assert.Error(t, err)

		// empty file
		reader = strings.NewReader(``)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(0)}, consumer)
		assert.Error(t, err)

		// redundant field
		reader = strings.NewReader(`{
			"rows":[
				{"dummy": 1, "FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4], "FieldJSON": {"a": 10, "b": true}}
			]
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.Error(t, err)

		// field missed
		reader = strings.NewReader(`{
			"rows":[
				{"FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4], "FieldJSON": {"a": 10, "b": true}}
			]
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.Error(t, err)

		// handle() error
		content := `{
			"rows":[
				{"FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4], "FieldJSON": {"a": 7, "b": true}},
				{"FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4], "FieldJSON": {"a": 8, "b": false}},
				{"FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4], "FieldJSON": {"a": 9, "b": true}}
			]
		}`
		consumer.handleErr = errors.New("error")
		reader = strings.NewReader(content)
		parser.bufRowCount = 2
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.Error(t, err)

		reader = strings.NewReader(content)
		parser.bufRowCount = 5
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

func Test_JSONParserParseRows_StrPK(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := strKeySchema()
	collectionInfo, err := NewCollectionInfo(schema, 2, []int64{1})
	assert.NoError(t, err)
	updateProgress := func(percent int64) {
		assert.Greater(t, percent, int64(0))
	}
	parser := NewJSONParser(ctx, collectionInfo, updateProgress)
	assert.NotNil(t, parser)

	// prepare test data
	content := &strKeyContent{
		Rows: make([]strKeyRow, 0),
	}
	for i := 0; i < 10; i++ {
		row := strKeyRow{
			UID:              "strID_" + strconv.FormatInt(int64(i), 10),
			FieldInt32:       int32(10000 + i),
			FieldFloat:       1 + float32(i)/13,
			FieldString:      strconv.FormatInt(int64(i+1), 10) + " this string contains unicode character: 🎵",
			FieldBool:        i%3 == 0,
			FieldFloatVector: []float32{float32(i) / 2, float32(i) / 3, float32(i) / 6, float32(i) / 9},
		}
		content.Rows = append(content.Rows, row)
	}

	binContent, err := json.Marshal(content)
	assert.NoError(t, err)
	strContent := string(binContent)
	reader := strings.NewReader(strContent)

	consumer := &mockJSONRowConsumer{
		handleErr:   nil,
		rows:        make([]map[int64]interface{}, 0),
		handleCount: 0,
	}

	err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(len(binContent))}, consumer)
	assert.NoError(t, err)
	assert.Equal(t, len(content.Rows), len(consumer.rows))
	for i := 0; i < len(consumer.rows); i++ {
		contenctRow := content.Rows[i]
		parsedRow := consumer.rows[i]

		v1, ok := parsedRow[101].(string)
		assert.True(t, ok)
		assert.Equal(t, contenctRow.UID, v1)

		v2, ok := parsedRow[102].(json.Number)
		assert.True(t, ok)
		assert.Equal(t, strconv.FormatInt(int64(contenctRow.FieldInt32), 10), string(v2))

		v3, ok := parsedRow[103].(json.Number)
		assert.True(t, ok)
		f32, err := parseFloat(string(v3), 32, "")
		assert.NoError(t, err)
		assert.InDelta(t, contenctRow.FieldFloat, float32(f32), 10e-6)

		v4, ok := parsedRow[104].(string)
		assert.True(t, ok)
		assert.Equal(t, contenctRow.FieldString, v4)

		v5, ok := parsedRow[105].(bool)
		assert.True(t, ok)
		assert.Equal(t, contenctRow.FieldBool, v5)

		v6, ok := parsedRow[106].([]interface{})
		assert.True(t, ok)
		assert.Equal(t, len(contenctRow.FieldFloatVector), len(v6))
		for k := 0; k < len(v6); k++ {
			val, ok := v6[k].(json.Number)
			assert.True(t, ok)
			fval, err := parseFloat(string(val), 64, "")
			assert.NoError(t, err)
			assert.InDelta(t, contenctRow.FieldFloatVector[k], float32(fval), 10e-6)
		}
	}
}

func Test_JSONParserCombineDynamicRow(t *testing.T) {
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
	parser := NewJSONParser(ctx, collectionInfo, nil)
	assert.NotNil(t, parser)

	// valid input:
	// case 1: {"id": 1, "vector": [], "x": 8, "$meta": "{\"y\": 8}"}
	// case 2: {"id": 1, "vector": [], "x": 8, "$meta": {}}
	// case 3: {"id": 1, "vector": [], "$meta": "{\"x\": 8}"}
	// case 4: {"id": 1, "vector": [], "$meta": {"x": 8}}
	// case 5: {"id": 1, "vector": [], "$meta": {}}
	// case 6: {"id": 1, "vector": [], "x": 8}
	// case 7: {"id": 1, "vector": []}

	t.Run("values combined for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]interface{}{
			"x": 8,
		}
		row := map[storage.FieldID]interface{}{
			106: 1,
			113: "{\"y\": 8}",
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row[113], "x")
		assert.Contains(t, row[113], "y")
	})

	t.Run("outside value for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]interface{}{
			"x": 8,
		}
		row := map[storage.FieldID]interface{}{
			106: 1,
			113: map[string]interface{}{},
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row[113], "x")
	})

	t.Run("JSON format string/object for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]interface{}{}
		row := map[storage.FieldID]interface{}{
			106: 1,
			113: "{\"x\": 8}",
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
	})

	t.Run("dynamic field is hidden", func(t *testing.T) {
		dynamicValues := map[string]interface{}{
			"x": 8,
		}
		row := map[storage.FieldID]interface{}{
			106: 1,
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row[113], "x")
	})

	t.Run("no values for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]interface{}{}
		row := map[storage.FieldID]interface{}{
			106: 1,
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
		assert.Equal(t, "{}", row[113])
	})

	t.Run("invalid input for dynamic field", func(t *testing.T) {
		dynamicValues := map[string]interface{}{
			"x": 8,
		}
		row := map[storage.FieldID]interface{}{
			106: 1,
			113: 5,
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.Error(t, err)

		row = map[storage.FieldID]interface{}{
			106: 1,
			113: "abc",
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.Error(t, err)
	})

	t.Run("not allow dynamic values if no dynamic field", func(t *testing.T) {
		parser.collectionInfo.DynamicField = nil
		dynamicValues := map[string]interface{}{
			"x": 8,
		}
		row := map[storage.FieldID]interface{}{
			106: 1,
		}
		err = parser.combineDynamicRow(dynamicValues, row)
		assert.NoError(t, err)
		assert.NotContains(t, row, int64(113))
	})
}

func Test_JSONParserVerifyRow(t *testing.T) {
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

	parser := NewJSONParser(ctx, collectionInfo, nil)
	assert.NotNil(t, parser)

	t.Run("input is not key-value map", func(t *testing.T) {
		_, err = parser.verifyRow(nil)
		assert.Error(t, err)

		_, err = parser.verifyRow([]int{0})
		assert.Error(t, err)
	})

	t.Run("not auto-id, dynamic field provided", func(t *testing.T) {
		raw := map[string]interface{}{
			"FieldID":      100,
			"FieldDynamic": "{\"x\": 8}",
			"y":            true,
		}
		row, err := parser.verifyRow(raw)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(106))
		assert.Contains(t, row, int64(113))
		assert.Contains(t, row[113], "x")
		assert.Contains(t, row[113], "y")
	})

	t.Run("not auto-id, dynamic field not provided", func(t *testing.T) {
		raw := map[string]interface{}{
			"FieldID": 100,
		}
		row, err := parser.verifyRow(raw)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(106))
		assert.Contains(t, row, int64(113))
		assert.Equal(t, "{}", row[113])
	})

	t.Run("not auto-id, invalid input dynamic field", func(t *testing.T) {
		raw := map[string]interface{}{
			"FieldID":      100,
			"FieldDynamic": true,
			"y":            true,
		}
		_, err = parser.verifyRow(raw)
		assert.Error(t, err)
	})

	schema.Fields[0].AutoID = true
	err = collectionInfo.resetSchema(schema)
	assert.NoError(t, err)

	t.Run("no need to provide value for auto-id", func(t *testing.T) {
		raw := map[string]interface{}{
			"FieldID":      100,
			"FieldDynamic": "{\"x\": 8}",
			"y":            true,
		}
		_, err := parser.verifyRow(raw)
		assert.Error(t, err)

		raw = map[string]interface{}{
			"FieldDynamic": "{\"x\": 8}",
			"y":            true,
		}
		row, err := parser.verifyRow(raw)
		assert.NoError(t, err)
		assert.Contains(t, row, int64(113))
	})

	schema.Fields[1].IsDynamic = false
	err = collectionInfo.resetSchema(schema)
	assert.NoError(t, err)

	t.Run("auto id, no dynamic field", func(t *testing.T) {
		raw := map[string]interface{}{
			"FieldDynamic": "{\"x\": 8}",
			"y":            true,
		}
		_, err := parser.verifyRow(raw)
		assert.Error(t, err)

		raw = map[string]interface{}{}
		_, err = parser.verifyRow(raw)
		assert.Error(t, err)
	})
}
