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
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
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
	parser := NewJSONParser(ctx, schema, nil)
	assert.NotNil(t, parser)
	assert.Greater(t, parser.bufRowCount, 0)

	// huge row
	schema.Fields[9].TypeParams = []*commonpb.KeyValuePair{
		{Key: "dim", Value: "32768"},
	}
	parser = NewJSONParser(ctx, schema, nil)
	assert.NotNil(t, parser)
	assert.Greater(t, parser.bufRowCount, 0)

	// no change
	schema = &schemapb.CollectionSchema{
		Name:        "schema",
		Description: "schema",
		AutoID:      true,
		Fields:      []*schemapb.FieldSchema{},
	}
	parser = NewJSONParser(ctx, schema, nil)
	assert.NotNil(t, parser)
	assert.Greater(t, parser.bufRowCount, 0)
}

func Test_JSONParserParseRows_IntPK(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := sampleSchema()
	parser := NewJSONParser(ctx, schema, nil)
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
			FieldBinaryVector: []int{(200 + i) % math.MaxUint8, 0},
			FieldFloatVector:  []float32{float32(i) + 0.1, float32(i) + 0.2, float32(i) + 0.3, float32(i) + 0.4},
		}
		content.Rows = append(content.Rows, row)
	}

	binContent, err := json.Marshal(content)
	assert.Nil(t, err)
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
		assert.Nil(t, err)
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
			assert.Nil(t, err)
			assert.InDelta(t, contenctRow.FieldFloat, float32(f32), 10e-6)

			v7, ok := parsedRow[108].(json.Number)
			assert.True(t, ok)
			f64, err := parseFloat(string(v7), 64, "")
			assert.Nil(t, err)
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
				assert.Nil(t, err)
				assert.InDelta(t, contenctRow.FieldFloatVector[k], float32(fval), 10e-6)
			}
		}
	})

	t.Run("error cases", func(t *testing.T) {
		// handler is nil
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(0)}, nil)
		assert.NotNil(t, err)

		// not a row-based format
		reader = strings.NewReader(`{
			"dummy":[]
		}`)

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(10)}, consumer)
		assert.NotNil(t, err)

		// rows is not a list
		reader = strings.NewReader(`{
			"rows":
		}`)

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(5)}, consumer)
		assert.NotNil(t, err)

		// typo
		reader = strings.NewReader(`{
			"rows": [}
		}`)

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(6)}, consumer)
		assert.NotNil(t, err)

		// rows is not a list
		reader = strings.NewReader(`{
			"rows": {}
		}`)

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(8)}, consumer)
		assert.NotNil(t, err)

		// rows is not a list of list
		reader = strings.NewReader(`{
			"rows": [[]]
		}`)

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(10)}, consumer)
		assert.NotNil(t, err)

		// not valid json format
		reader = strings.NewReader(`[]`)

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(2)}, consumer)
		assert.NotNil(t, err)

		// empty content
		reader = strings.NewReader(`{}`)

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(2)}, consumer)
		assert.NotNil(t, err)

		// empty content
		reader = strings.NewReader(``)

		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(0)}, consumer)
		assert.NotNil(t, err)

		// redundant field
		reader = strings.NewReader(`{
			"rows":[
				{"dummy": 1, "FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4]}
			]
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NotNil(t, err)

		// field missed
		reader = strings.NewReader(`{
			"rows":[
				{"FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4]}
			]
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NotNil(t, err)

		// handle() error
		content := `{
			"rows":[
				{"FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4]},
				{"FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4]},
				{"FieldBool": true, "FieldInt8": 10, "FieldInt16": 101, "FieldInt32": 1001, "FieldInt64": 10001, "FieldFloat": 3.14, "FieldDouble": 1.56, "FieldString": "hello world", "FieldBinaryVector": [254, 0], "FieldFloatVector": [1.1, 1.2, 1.3, 1.4]}
			]
		}`
		consumer.handleErr = errors.New("error")
		reader = strings.NewReader(content)
		parser.bufRowCount = 2
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NotNil(t, err)

		reader = strings.NewReader(content)
		parser.bufRowCount = 5
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NotNil(t, err)

		// row count is 0
		reader = strings.NewReader(`{
			"rows":[]
		}`)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NotNil(t, err)

		// canceled
		consumer.handleErr = nil
		cancel()
		reader = strings.NewReader(content)
		err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(100)}, consumer)
		assert.NotNil(t, err)
	})
}

func Test_JSONParserParseRows_StrPK(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	schema := strKeySchema()
	updateProgress := func(percent int64) {
		assert.Greater(t, percent, int64(0))
	}
	parser := NewJSONParser(ctx, schema, updateProgress)
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
	assert.Nil(t, err)
	strContent := string(binContent)
	reader := strings.NewReader(strContent)

	consumer := &mockJSONRowConsumer{
		handleErr:   nil,
		rows:        make([]map[int64]interface{}, 0),
		handleCount: 0,
	}

	err = parser.ParseRows(&IOReader{r: reader, fileSize: int64(len(binContent))}, consumer)
	assert.Nil(t, err)
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
		assert.Nil(t, err)
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
			assert.Nil(t, err)
			assert.InDelta(t, contenctRow.FieldFloatVector[k], float32(fval), 10e-6)
		}
	}
}
