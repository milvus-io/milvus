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

package csv

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestNewRowParser_Invalid(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    2,
				Name:       "vector",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:  3,
				Name:     "str",
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:   4,
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}

	type testCase struct {
		header    []string
		expectErr string
	}

	cases := []testCase{
		{header: []string{"id", "vector", "$meta"}, expectErr: "value of field is missed: 'str'"},
	}

	nullkey := ""

	for i, c := range cases {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			_, err := NewRowParser(schema, c.header, nullkey)
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), c.expectErr))
		})
	}
}

func TestRowParser_Parse_SparseVector(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    2,
				Name:       "sparse_vector",
				DataType:   schemapb.DataType_SparseFloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
			},
		},
	}

	tests := []struct {
		name       string
		header     []string
		row        []string
		wantMaxIdx uint32
		wantErr    bool
	}{
		{
			name:       "empty sparse vector",
			header:     []string{"id", "sparse_vector"},
			row:        []string{"1", "{}"},
			wantMaxIdx: 0,
			wantErr:    false,
		},
		{
			name:       "key-value format",
			header:     []string{"id", "sparse_vector"},
			row:        []string{"1", "{\"5\":3.14}"},
			wantMaxIdx: 6, // max index 5 + 1
			wantErr:    false,
		},
		{
			name:       "multiple key-value pairs",
			header:     []string{"id", "sparse_vector"},
			row:        []string{"1", "{\"1\":0.5,\"10\":1.5,\"100\":2.5}"},
			wantMaxIdx: 101, // max index 100 + 1
			wantErr:    false,
		},
		{
			name:    "invalid format",
			header:  []string{"id", "sparse_vector"},
			row:     []string{"1", "{275574541:1.5383775}"},
			wantErr: true,
		},
	}

	nullkey := ""

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewRowParser(schema, tt.header, nullkey)
			assert.NoError(t, err)

			row, err := parser.Parse(tt.row)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Contains(t, row, int64(2)) // sparse_vector field ID

			sparseVec := row[int64(2)].([]byte)

			if tt.wantMaxIdx > 0 {
				elemCount := len(sparseVec) / 8
				assert.Greater(t, elemCount, 0)

				// Check the last index matches our expectation
				lastIdx := typeutil.SparseFloatRowIndexAt(sparseVec, elemCount-1)
				assert.Equal(t, tt.wantMaxIdx-1, lastIdx)
			} else {
				assert.Empty(t, sparseVec)
			}
		})
	}
}

func TestRowParser_Parse_Valid(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    2,
				Name:       "vector",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:   3,
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}

	type testCase struct {
		header   []string
		row      []string
		dyFields map[string]any // expect dynamic fields
	}

	cases := []testCase{
		{header: []string{"id", "vector", "$meta"}, row: []string{"1", "[1, 2]", "{\"y\": 2}"}, dyFields: map[string]any{"y": 2.0}},
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "8", "{\"y\": 2}"}, dyFields: map[string]any{"x": "8", "y": 2.0}},
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "8", "{}"}, dyFields: map[string]any{"x": "8"}},
		{header: []string{"id", "vector", "x"}, row: []string{"1", "[1, 2]", "8"}, dyFields: map[string]any{"x": "8"}},
		{header: []string{"id", "vector", "str", "$meta"}, row: []string{"1", "[1, 2]", "xxsddsffwq", "{\"y\": 2}"}, dyFields: map[string]any{"y": 2.0, "str": "xxsddsffwq"}},
	}

	nullkey := ""

	for i, c := range cases {
		r, err := NewRowParser(schema, c.header, nullkey)
		assert.NoError(t, err)
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			data, err := r.Parse(c.row)
			assert.NoError(t, err)

			// validate contains fields
			for _, field := range schema.GetFields() {
				_, ok := data[field.GetFieldID()]
				assert.True(t, ok)
			}

			// validate dynamic fields
			var dynamicFields map[string]interface{}
			err = json.Unmarshal(data[r.(*rowParser).dynamicField.GetFieldID()].([]byte), &dynamicFields)
			assert.NoError(t, err)
			assert.Len(t, dynamicFields, len(c.dyFields))
			for k, v := range c.dyFields {
				rv, ok := dynamicFields[k]
				assert.True(t, ok)
				assert.EqualValues(t, rv, v)
			}
		})
	}
}

func TestRowParser_Parse_Invalid(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    2,
				Name:       "vector",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:   3,
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}

	type testCase struct {
		header    []string
		row       []string
		expectErr string
	}

	cases := []testCase{
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "6", "{\"x\": 8}"}, expectErr: "duplicated key in dynamic field, key=x"},
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "8", "{*&%%&$*(&}"}, expectErr: "illegal value for dynamic field, not a JSON format string"},
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "8"}, expectErr: "the number of fields in the row is not equal to the header"},
	}

	nullkey := ""

	for i, c := range cases {
		r, err := NewRowParser(schema, c.header, nullkey)
		assert.NoError(t, err)
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			_, err := r.Parse(c.row)
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), c.expectErr))
		})
	}
}

func TestRowParser_Parse_NULL(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    2,
				Name:       "vector",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:  3,
				Name:     "str",
				DataType: schemapb.DataType_String,
				TypeParams: []*commonpb.KeyValuePair{
					{
						Key:   common.MaxLengthKey,
						Value: "128",
					},
				},
				Nullable: true,
			},
		},
	}

	header := []string{"id", "vector", "str"}

	type testCase struct {
		nullkey  string
		row      []string
		nulldata interface{}
	}

	cases := []testCase{
		{nullkey: "", row: []string{"1", "[1, 2]", ""}, nulldata: nil},
		{nullkey: "NULL", row: []string{"1", "[1, 2]", "NULL"}, nulldata: nil},
		{nullkey: "\\N", row: []string{"1", "[1, 2]", "\\N"}, nulldata: nil},
	}

	for i, c := range cases {
		r, err := NewRowParser(schema, header, c.nullkey)
		assert.NoError(t, err)
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			data, err := r.Parse(c.row)
			assert.NoError(t, err)
			assert.EqualValues(t, c.nulldata, data[3])
		})
	}
}
