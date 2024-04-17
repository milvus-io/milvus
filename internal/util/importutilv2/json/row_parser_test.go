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

package json

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

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
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "0"}},
			},
			{
				FieldID:   3,
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}
	r, err := NewRowParser(schema)
	assert.NoError(t, err)

	type testCase struct {
		name     string   // input json
		dyFields []string // expect dynamic fields
	}

	cases := []testCase{
		{name: `{"id": 1, "vector": [], "x": 8, "$meta": "{\"y\": 8}"}`, dyFields: []string{"x", "y"}},
		{name: `{"id": 1, "vector": [], "x": 8, "$meta": {}}`, dyFields: []string{"x"}},
		{name: `{"id": 1, "vector": [], "$meta": "{\"x\": 8}"}`, dyFields: []string{"x"}},
		{name: `{"id": 1, "vector": [], "$meta": {"x": 8}}`, dyFields: []string{"x"}},
		{name: `{"id": 1, "vector": [], "$meta": {}}`, dyFields: nil},
		{name: `{"id": 1, "vector": [], "x": 8}`, dyFields: []string{"x"}},
		{name: `{"id": 1, "vector": []}`, dyFields: nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var mp map[string]interface{}

			desc := json.NewDecoder(strings.NewReader(c.name))
			desc.UseNumber()
			err = desc.Decode(&mp)
			assert.NoError(t, err)

			row, err := r.Parse(mp)
			assert.NoError(t, err)

			// validate contains fields
			for _, field := range schema.GetFields() {
				_, ok := row[field.GetFieldID()]
				assert.True(t, ok)
			}

			// validate dynamic fields
			var dynamicFields map[string]interface{}
			err = json.Unmarshal(row[r.(*rowParser).dynamicField.GetFieldID()].([]byte), &dynamicFields)
			assert.NoError(t, err)
			for _, k := range c.dyFields {
				_, ok := dynamicFields[k]
				assert.True(t, ok)
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
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "0"}},
			},
			{
				FieldID:   3,
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}
	r, err := NewRowParser(schema)
	assert.NoError(t, err)

	type testCase struct {
		name      string // input json
		expectErr string
	}

	cases := []testCase{
		{name: `{"id": 1, "vector": [], "x": 6, "$meta": {"x": 8}}`, expectErr: "duplicated key is not allowed"},
		{name: `{"id": 1, "vector": [], "x": 6, "$meta": "{\"x\": 8}"}`, expectErr: "duplicated key is not allowed"},
		{name: `{"id": 1, "vector": [], "x": 6, "$meta": "{*&%%&$*(&"}`, expectErr: "not a JSON format string"},
		{name: `{"id": 1, "vector": [], "x": 6, "$meta": []}`, expectErr: "not a JSON object"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var mp map[string]interface{}

			desc := json.NewDecoder(strings.NewReader(c.name))
			desc.UseNumber()
			err = desc.Decode(&mp)
			assert.NoError(t, err)

			_, err = r.Parse(mp)
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), c.expectErr))
		})
	}
}
