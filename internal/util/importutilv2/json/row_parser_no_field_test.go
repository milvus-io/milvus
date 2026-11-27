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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
)

// A JSON row that names no field of the collection is rejected instead of
// becoming a row of nulls and defaults; a row with dynamic data is accepted.
func TestParseRejectsRowWithNoCollectionField(t *testing.T) {
	collection := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", IsPrimaryKey: true, AutoID: true, DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int32, Nullable: true},
	}}
	parser, err := NewRowParser(collection)
	require.NoError(t, err)
	for _, raw := range []map[string]any{{}, {"other": json.Number("1")}} {
		_, err = parser.Parse(raw)
		require.Error(t, err)
		require.Contains(t, err.Error(), "provides no field of the collection")
	}
	_, err = parser.Parse(map[string]any{"age": json.Number("7")})
	require.NoError(t, err)

	dynamic := &schemapb.CollectionSchema{EnableDynamicField: true, Fields: append(collection.Fields,
		&schemapb.FieldSchema{FieldID: 102, Name: "$meta", DataType: schemapb.DataType_JSON, IsDynamic: true})}
	parser, err = NewRowParser(dynamic)
	require.NoError(t, err)
	_, err = parser.Parse(map[string]any{"other": json.Number("1")})
	require.NoError(t, err)
}
