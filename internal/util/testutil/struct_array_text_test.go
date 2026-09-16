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

package testutil_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	csvimport "github.com/milvus-io/milvus/internal/util/importutilv2/csv"
	jsonimport "github.com/milvus-io/milvus/internal/util/importutilv2/json"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestStructArrayTextFixtureRoundTrip(t *testing.T) {
	values := []float32{0.25, -0.5, 0.75, 1}
	tests := []struct {
		elementType schemapb.DataType
		dim         string
		vectors     *schemapb.VectorField
	}{
		{schemapb.DataType_FloatVector, "2", &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: values}}}},
		{schemapb.DataType_Float16Vector, "2", &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Float16Vector{Float16Vector: typeutil.Float32ArrayToFloat16Bytes(values)}}},
		{schemapb.DataType_BFloat16Vector, "2", &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: typeutil.Float32ArrayToBFloat16Bytes(values)}}},
		{schemapb.DataType_Int8Vector, "2", &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_Int8Vector{Int8Vector: typeutil.Int8ArrayToBytes([]int8{1, -2, 3, -4})}}},
		{schemapb.DataType_BinaryVector, "16", &schemapb.VectorField{Dim: 16, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{0, 255, 128, 1}}}},
	}
	for _, test := range tests {
		t.Run(test.elementType.String(), func(t *testing.T) {
			schema := &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
				StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 101, Name: "items", Fields: []*schemapb.FieldSchema{{
					FieldID: 102, Name: "vec", DataType: schemapb.DataType_ArrayOfVector, ElementType: test.elementType,
					TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: test.dim}, {Key: "max_capacity", Value: "4"}},
				}}}},
			}
			insertData := &storage.InsertData{Data: map[int64]storage.FieldData{
				100: &storage.Int64FieldData{Data: []int64{1}},
				102: &storage.VectorArrayFieldData{Dim: test.vectors.GetDim(), ElementType: test.elementType, Data: []*schemapb.VectorField{test.vectors}},
			}}
			// Import parsers receive qualified subfield names after CreateCollection.
			parserSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
			parserSchema.StructArrayFields[0].Fields[0].Name = "items[vec]"
			t.Run("JSON", func(t *testing.T) {
				rows, err := testutil.CreateInsertDataRowsForJSON(schema, insertData)
				require.NoError(t, err)
				encoded, err := json.Marshal(rows)
				require.NoError(t, err)
				var decoded []map[string]any
				decoder := json.NewDecoder(bytes.NewReader(encoded))
				decoder.UseNumber()
				require.NoError(t, decoder.Decode(&decoded))
				require.Len(t, decoded, 1)
				parser, err := jsonimport.NewRowParser(parserSchema)
				require.NoError(t, err)
				row, err := parser.Parse(decoded[0])
				require.NoError(t, err)
				require.True(t, proto.Equal(test.vectors, row[102].(*schemapb.VectorField)), "expected %v, got %v", test.vectors, row[102])
			})
			t.Run("CSV", func(t *testing.T) {
				rows, err := testutil.CreateInsertDataForCSV(schema, insertData, "")
				require.NoError(t, err)
				require.Len(t, rows, 2)
				parser, err := csvimport.NewRowParser(parserSchema, rows[0], "")
				require.NoError(t, err)
				row, err := parser.Parse(rows[1])
				require.NoError(t, err)
				require.True(t, proto.Equal(test.vectors, row[102].(*schemapb.VectorField)), "expected %v, got %v", test.vectors, row[102])
			})
		})
	}
}
