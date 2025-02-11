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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestConvertArrowSchema(t *testing.T) {
	fieldSchemas := []*schemapb.FieldSchema{
		{FieldID: 1, Name: "field0", DataType: schemapb.DataType_Bool},
		{FieldID: 2, Name: "field1", DataType: schemapb.DataType_Int8},
		{FieldID: 3, Name: "field2", DataType: schemapb.DataType_Int16},
		{FieldID: 4, Name: "field3", DataType: schemapb.DataType_Int32},
		{FieldID: 5, Name: "field4", DataType: schemapb.DataType_Int64},
		{FieldID: 6, Name: "field5", DataType: schemapb.DataType_Float},
		{FieldID: 7, Name: "field6", DataType: schemapb.DataType_Double},
		{FieldID: 8, Name: "field7", DataType: schemapb.DataType_String},
		{FieldID: 9, Name: "field8", DataType: schemapb.DataType_VarChar},
		{FieldID: 10, Name: "field9", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 11, Name: "field10", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 12, Name: "field11", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		{FieldID: 13, Name: "field12", DataType: schemapb.DataType_JSON},
		{FieldID: 14, Name: "field13", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 15, Name: "field14", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 16, Name: "field15", DataType: schemapb.DataType_Int8Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
	}

	schema, err := ConvertToArrowSchema(fieldSchemas)
	assert.NoError(t, err)
	assert.Equal(t, len(fieldSchemas), len(schema.Fields()))
}

func TestConvertArrowSchemaWithoutDim(t *testing.T) {
	fieldSchemas := []*schemapb.FieldSchema{
		{FieldID: 1, Name: "field0", DataType: schemapb.DataType_Bool},
		{FieldID: 2, Name: "field1", DataType: schemapb.DataType_Int8},
		{FieldID: 3, Name: "field2", DataType: schemapb.DataType_Int16},
		{FieldID: 4, Name: "field3", DataType: schemapb.DataType_Int32},
		{FieldID: 5, Name: "field4", DataType: schemapb.DataType_Int64},
		{FieldID: 6, Name: "field5", DataType: schemapb.DataType_Float},
		{FieldID: 7, Name: "field6", DataType: schemapb.DataType_Double},
		{FieldID: 8, Name: "field7", DataType: schemapb.DataType_String},
		{FieldID: 9, Name: "field8", DataType: schemapb.DataType_VarChar},
		{FieldID: 10, Name: "field9", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 11, Name: "field10", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		{FieldID: 12, Name: "field11", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		{FieldID: 13, Name: "field12", DataType: schemapb.DataType_JSON},
		{FieldID: 14, Name: "field13", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{}},
		{FieldID: 15, Name: "field14", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{}},
		{FieldID: 16, Name: "field15", DataType: schemapb.DataType_Int8Vector, TypeParams: []*commonpb.KeyValuePair{}},
	}

	_, err := ConvertToArrowSchema(fieldSchemas)
	assert.Error(t, err)
}
