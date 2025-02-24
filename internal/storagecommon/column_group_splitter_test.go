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

package storagecommon

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestColumnGroupSplitter_Split(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "double", DataType: schemapb.DataType_Double},
			{Name: "bool", DataType: schemapb.DataType_Bool},
			{Name: "string", DataType: schemapb.DataType_String},
			{Name: "float", DataType: schemapb.DataType_Float},
			{Name: "int8", DataType: schemapb.DataType_Int8},
			{Name: "int16", DataType: schemapb.DataType_Int16},
			{Name: "int32", DataType: schemapb.DataType_Int32},
			{Name: "int64", DataType: schemapb.DataType_Int64},
			{Name: "FloatVector", DataType: schemapb.DataType_FloatVector},
			{Name: "BinaryVector", DataType: schemapb.DataType_BinaryVector},
		},
	}

	expected := []ColumnGroup{
		{Columns: []int{2}},
		{Columns: []int{8}},
		{Columns: []int{9}},
		{Columns: []int{0, 1, 3, 4, 5, 6, 7}},
	}

	splitter := NewColumnGroupSplitter(schema)
	result := splitter.SplitByFieldType()

	assert.True(t, reflect.DeepEqual(result, expected))
}
