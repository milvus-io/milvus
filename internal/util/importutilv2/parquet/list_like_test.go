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

package parquet

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// TestNullableArray_EmptyListIsNotNull guards against regressing
// https://github.com/milvus-io/milvus/issues/53438: a nullable ARRAY field
// with a valid-but-empty list `[]` must stay valid, and must be kept
// distinct from an actual null row.
func TestNullableArray_EmptyListIsNotNull(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:     100,
		Name:        "int64_array",
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Int64,
		Nullable:    true,
	}

	mem := memory.NewGoAllocator()
	builder := array.NewListBuilder(mem, arrow.PrimitiveTypes.Int64)
	valueBuilder := builder.ValueBuilder().(*array.Int64Builder)

	builder.Append(true) // row 0: valid empty list []
	builder.Append(true) // row 1: valid list [10, 20]
	valueBuilder.Append(10)
	valueBuilder.Append(20)
	builder.AppendNull() // row 2: actual null

	listReader, err := newListLikeArray(builder.NewListArray(), field)
	assert.NoError(t, err)

	var data [][]int64
	var validData []bool
	err = readIntegerOrFloatListLikeData(field, listReader, func(arr []int64, valid bool) {
		data = append(data, arr)
		validData = append(validData, valid)
	})
	assert.NoError(t, err)

	assert.Equal(t, []bool{true, true, false}, validData)
	assert.Equal(t, [][]int64{{}, {10, 20}, nil}, data)
}
