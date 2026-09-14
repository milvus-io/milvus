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

package taskcommon

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func sizedField(dataType schemapb.DataType, params ...string) *schemapb.FieldSchema {
	field := &schemapb.FieldSchema{FieldID: 100, DataType: dataType}
	for i := 0; i+1 < len(params); i += 2 {
		field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{Key: params[i], Value: params[i+1]})
	}
	return field
}

func TestFixedFieldWidth(t *testing.T) {
	for dataType, want := range map[schemapb.DataType]int64{
		schemapb.DataType_Bool: 1, schemapb.DataType_Int8: 1, schemapb.DataType_Int16: 2,
		schemapb.DataType_Int32: 4, schemapb.DataType_Float: 4,
		schemapb.DataType_Int64: 8, schemapb.DataType_Double: 8, schemapb.DataType_Timestamptz: 8,
		schemapb.DataType_VarChar: 0, schemapb.DataType_JSON: 0, schemapb.DataType_Array: 0,
		schemapb.DataType_SparseFloatVector: 0, schemapb.DataType_ArrayOfVector: 0,
	} {
		assert.Equal(t, want, FixedFieldWidth(sizedField(dataType, "dim", "8")), dataType.String())
	}
	for dataType, want := range map[schemapb.DataType]int64{
		schemapb.DataType_FloatVector: 32, schemapb.DataType_Float16Vector: 16, schemapb.DataType_BFloat16Vector: 16,
		schemapb.DataType_BinaryVector: 1, schemapb.DataType_Int8Vector: 8,
	} {
		assert.Equal(t, want, FixedFieldWidth(sizedField(dataType, "dim", "8")), dataType.String())
	}
	// A dense vector without a dim has no width.
	assert.Equal(t, int64(0), FixedFieldWidth(sizedField(schemapb.DataType_FloatVector)))
}

func TestSchemaFieldSize(t *testing.T) {
	size, exact, ok := SchemaFieldSize(sizedField(schemapb.DataType_Int64), 1000)
	assert.Equal(t, int64(8000), size)
	assert.True(t, exact)
	assert.True(t, ok)

	// Nullable adds the validity bitmap.
	nullable := sizedField(schemapb.DataType_Int64)
	nullable.Nullable = true
	size, _, _ = SchemaFieldSize(nullable, 1000)
	assert.Equal(t, int64(8000+125), size)

	// A varchar is bounded by max_length plus the offset, and is not exact.
	size, exact, ok = SchemaFieldSize(sizedField(schemapb.DataType_VarChar, "enable_match", "true", "max_length", "256"), 1000)
	assert.Equal(t, int64(1000*(256+4)), size)
	assert.False(t, exact)
	assert.True(t, ok)

	// Types the schema does not bound.
	for _, field := range []*schemapb.FieldSchema{
		sizedField(schemapb.DataType_VarChar),
		sizedField(schemapb.DataType_VarChar, "max_length", "junk"),
		sizedField(schemapb.DataType_JSON),
		sizedField(schemapb.DataType_Text),
		sizedField(schemapb.DataType_Array, "max_capacity", "4"),
		sizedField(schemapb.DataType_SparseFloatVector),
		sizedField(schemapb.DataType_FloatVector),
	} {
		_, _, ok := SchemaFieldSize(field, 1000)
		assert.False(t, ok, field.GetDataType().String())
	}
	// No rows, no field.
	_, _, ok = SchemaFieldSize(sizedField(schemapb.DataType_Int64), 0)
	assert.False(t, ok)
	_, _, ok = SchemaFieldSize(nil, 1000)
	assert.False(t, ok)
}

func TestColumnGroupSize(t *testing.T) {
	binlogs := []*datapb.FieldBinlog{
		// storage v1: one FieldBinlog per field.
		{FieldID: 100, Binlogs: []*datapb.Binlog{{MemorySize: 10}, {MemorySize: 20}}},
		// storage v2/v3: a column group listing its fields as children.
		{FieldID: 1, ChildFields: []int64{101, 102}, Binlogs: []*datapb.Binlog{{MemorySize: 300}}},
		{FieldID: 103, ChildFields: []int64{103}, Binlogs: []*datapb.Binlog{{MemorySize: 7}}},
	}
	assert.Equal(t, int64(30), ColumnGroupSize(binlogs, 100))
	assert.Equal(t, int64(300), ColumnGroupSize(binlogs, 101))
	assert.Equal(t, int64(300), ColumnGroupSize(binlogs, 102))
	// A group whose id equals the field id and that also lists it is counted once.
	assert.Equal(t, int64(7), ColumnGroupSize(binlogs, 103))
	assert.Equal(t, int64(0), ColumnGroupSize(binlogs, 999))
	assert.Equal(t, int64(0), ColumnGroupSize(nil, 100))
}

func TestEstimateFieldSize(t *testing.T) {
	int64Field := sizedField(schemapb.DataType_Int64)
	varChar := sizedField(schemapb.DataType_VarChar, "max_length", "256")
	jsonField := sizedField(schemapb.DataType_JSON)

	// An Int64 sharing a 150MB short column group is its own 8 bytes a row.
	assert.Equal(t, int64(8000), EstimateFieldSize(int64Field, 1000, 150<<20))
	// A short column group smaller than the bound wins.
	assert.Equal(t, int64(5000), EstimateFieldSize(int64Field, 1000, 5000))
	// A varchar whose values are short: the group is smaller than max_length allows.
	assert.Equal(t, int64(40_000), EstimateFieldSize(varChar, 1000, 40_000))
	// A varchar sharing a big group: max_length bounds it.
	assert.Equal(t, int64(1000*260), EstimateFieldSize(varChar, 1000, 1<<30))
	// Unknown container: the schema bound alone.
	assert.Equal(t, int64(8000), EstimateFieldSize(int64Field, 1000, 0))
	// An unbounded type: the container alone, or nothing.
	assert.Equal(t, int64(12345), EstimateFieldSize(jsonField, 1000, 12345))
	assert.Equal(t, int64(0), EstimateFieldSize(jsonField, 1000, 0))
}
