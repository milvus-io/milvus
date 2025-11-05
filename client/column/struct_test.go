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

package column

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type StructArraySuite struct {
	suite.Suite
}

func (s *StructArraySuite) TestBasic() {
	name := fmt.Sprintf("struct_array_%d", rand.Intn(100))

	// Create sub-fields for the struct
	int32Data := []int32{1, 2, 3, 4, 5}
	floatData := []float32{1.1, 2.2, 3.3, 4.4, 5.5}
	varcharData := []string{"a", "b", "c", "d", "e"}

	int32Col := NewColumnInt32("int_field", int32Data)
	floatCol := NewColumnFloat("float_field", floatData)
	varcharCol := NewColumnVarChar("varchar_field", varcharData)

	fields := []Column{int32Col, floatCol, varcharCol}
	column := NewColumnStructArray(name, fields)

	// Test Name()
	s.Equal(name, column.Name())

	// Test Type()
	s.Equal(entity.FieldTypeArray, column.Type())

	// Test Len()
	s.EqualValues(5, column.Len())

	// Test FieldData()
	fd := column.FieldData()
	s.Equal(schemapb.DataType_Array, fd.GetType())
	s.Equal(name, fd.GetFieldName())

	structArrays := fd.GetStructArrays()
	s.NotNil(structArrays)
	s.Equal(3, len(structArrays.GetFields()))

	// Verify each field in the struct array
	fieldNames := []string{"int_field", "float_field", "varchar_field"}
	for i, field := range structArrays.GetFields() {
		s.Equal(fieldNames[i], field.GetFieldName())
	}

	// Test Get() - retrieve values as map
	val, err := column.Get(0)
	s.NoError(err)
	m, ok := val.(map[string]any)
	s.True(ok)
	s.Equal(int32(1), m["int_field"])
	s.Equal(float32(1.1), m["float_field"])
	s.Equal("a", m["varchar_field"])

	val, err = column.Get(2)
	s.NoError(err)
	m, ok = val.(map[string]any)
	s.True(ok)
	s.Equal(int32(3), m["int_field"])
	s.Equal(float32(3.3), m["float_field"])
	s.Equal("c", m["varchar_field"])
}

func (s *StructArraySuite) TestSlice() {
	name := "struct_array_slice"

	// Create sub-fields
	int64Data := []int64{10, 20, 30, 40, 50}
	boolData := []bool{true, false, true, false, true}

	int64Col := NewColumnInt64("id", int64Data)
	boolCol := NewColumnBool("flag", boolData)

	fields := []Column{int64Col, boolCol}
	column := NewColumnStructArray(name, fields)

	// Test Slice(1, 4) - should slice all sub-fields
	sliced := column.Slice(1, 4)
	s.NotNil(sliced)

	// Verify sliced data
	val, err := sliced.Get(0)
	s.NoError(err)
	m, ok := val.(map[string]any)
	s.True(ok)
	s.Equal(int64(20), m["id"])
	s.Equal(false, m["flag"])

	val, err = sliced.Get(2)
	s.NoError(err)
	m, ok = val.(map[string]any)
	s.True(ok)
	s.Equal(int64(40), m["id"])
	s.Equal(false, m["flag"])
}

func (s *StructArraySuite) TestNullable() {
	name := "struct_array_nullable"

	// Create sub-fields
	int32Data := []int32{1, 2, 3}
	int32Col := NewColumnInt32("field1", int32Data)

	varcharData := []string{"x", "y", "z"}
	varcharCol := NewColumnVarChar("field2", varcharData)

	fields := []Column{int32Col, varcharCol}
	column := NewColumnStructArray(name, fields)

	// Test Nullable() - should return false by default
	s.False(column.Nullable())

	// Test ValidateNullable() - should validate all sub-fields
	err := column.ValidateNullable()
	s.NoError(err)

	// Test CompactNullableValues() - should not panic
	s.NotPanics(func() {
		column.CompactNullableValues()
	})
}

func (s *StructArraySuite) TestNotImplementedMethods() {
	name := "struct_array_not_impl"

	int32Data := []int32{1, 2, 3}
	int32Col := NewColumnInt32("field1", int32Data)
	fields := []Column{int32Col}
	column := NewColumnStructArray(name, fields)

	// Test AppendValue - should return error
	err := column.AppendValue(map[string]any{"field1": int32(4)})
	s.Error(err)
	s.Contains(err.Error(), "not implemented")

	// Test GetAsInt64 - should return error
	_, err = column.GetAsInt64(0)
	s.Error(err)
	s.Contains(err.Error(), "not implemented")

	// Test GetAsString - should return error
	_, err = column.GetAsString(0)
	s.Error(err)
	s.Contains(err.Error(), "not implemented")

	// Test GetAsDouble - should return error
	_, err = column.GetAsDouble(0)
	s.Error(err)
	s.Contains(err.Error(), "not implemented")

	// Test GetAsBool - should return error
	_, err = column.GetAsBool(0)
	s.Error(err)
	s.Contains(err.Error(), "not implemented")

	// Test IsNull - should return error
	_, err = column.IsNull(0)
	s.Error(err)
	s.Contains(err.Error(), "not implemented")

	// Test AppendNull - should return error
	err = column.AppendNull()
	s.Error(err)
	s.Contains(err.Error(), "not implemented")
}

func (s *StructArraySuite) TestParseStructArrayData() {
	// Create a FieldData with struct array
	int32FieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Int32,
		FieldName: "age",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: []int32{10, 20, 30},
					},
				},
			},
		},
	}

	varcharFieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: "name",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{"alice", "bob", "charlie"},
					},
				},
			},
		},
	}

	structArrayField := &schemapb.StructArrayField{
		Fields: []*schemapb.FieldData{int32FieldData, varcharFieldData},
	}

	// Test parseStructArrayData
	column, err := parseStructArrayData("person", structArrayField, 0, -1)
	s.NoError(err)
	s.NotNil(column)
	s.Equal("person", column.Name())
	s.Equal(entity.FieldTypeArray, column.Type())

	// Verify we can get values
	val, err := column.Get(0)
	s.NoError(err)
	m, ok := val.(map[string]any)
	s.True(ok)
	s.Equal(int32(10), m["age"])
	s.Equal("alice", m["name"])

	val, err = column.Get(1)
	s.NoError(err)
	m, ok = val.(map[string]any)
	s.True(ok)
	s.Equal(int32(20), m["age"])
	s.Equal("bob", m["name"])
}

func (s *StructArraySuite) TestParseStructArrayDataWithRange() {
	// Create a FieldData with struct array
	int64FieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "id",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: []int64{100, 200, 300, 400, 500},
					},
				},
			},
		},
	}

	structArrayField := &schemapb.StructArrayField{
		Fields: []*schemapb.FieldData{int64FieldData},
	}

	// Test parseStructArrayData with range [1, 4)
	column, err := parseStructArrayData("data", structArrayField, 1, 4)
	s.NoError(err)
	s.NotNil(column)

	// Verify sliced data (should contain indices 1, 2, 3)
	val, err := column.Get(0)
	s.NoError(err)
	m, ok := val.(map[string]any)
	s.True(ok)
	s.Equal(int64(200), m["id"])

	val, err = column.Get(2)
	s.NoError(err)
	m, ok = val.(map[string]any)
	s.True(ok)
	s.Equal(int64(400), m["id"])
}

func TestStructArray(t *testing.T) {
	suite.Run(t, new(StructArraySuite))
}
