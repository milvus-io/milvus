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

package entity

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

func TestCL_CommonCL(t *testing.T) {
	cls := []ConsistencyLevel{
		ClStrong,
		ClBounded,
		ClSession,
		ClEventually,
	}
	for _, cl := range cls {
		assert.EqualValues(t, commonpb.ConsistencyLevel(cl), cl.CommonConsistencyLevel())
	}
}

type SchemaSuite struct {
	suite.Suite
}

func (s *SchemaSuite) TestBasic() {
	cases := []struct {
		tag    string
		input  *Schema
		pkName string
	}{
		{
			"test_collection",
			NewSchema().WithName("test_collection_1").WithDescription("test_collection_1 desc").WithAutoID(false).
				WithField(NewField().WithName("ID").WithDataType(FieldTypeInt64).WithIsPrimaryKey(true)).
				WithField(NewField().WithName("vector").WithDataType(FieldTypeFloatVector).WithDim(128)).
				WithFunction(NewFunction()),
			"ID",
		},
		{
			"dynamic_schema",
			NewSchema().WithName("dynamic_schema").WithDescription("dynamic_schema desc").WithAutoID(true).WithDynamicFieldEnabled(true).
				WithField(NewField().WithName("ID").WithDataType(FieldTypeVarChar).WithMaxLength(256)).
				WithField(NewField().WithName("$meta").WithIsDynamic(true)),
			"",
		},
	}

	for _, c := range cases {
		s.Run(c.tag, func() {
			sch := c.input
			p := sch.ProtoMessage()
			s.Equal(sch.CollectionName, p.GetName())
			s.Equal(sch.AutoID, p.GetAutoID())
			s.Equal(sch.Description, p.GetDescription())
			s.Equal(sch.EnableDynamicField, p.GetEnableDynamicField())
			s.Equal(len(sch.Fields), len(p.GetFields()))
			s.Equal(len(sch.Functions), len(p.GetFunctions()))

			nsch := &Schema{}
			nsch = nsch.ReadProto(p)

			s.Equal(sch.CollectionName, nsch.CollectionName)
			s.Equal(sch.Description, nsch.Description)
			s.Equal(sch.EnableDynamicField, nsch.EnableDynamicField)
			s.Equal(len(sch.Fields), len(nsch.Fields))
			s.Equal(len(sch.Functions), len(nsch.Functions))
			s.Equal(c.pkName, sch.PKFieldName())
			s.Equal(c.pkName, nsch.PKFieldName())
		})
	}
}

func (s *SchemaSuite) TestStructArrayField() {
	// Create a struct schema
	structSchema := NewStructSchema().
		WithField(NewField().WithName("age").WithDataType(FieldTypeInt32)).
		WithField(NewField().WithName("name").WithDataType(FieldTypeVarChar).WithMaxLength(100)).
		WithField(NewField().WithName("score").WithDataType(FieldTypeFloat))

	// Create a schema with struct array field
	schema := NewSchema().
		WithName("test_struct_array_collection").
		WithDescription("collection with struct array field").
		WithAutoID(false).
		WithField(NewField().WithName("ID").WithDataType(FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(NewField().WithName("vector").WithDataType(FieldTypeFloatVector).WithDim(128)).
		WithField(NewField().
			WithName("person_data").
			WithDataType(FieldTypeArray).
			WithElementType(FieldTypeStruct).
			WithStructSchema(structSchema))

	// Convert to proto
	p := schema.ProtoMessage()

	// Verify basic schema properties
	s.Equal("test_struct_array_collection", p.GetName())
	s.Equal("collection with struct array field", p.GetDescription())
	s.Equal(false, p.GetAutoID())

	// Verify regular fields (should not include struct array field)
	s.Equal(2, len(p.GetFields()))
	s.Equal("ID", p.GetFields()[0].GetName())
	s.Equal("vector", p.GetFields()[1].GetName())

	// Verify struct array fields
	s.Equal(1, len(p.GetStructArrayFields()))
	structArrayField := p.GetStructArrayFields()[0]
	s.Equal("person_data", structArrayField.GetName())
	s.Equal(3, len(structArrayField.GetFields()))

	// Verify struct array sub-fields
	s.Equal("age", structArrayField.GetFields()[0].GetName())
	s.Equal("name", structArrayField.GetFields()[1].GetName())
	s.Equal("score", structArrayField.GetFields()[2].GetName())
}

func (s *SchemaSuite) TestStructArrayFieldWithVectorElement() {
	// Create a struct schema with vector field
	structSchema := NewStructSchema().
		WithField(NewField().WithName("id").WithDataType(FieldTypeInt64)).
		WithField(NewField().WithName("embedding").WithDataType(FieldTypeFloatVector).WithDim(256))

	schema := NewSchema().
		WithName("test_struct_with_vector").
		WithAutoID(true).
		WithField(NewField().WithName("pk").WithDataType(FieldTypeVarChar).WithMaxLength(100).WithIsPrimaryKey(true)).
		WithField(NewField().
			WithName("data").
			WithDataType(FieldTypeArray).
			WithElementType(FieldTypeStruct).
			WithStructSchema(structSchema))

	p := schema.ProtoMessage()

	// Verify struct array field with vector element
	s.Equal(1, len(p.GetStructArrayFields()))
	structArrayField := p.GetStructArrayFields()[0]
	s.Equal("data", structArrayField.GetName())
	s.Equal(2, len(structArrayField.GetFields()))

	// Verify that vector field is converted to ArrayOfVector
	embeddingField := structArrayField.GetFields()[1]
	s.Equal("embedding", embeddingField.GetName())
	// The DataType should be changed to ArrayOfVector for vector types
	s.NotEqual(FieldTypeFloatVector, embeddingField.GetDataType())
}

func (s *SchemaSuite) TestMultipleStructArrayFields() {
	// Create multiple struct schemas
	structSchema1 := NewStructSchema().
		WithField(NewField().WithName("field1").WithDataType(FieldTypeInt32))

	structSchema2 := NewStructSchema().
		WithField(NewField().WithName("field2").WithDataType(FieldTypeVarChar).WithMaxLength(50)).
		WithField(NewField().WithName("field3").WithDataType(FieldTypeDouble))

	schema := NewSchema().
		WithName("test_multiple_struct_arrays").
		WithField(NewField().WithName("pk").WithDataType(FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(NewField().
			WithName("struct_array_1").
			WithDataType(FieldTypeArray).
			WithElementType(FieldTypeStruct).
			WithStructSchema(structSchema1)).
		WithField(NewField().
			WithName("struct_array_2").
			WithDataType(FieldTypeArray).
			WithElementType(FieldTypeStruct).
			WithStructSchema(structSchema2))

	p := schema.ProtoMessage()

	// Verify we have 2 struct array fields
	s.Equal(2, len(p.GetStructArrayFields()))
	s.Equal("struct_array_1", p.GetStructArrayFields()[0].GetName())
	s.Equal("struct_array_2", p.GetStructArrayFields()[1].GetName())

	// Verify each struct array has correct number of fields
	s.Equal(1, len(p.GetStructArrayFields()[0].GetFields()))
	s.Equal(2, len(p.GetStructArrayFields()[1].GetFields()))
}

func TestSchema(t *testing.T) {
	suite.Run(t, new(SchemaSuite))
}
