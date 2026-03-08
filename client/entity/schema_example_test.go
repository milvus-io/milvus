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

// nolint
package entity_test

import (
	"log"

	"github.com/milvus-io/milvus/client/v2/entity"
)

func ExampleNewSchema() {
	schema := entity.NewSchema()
	log.Println(schema)
}

func ExampleSchema_WithField_primaryKey() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_id").
		WithDataType(entity.FieldTypeInt64).
		WithIsPrimaryKey(true).
		WithIsAutoID(false),
	)
}

func ExampleSchema_WithField_varcharPK() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_id").
		WithDataType(entity.FieldTypeVarChar).
		WithIsPrimaryKey(true).
		WithMaxLength(100),
	)
}

func ExampleSchema_WithField_vectorField() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(5),
	)
}

func ExampleSchema_WithField_varcharField() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_varchar").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(512),
	)
}

func ExampleSchema_WithField_int64Field() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_int64").
		WithDataType(entity.FieldTypeInt64),
	)
}

func ExampleSchema_WithField_boolField() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_bool").
		WithDataType(entity.FieldTypeBool),
	)
}

func ExampleSchema_WithField_jsonField() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_json").
		WithDataType(entity.FieldTypeJSON),
	)
}

func ExampleSchema_WithField_arrayField() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().WithName("my_array").
		WithDataType(entity.FieldTypeArray).
		WithElementType(entity.FieldTypeInt64).
		WithMaxLength(512).
		WithMaxCapacity(5),
	)
}

func ExampleSchema_WithField_pkAndVector() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("pk").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(100),
	).WithField(entity.NewField().
		WithName("dense_vector").
		WithDataType(entity.FieldTypeFloatVector).
		WithDim(4),
	)
}

func ExampleSchema_WithField_binaryVector() {
	schema := entity.NewSchema()
	schema.WithField(entity.NewField().
		WithName("pk").
		WithDataType(entity.FieldTypeVarChar).
		WithMaxLength(100).
		WithIsAutoID(true),
	).WithField(entity.NewField().
		WithName("binary_vector").
		WithDataType(entity.FieldTypeBinaryVector).
		WithDim(128),
	)
}
