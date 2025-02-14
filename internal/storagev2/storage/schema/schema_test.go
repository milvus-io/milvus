// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema

import (
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/stretchr/testify/assert"
)

// Test Schema.Schema
func TestBuildSchema(t *testing.T) {
	pkField := arrow.Field{
		Name:     "pk_field",
		Type:     arrow.DataType(&arrow.Int64Type{}),
		Nullable: false,
	}
	vsField := arrow.Field{
		Name:     "vs_field",
		Type:     arrow.DataType(&arrow.Int64Type{}),
		Nullable: false,
	}
	vecField := arrow.Field{
		Name:     "vec_field",
		Type:     arrow.DataType(&arrow.FixedSizeBinaryType{ByteWidth: 16}),
		Nullable: false,
	}
	fields := []arrow.Field{pkField, vsField, vecField}

	as := arrow.NewSchema(fields, nil)
	schemaOptions := &SchemaOptions{
		PrimaryColumn: "pk_field",
		VersionColumn: "vs_field",
		VectorColumn:  "vec_field",
	}

	sc := NewSchema(as, schemaOptions)
	err := sc.Validate()
	assert.NoError(t, err)
}
