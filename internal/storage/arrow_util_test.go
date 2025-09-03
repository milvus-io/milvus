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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestGenerateEmptyArray(t *testing.T) {
	type testCase struct {
		tag         string
		field       *schemapb.FieldSchema
		expectErr   bool
		expectNull  bool
		expectValue any
	}

	cases := []testCase{
		{
			tag: "no_default_value",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int8,
				Nullable: true,
			},
			expectErr:  false,
			expectNull: true,
		},
		{
			tag: "int8",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int8,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 10,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: int8(10),
		},
		{
			tag: "int16",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int16,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 16,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: int16(16),
		},
		{
			tag: "int32",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int32,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 32,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: int32(32),
		},
		{
			tag: "int64",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int64,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_LongData{
						LongData: 64,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: int64(64),
		},
		{
			tag: "bool",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Bool,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_BoolData{
						BoolData: true,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: true,
		},
		{
			tag: "float",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Float,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_FloatData{
						FloatData: 0.1,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: float32(0.1),
		},
		{
			tag: "double",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Double,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_DoubleData{
						DoubleData: 1.2,
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: float64(1.2),
		},
		{
			tag: "varchar",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_VarChar,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_StringData{
						StringData: "varchar",
					},
				},
			},
			expectErr:   false,
			expectNull:  false,
			expectValue: "varchar",
		},
		{
			tag: "invalid_schema_datatype",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_FloatVector,
				Nullable: true,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 10,
					},
				},
			},
			expectErr: true,
		},
		{
			tag: "invalid_schema_nullable",
			field: &schemapb.FieldSchema{
				DataType: schemapb.DataType_Int8,
				Nullable: false,
				DefaultValue: &schemapb.ValueField{
					Data: &schemapb.ValueField_IntData{
						IntData: 10,
					},
				},
			},
			expectErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			rowNum := rand.Intn(100) + 1
			a, err := GenerateEmptyArrayFromSchema(tc.field, rowNum)
			switch {
			case tc.expectErr:
				assert.Error(t, err)
			case tc.expectNull:
				assert.NoError(t, err)
				assert.EqualValues(t, rowNum, a.Len())
				for i := range rowNum {
					assert.True(t, a.IsNull(i))
				}
			default:
				assert.NoError(t, err)
				assert.EqualValues(t, rowNum, a.Len())
				for i := range rowNum {
					value, ok := serdeMap[tc.field.DataType].deserialize(a, i, schemapb.DataType_None, 0, false)
					assert.True(t, a.IsValid(i))
					assert.True(t, ok)
					assert.Equal(t, tc.expectValue, value)
				}
			}
		})
	}
}
