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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestGenEmptyNestedArrayFieldData(t *testing.T) {
	field := &schemapb.FieldSchema{
		Name:        "nested",
		FieldID:     100,
		DataType:    schemapb.DataType_Array,
		ElementType: schemapb.DataType_Array,
		TypeSchema: &schemapb.TypeSchema{
			Kind: &schemapb.TypeSchema_ArrayElement{
				ArrayElement: &schemapb.TypeSchema{
					Kind: &schemapb.TypeSchema_ArrayElement{
						ArrayElement: &schemapb.TypeSchema{
							Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_Int32},
						},
					},
				},
			},
		},
	}

	data, err := GenEmptyFieldData(field)
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_Array, data.GetScalars().GetArrayData().GetElementType())
}

func TestGenEmptyScalarFieldDataLayout(t *testing.T) {
	for _, dtype := range []schemapb.DataType{schemapb.DataType_VarChar, schemapb.DataType_Text, schemapb.DataType_Int64, schemapb.DataType_Timestamptz} {
		t.Run(dtype.String(), func(t *testing.T) {
			schema := &schemapb.FieldSchema{Name: "value", FieldID: 100, DataType: dtype, IsDynamic: true}
			field, err := GenEmptyFieldData(schema)
			require.NoError(t, err)
			require.Equal(t, dtype, field.Type)
			require.Equal(t, schema.Name, field.FieldName)
			require.Equal(t, schema.FieldID, field.FieldId)
			require.True(t, field.IsDynamic)
			switch dtype {
			case schemapb.DataType_Int64:
				require.NotNil(t, field.GetScalars().GetLongData())
				require.Empty(t, field.GetScalars().GetLongData().GetData())
			case schemapb.DataType_Timestamptz:
				// Preserve the shared helper's historical empty-result layout.
				require.NotNil(t, field.GetScalars().GetLongData())
				require.Nil(t, field.GetScalars().GetTimestamptzData())
				require.Empty(t, field.GetScalars().GetLongData().GetData())
			default:
				require.NotNil(t, field.GetScalars().GetStringData())
				require.Empty(t, field.GetScalars().GetStringData().GetData())
			}
		})
	}
}
