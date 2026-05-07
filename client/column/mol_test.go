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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

func TestColumnMolSmilesMethods(t *testing.T) {
	column := NewColumnMolSmiles("mol", []string{"CCO", "c1ccccc1"})

	assert.Equal(t, "mol", column.Name())
	assert.Equal(t, entity.FieldTypeMol, column.Type())
	assert.Equal(t, 2, column.Len())
	assert.Equal(t, []string{"CCO", "c1ccccc1"}, column.Data())
	assert.Equal(t, []string{"CCO", "c1ccccc1"}, column.FieldData().GetScalars().GetMolSmilesData().GetData())

	value, err := column.Get(0)
	require.NoError(t, err)
	assert.Equal(t, "CCO", value)

	stringValue, err := column.GetAsString(1)
	require.NoError(t, err)
	assert.Equal(t, "c1ccccc1", stringValue)

	valueByIdx, err := column.ValueByIdx(0)
	require.NoError(t, err)
	assert.Equal(t, "CCO", valueByIdx)

	_, err = column.Get(-1)
	assert.Error(t, err)
	_, err = column.Get(2)
	assert.Error(t, err)
	_, err = column.GetAsString(2)
	assert.Error(t, err)
	_, err = column.ValueByIdx(-1)
	assert.Error(t, err)

	sliced, ok := column.Slice(1, -1).(*ColumnMolSmiles)
	require.True(t, ok)
	assert.Equal(t, 1, sliced.Len())
	assert.Equal(t, []string{"c1ccccc1"}, sliced.Data())

	emptySlice, ok := column.Slice(10, 10).(*ColumnMolSmiles)
	require.True(t, ok)
	assert.Equal(t, 0, emptySlice.Len())

	err = column.AppendValue("N#N")
	require.NoError(t, err)
	assert.Equal(t, 3, column.Len())
	assert.Equal(t, []string{"CCO", "c1ccccc1", "N#N"}, column.Data())

	err = column.AppendValue([]byte("invalid"))
	assert.Error(t, err)
}

func TestNullableColumnMolSmilesCompactAccess(t *testing.T) {
	column, err := NewNullableColumnMolSmiles("mol", []string{"CCO", "N#N"}, []bool{true, false, true})
	require.NoError(t, err)

	assert.True(t, column.Nullable())
	assert.Equal(t, entity.FieldTypeMol, column.Type())
	assert.Equal(t, 3, column.Len())
	assert.Equal(t, []string{"CCO", "N#N"}, column.Data())

	value, err := column.GetAsString(0)
	require.NoError(t, err)
	assert.Equal(t, "CCO", value)

	_, err = column.GetAsString(1)
	assert.Error(t, err)

	value, err = column.GetAsString(2)
	require.NoError(t, err)
	assert.Equal(t, "N#N", value)

	err = column.AppendValue("CO")
	require.NoError(t, err)
	assert.Equal(t, 4, column.Len())

	value, err = column.GetAsString(3)
	require.NoError(t, err)
	assert.Equal(t, "CO", value)
}

func TestFieldDataColumnMolDataNullableSlice(t *testing.T) {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Mol,
		FieldName: "mol",
		ValidData: []bool{true, false, true},
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_MolData{
					MolData: &schemapb.MolArray{
						Data: [][]byte{[]byte("CCO"), []byte(""), []byte("N#N")},
					},
				},
			},
		},
	}

	result, err := FieldDataColumn(fd, 1, 3)
	require.NoError(t, err)

	column, ok := result.(*ColumnMolSmiles)
	require.True(t, ok)
	assert.Equal(t, 2, column.Len())
	assert.Equal(t, []string{"", "N#N"}, column.Data())

	isNull, err := column.IsNull(0)
	require.NoError(t, err)
	assert.True(t, isNull)

	value, err := column.GetAsString(1)
	require.NoError(t, err)
	assert.Equal(t, "N#N", value)
}
