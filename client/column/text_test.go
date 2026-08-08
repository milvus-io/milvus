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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
)

func TestColumnTextRoundTrip(t *testing.T) {
	values := []string{"short text", "a much longer text value"}
	textColumn := NewColumnText("document", values)

	assert.Equal(t, entity.FieldTypeText, textColumn.Type())
	assert.Equal(t, values, textColumn.Data())

	fieldData := textColumn.FieldData()
	assert.Equal(t, schemapb.DataType_Text, fieldData.GetType())
	assert.Equal(t, values, fieldData.GetScalars().GetStringData().GetData())

	parsed, err := FieldDataColumn(fieldData, 0, -1)
	require.NoError(t, err)
	parsedText, ok := parsed.(*ColumnText)
	require.True(t, ok)
	assert.Equal(t, entity.FieldTypeText, parsedText.Type())
	assert.Equal(t, values, parsedText.Data())

	sliced, ok := textColumn.Slice(1, -1).(*ColumnText)
	require.True(t, ok)
	assert.Equal(t, values[1:], sliced.Data())
}

func TestNullableColumnTextRoundTrip(t *testing.T) {
	values := []string{"first", "third"}
	validData := []bool{true, false, true}
	textColumn, err := NewNullableColumnText("document", values, validData)
	require.NoError(t, err)

	fieldData := textColumn.FieldData()
	assert.Equal(t, schemapb.DataType_Text, fieldData.GetType())
	assert.Equal(t, values, fieldData.GetScalars().GetStringData().GetData())
	assert.Equal(t, validData, fieldData.GetValidData())

	parsed, err := FieldDataColumn(fieldData, 0, -1)
	require.NoError(t, err)
	parsedText, ok := parsed.(*ColumnText)
	require.True(t, ok)
	assert.Equal(t, entity.FieldTypeText, parsedText.Type())
	assert.Equal(t, 3, parsedText.Len())
	assert.Equal(t, values, parsedText.Data())
	isNull, err := parsedText.IsNull(1)
	require.NoError(t, err)
	assert.True(t, isNull)
}
