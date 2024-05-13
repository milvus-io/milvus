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
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

func TestColumnVarChar(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	columnName := fmt.Sprintf("column_VarChar_%d", rand.Int())
	columnLen := 8 + rand.Intn(10)

	v := make([]string, columnLen)
	column := NewColumnVarChar(columnName, v)

	t.Run("test meta", func(t *testing.T) {
		ft := entity.FieldTypeVarChar
		assert.Equal(t, "VarChar", ft.Name())
		assert.Equal(t, "string", ft.String())
		pbName, pbType := ft.PbFieldType()
		assert.Equal(t, "VarChar", pbName)
		assert.Equal(t, "string", pbType)
	})

	t.Run("test column attribute", func(t *testing.T) {
		assert.Equal(t, columnName, column.Name())
		assert.Equal(t, entity.FieldTypeVarChar, column.Type())
		assert.Equal(t, columnLen, column.Len())
		assert.EqualValues(t, v, column.Data())
	})

	t.Run("test column field data", func(t *testing.T) {
		fd := column.FieldData()
		assert.NotNil(t, fd)
		assert.Equal(t, fd.GetFieldName(), columnName)
	})

	t.Run("test column value by idx", func(t *testing.T) {
		_, err := column.ValueByIdx(-1)
		assert.NotNil(t, err)
		_, err = column.ValueByIdx(columnLen)
		assert.NotNil(t, err)
		for i := 0; i < columnLen; i++ {
			v, err := column.ValueByIdx(i)
			assert.Nil(t, err)
			assert.Equal(t, column.values[i], v)
		}
	})
}

func TestFieldDataVarCharColumn(t *testing.T) {
	colLen := rand.Intn(10) + 8
	name := fmt.Sprintf("fd_VarChar_%d", rand.Int())
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldName: name,
	}

	t.Run("normal usage", func(t *testing.T) {
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: make([]string, colLen),
					},
				},
			},
		}
		column, err := FieldDataColumn(fd, 0, colLen)
		assert.Nil(t, err)
		assert.NotNil(t, column)

		assert.Equal(t, name, column.Name())
		assert.Equal(t, colLen, column.Len())
		assert.Equal(t, entity.FieldTypeVarChar, column.Type())

		var ev string
		err = column.AppendValue(ev)
		assert.Equal(t, colLen+1, column.Len())
		assert.Nil(t, err)

		err = column.AppendValue(struct{}{})
		assert.Equal(t, colLen+1, column.Len())
		assert.NotNil(t, err)
	})

	t.Run("nil data", func(t *testing.T) {
		fd.Field = nil
		_, err := FieldDataColumn(fd, 0, colLen)
		assert.NotNil(t, err)
	})

	t.Run("get all data", func(t *testing.T) {
		fd.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: make([]string, colLen),
					},
				},
			},
		}
		column, err := FieldDataColumn(fd, 0, -1)
		assert.Nil(t, err)
		assert.NotNil(t, column)

		assert.Equal(t, name, column.Name())
		assert.Equal(t, colLen, column.Len())
		assert.Equal(t, entity.FieldTypeVarChar, column.Type())
	})
}
