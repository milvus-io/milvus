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

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v2/entity"
)

type ColumnJSONBytesSuite struct {
	suite.Suite
}

func (s *ColumnJSONBytesSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *ColumnJSONBytesSuite) TestAttrMethods() {
	columnName := fmt.Sprintf("column_jsonbs_%d", rand.Int())
	columnLen := 8 + rand.Intn(10)

	v := make([][]byte, columnLen)
	column := NewColumnJSONBytes(columnName, v).WithIsDynamic(true)

	s.Run("test_meta", func() {
		ft := entity.FieldTypeJSON
		s.Equal("JSON", ft.Name())
		s.Equal("JSON", ft.String())
		pbName, pbType := ft.PbFieldType()
		s.Equal("JSON", pbName)
		s.Equal("JSON", pbType)
	})

	s.Run("test_column_attribute", func() {
		s.Equal(columnName, column.Name())
		s.Equal(entity.FieldTypeJSON, column.Type())
		s.Equal(columnLen, column.Len())
		s.EqualValues(v, column.Data())
	})

	s.Run("test_column_field_data", func() {
		fd := column.FieldData()
		s.NotNil(fd)
		s.Equal(fd.GetFieldName(), columnName)
	})

	s.Run("test_column_valuer_by_idx", func() {
		_, err := column.ValueByIdx(-1)
		s.Error(err)
		_, err = column.ValueByIdx(columnLen)
		s.Error(err)
		for i := 0; i < columnLen; i++ {
			v, err := column.ValueByIdx(i)
			s.NoError(err)
			s.Equal(column.values[i], v)
		}
	})

	s.Run("test_append_value", func() {
		item := make([]byte, 10)
		err := column.AppendValue(item)
		s.NoError(err)
		s.Equal(columnLen+1, column.Len())
		val, err := column.ValueByIdx(columnLen)
		s.NoError(err)
		s.Equal(item, val)

		err = column.AppendValue(&struct{ Tag string }{Tag: "abc"})
		s.NoError(err)

		err = column.AppendValue(map[string]interface{}{"Value": 123})
		s.NoError(err)

		err = column.AppendValue(1)
		s.Error(err)
	})
}

func TestColumnJSONBytes(t *testing.T) {
	suite.Run(t, new(ColumnJSONBytesSuite))
}
