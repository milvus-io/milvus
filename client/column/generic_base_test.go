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

type GenericBaseSuite struct {
	suite.Suite
}

func (s *GenericBaseSuite) TestBasic() {
	name := fmt.Sprintf("test_%d", rand.Intn(10))
	gb := &genericColumnBase[int64]{
		name:      name,
		fieldType: entity.FieldTypeInt64,
		values:    []int64{1, 2, 3},
	}

	s.Equal(name, gb.Name())
	s.Equal(entity.FieldTypeInt64, gb.Type())
	s.EqualValues(3, gb.Len())

	err := gb.AppendValue("abc")
	s.Error(err)
	s.EqualValues(3, gb.Len())

	err = gb.AppendValue(int64(4))
	s.NoError(err)
	s.EqualValues(4, gb.Len())
}

func (s *GenericBaseSuite) TestIndexAccess() {
	name := fmt.Sprintf("test_%d", rand.Intn(10))
	values := []int64{1, 2, 3}
	gb := &genericColumnBase[int64]{
		name:      name,
		fieldType: entity.FieldTypeInt64,
		values:    values,
	}

	for idx, value := range values {
		v, err := gb.Value(idx)
		s.NoError(err)
		s.Equal(value, v)

		s.NotPanics(func() {
			v = gb.MustValue(idx)
		})
		s.Equal(value, v)
	}

	s.Panics(func() {
		gb.MustValue(-1)
	}, "out of range, negative index")

	s.Panics(func() {
		gb.MustValue(3)
	}, "out of range, LTE len")

	s.NotPanics(func() {
		_, err := gb.Value(-1)
		s.Error(err)

		_, err = gb.Value(3)
		s.Error(err)
	})
}

func (s *GenericBaseSuite) TestIndexAccess_Nullable() {
	name := fmt.Sprintf("test_%d", rand.Intn(10))

	s.Run("compact_mode", func() {
		values := []int64{1, 2, 3}
		validData := []bool{true, false, true, false, true}
		gb := &genericColumnBase[int64]{
			name:       name,
			fieldType:  entity.FieldTypeInt64,
			values:     values,
			nullable:   true,
			validData:  validData,
			sparseMode: false,
		}

		err := gb.ValidateNullable()
		s.NoError(err)

		for idx, valid := range validData {
			if valid {
				v, err := gb.Value(idx)
				s.NoError(err)
				s.Equal(values[gb.indexMapping[idx]], v)

				s.NotPanics(func() {
					v = gb.MustValue(idx)
				})
				s.Equal(values[gb.indexMapping[idx]], v)
			} else {
				result, err := gb.IsNull(idx)
				s.NoError(err)
				s.True(result)

				_, err = gb.Value(idx)
				s.Error(err)
			}
		}
	})

	s.Run("sparse_mode", func() {
		values := []int64{1, 0, 2, 0, 3}
		validData := []bool{true, false, true, false, true}
		gb := &genericColumnBase[int64]{
			name:       name,
			fieldType:  entity.FieldTypeInt64,
			values:     values,
			nullable:   true,
			validData:  validData,
			sparseMode: true,
		}

		err := gb.ValidateNullable()
		s.NoError(err)

		for idx, valid := range validData {
			if valid {
				v, err := gb.Value(idx)
				s.NoError(err)
				s.Equal(values[idx], v)

				s.NotPanics(func() {
					v = gb.MustValue(idx)
				})
				s.Equal(values[idx], v)
			} else {
				result, err := gb.IsNull(idx)
				s.NoError(err)
				s.True(result)

				_, err = gb.Value(idx)
				s.NoError(err)
			}
		}
	})
}

func (s *GenericBaseSuite) TestSlice() {
	name := fmt.Sprintf("test_%d", rand.Intn(10))
	values := []int64{1, 2, 3}
	gb := &genericColumnBase[int64]{
		name:      name,
		fieldType: entity.FieldTypeInt64,
		values:    values,
	}

	l := rand.Intn(3)
	another := gb.Slice(0, l)
	s.Equal(l, another.Len())
	agb, ok := another.(*genericColumnBase[int64])
	s.Require().True(ok)

	for i := 0; i < l; i++ {
		s.Equal(gb.MustValue(i), agb.MustValue(i))
	}

	s.NotPanics(func() {
		agb := gb.Slice(10, 10)
		s.Equal(0, agb.Len())
	})
}

func (s *GenericBaseSuite) TestFieldData() {
	name := fmt.Sprintf("test_%d", rand.Intn(10))
	values := []int64{1, 2, 3}
	gb := &genericColumnBase[int64]{
		name:      name,
		fieldType: entity.FieldTypeInt64,
		values:    values,
	}

	fd := gb.FieldData()
	s.Equal(name, fd.GetFieldName())
	s.Equal(schemapb.DataType_Int64, fd.GetType())
}

func (s *GenericBaseSuite) TestConversion() {
	name := fmt.Sprintf("test_%d", rand.Intn(10))
	values := []int64{1, 2, 3}
	gb := &genericColumnBase[int64]{
		name:      name,
		fieldType: entity.FieldTypeInt64,
		values:    values,
	}

	val, err := gb.GetAsInt64(0)
	s.NoError(err)
	s.EqualValues(1, val)

	_, err = gb.GetAsBool(0)
	s.Error(err)

	_, err = gb.GetAsString(0)
	s.Error(err)

	_, err = gb.GetAsDouble(0)
	s.Error(err)

	strValues := []string{"1", "2", "3"}
	strGb := &genericColumnBase[string]{
		name:      name,
		fieldType: entity.FieldTypeVarChar,
		values:    strValues,
	}
	sv, err := strGb.GetAsString(0)
	s.NoError(err)
	s.Equal("1", sv)
}

func (s *GenericBaseSuite) TestNullable() {
	name := fmt.Sprintf("test_%d", rand.Intn(10))
	var values []int64
	gb := &genericColumnBase[int64]{
		name:      name,
		fieldType: entity.FieldTypeInt64,
		values:    values,
	}

	s.False(gb.Nullable())
	s.NoError(gb.ValidateNullable())
	s.Error(gb.AppendNull())
	s.EqualValues(0, gb.Len())

	gb.SetNullable(true)
	s.NoError(gb.ValidateNullable())
	s.NoError(gb.AppendNull())
	s.EqualValues(1, gb.Len())

	gb.SetNullable(false)
	s.NoError(gb.ValidateNullable())
	s.EqualValues(0, gb.Len())

	gb = &genericColumnBase[int64]{
		name:       name,
		fieldType:  entity.FieldTypeInt64,
		values:     []int64{0},
		validData:  []bool{true, false},
		sparseMode: true,
		nullable:   true,
	}
	s.Error(gb.ValidateNullable())
}

func TestGenericBase(t *testing.T) {
	suite.Run(t, new(GenericBaseSuite))
}
