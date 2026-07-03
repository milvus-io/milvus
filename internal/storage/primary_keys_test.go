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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

type PrimaryKeysSuite struct {
	suite.Suite
}

func (s *PrimaryKeysSuite) TestAppend() {
	s.Run("IntAppend", func() {
		intPks := NewInt64PrimaryKeys(10)
		s.Equal(schemapb.DataType_Int64, intPks.Type())
		s.EqualValues(0, intPks.Len())
		s.EqualValues(0, intPks.Size())

		err := intPks.Append(NewInt64PrimaryKey(1))
		s.NoError(err)
		s.EqualValues(1, intPks.Len())
		s.EqualValues(8, intPks.Size())

		val := intPks.Get(0)
		pk, ok := val.(*Int64PrimaryKey)
		s.Require().True(ok)
		s.EqualValues(1, pk.Value)

		err = intPks.Append(NewVarCharPrimaryKey("1"))
		s.Error(err)
	})

	s.Run("VarcharAppend", func() {
		strPks := NewVarcharPrimaryKeys(10)
		s.Equal(schemapb.DataType_VarChar, strPks.Type())
		s.EqualValues(0, strPks.Len())
		s.EqualValues(0, strPks.Size())

		err := strPks.Append(NewVarCharPrimaryKey("1"))
		s.NoError(err)
		s.EqualValues(1, strPks.Len())
		s.EqualValues(17, strPks.Size())
		val := strPks.Get(0)
		pk, ok := val.(*VarCharPrimaryKey)
		s.Require().True(ok)
		s.EqualValues("1", pk.Value)

		err = strPks.Append(NewInt64PrimaryKey(1))
		s.Error(err)
	})

	s.Run("IntMustAppend", func() {
		intPks := NewInt64PrimaryKeys(10)

		s.NotPanics(func() {
			intPks.MustAppend(NewInt64PrimaryKey(1))
		})
		s.Panics(func() {
			intPks.MustAppend(NewVarCharPrimaryKey("1"))
		})
	})

	s.Run("VarcharMustAppend", func() {
		strPks := NewVarcharPrimaryKeys(10)

		s.NotPanics(func() {
			strPks.MustAppend(NewVarCharPrimaryKey("1"))
		})
		s.Panics(func() {
			strPks.MustAppend(NewInt64PrimaryKey(1))
		})
	})

	s.Run("UUIDAppend", func() {
		uuid1 := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
		uuidPks := NewUUIDPrimaryKeys()
		s.Equal(schemapb.DataType_UUID, uuidPks.Type())
		s.EqualValues(0, uuidPks.Len())
		s.EqualValues(0, uuidPks.Size())

		err := uuidPks.Append(&UUIDPrimaryKey{Value: uuid1})
		s.NoError(err)
		s.EqualValues(1, uuidPks.Len())
		s.EqualValues(1, uuidPks.Size())

		val := uuidPks.Get(0)
		pk, ok := val.(*UUIDPrimaryKey)
		s.Require().True(ok)
		s.Equal(uuid1, pk.Value)

		err = uuidPks.Append(NewInt64PrimaryKey(1))
		s.Error(err)
	})

	s.Run("UUIDMustAppend", func() {
		uuid1 := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
		uuidPks := NewUUIDPrimaryKeys()

		s.NotPanics(func() {
			uuidPks.MustAppend(&UUIDPrimaryKey{Value: uuid1})
		})
		s.Panics(func() {
			uuidPks.MustAppend(NewInt64PrimaryKey(1))
		})
	})
}

func (s *PrimaryKeysSuite) TestMustMerge() {
	s.Run("IntPksMustMerge", func() {
		intPks := NewInt64PrimaryKeys(10)
		intPks.AppendRaw(1, 2, 3)

		anotherPks := NewInt64PrimaryKeys(10)
		anotherPks.AppendRaw(4, 5, 6)

		strPks := NewVarcharPrimaryKeys(10)
		strPks.AppendRaw("1", "2", "3")

		s.NotPanics(func() {
			intPks.MustMerge(anotherPks)

			s.Equal(6, intPks.Len())
		})

		s.Panics(func() {
			intPks.MustMerge(strPks)
		})
	})

	s.Run("StrPksMustMerge", func() {
		strPks := NewVarcharPrimaryKeys(10)
		strPks.AppendRaw("1", "2", "3")
		intPks := NewInt64PrimaryKeys(10)
		intPks.AppendRaw(1, 2, 3)
		anotherPks := NewVarcharPrimaryKeys(10)
		anotherPks.AppendRaw("4", "5", "6")

		s.NotPanics(func() {
			strPks.MustMerge(anotherPks)
			s.Equal(6, strPks.Len())
		})

		s.Panics(func() {
			strPks.MustMerge(intPks)
		})
	})

	s.Run("UUIDPksMustMerge", func() {
		uuid1 := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
		uuid2 := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

		uuidPks := NewUUIDPrimaryKeys()
		uuidPks.MustAppend(&UUIDPrimaryKey{Value: uuid1})
		uuidPks.MustAppend(&UUIDPrimaryKey{Value: uuid2})

		anotherPks := NewUUIDPrimaryKeys()
		anotherPks.MustAppend(&UUIDPrimaryKey{Value: uuid2})

		intPks := NewInt64PrimaryKeys(10)
		intPks.AppendRaw(1, 2, 3)

		s.NotPanics(func() {
			uuidPks.MustMerge(anotherPks)
			s.Equal(3, uuidPks.Len())
		})

		s.Panics(func() {
			uuidPks.MustMerge(intPks)
		})
	})
}

func TestUUIDPrimaryKeysGet(t *testing.T) {
	uuid1 := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	uuid2 := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	pks := NewUUIDPrimaryKeys()
	pks.MustAppend(&UUIDPrimaryKey{Value: uuid1})
	pks.MustAppend(&UUIDPrimaryKey{Value: uuid2})

	assert.Equal(t, schemapb.DataType_UUID, pks.Type())
	assert.Equal(t, 2, pks.Len())

	pk0 := pks.Get(0)
	assert.Equal(t, uuid1, pk0.(*UUIDPrimaryKey).Value)

	pk1 := pks.Get(1)
	assert.Equal(t, uuid2, pk1.(*UUIDPrimaryKey).Value)

	// Out of range
	assert.Nil(t, pks.Get(-1))
	assert.Nil(t, pks.Get(2))
}

func TestUUIDPrimaryKeysReset(t *testing.T) {
	uuid1 := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	pks := NewUUIDPrimaryKeys()
	pks.MustAppend(&UUIDPrimaryKey{Value: uuid1})
	assert.Equal(t, 1, pks.Len())

	pks.Reset()
	assert.Equal(t, 0, pks.Len())
	assert.Equal(t, 0, int(pks.Size()))
}

func TestPrimaryKeys(t *testing.T) {
	suite.Run(t, new(PrimaryKeysSuite))
}
