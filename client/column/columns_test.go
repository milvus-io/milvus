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
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

func TestIDColumns(t *testing.T) {
	dataLen := rand.Intn(100) + 1
	base := rand.Intn(5000) // id start point

	intPKCol := entity.NewSchema().WithField(
		entity.NewField().WithName("pk").WithIsPrimaryKey(true).WithDataType(entity.FieldTypeInt64),
	)
	strPKCol := entity.NewSchema().WithField(
		entity.NewField().WithName("pk").WithIsPrimaryKey(true).WithDataType(entity.FieldTypeVarChar),
	)

	t.Run("nil id", func(t *testing.T) {
		_, err := IDColumns(intPKCol, nil, 0, -1)
		assert.Error(t, err)
		_, err = IDColumns(strPKCol, nil, 0, -1)
		assert.Error(t, err)

		idField := &schemapb.IDs{}
		col, err := IDColumns(intPKCol, idField, 0, -1)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, col.Len())
		col, err = IDColumns(strPKCol, idField, 0, -1)
		assert.NoError(t, err)
		assert.EqualValues(t, 0, col.Len())
	})

	t.Run("int ids", func(t *testing.T) {
		ids := make([]int64, 0, dataLen)
		for i := 0; i < dataLen; i++ {
			ids = append(ids, int64(i+base))
		}
		idField := &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: ids,
				},
			},
		}
		column, err := IDColumns(intPKCol, idField, 0, dataLen)
		assert.Nil(t, err)
		assert.NotNil(t, column)
		assert.Equal(t, dataLen, column.Len())

		column, err = IDColumns(intPKCol, idField, 0, -1) // test -1 method
		assert.Nil(t, err)
		assert.NotNil(t, column)
		assert.Equal(t, dataLen, column.Len())
	})
	t.Run("string ids", func(t *testing.T) {
		ids := make([]string, 0, dataLen)
		for i := 0; i < dataLen; i++ {
			ids = append(ids, strconv.FormatInt(int64(i+base), 10))
		}
		idField := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: ids,
				},
			},
		}
		column, err := IDColumns(strPKCol, idField, 0, dataLen)
		assert.Nil(t, err)
		assert.NotNil(t, column)
		assert.Equal(t, dataLen, column.Len())

		column, err = IDColumns(strPKCol, idField, 0, -1) // test -1 method
		assert.Nil(t, err)
		assert.NotNil(t, column)
		assert.Equal(t, dataLen, column.Len())
	})
}

func TestGetIntData(t *testing.T) {
	type testCase struct {
		tag      string
		fd       *schemapb.FieldData
		expectOK bool
	}

	cases := []testCase{
		{
			tag: "normal_IntData",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}},
						},
					},
				},
			},
			expectOK: true,
		},
		{
			tag: "empty_LongData",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: nil},
						},
					},
				},
			},
			expectOK: true,
		},
		{
			tag: "nonempty_LongData",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
						},
					},
				},
			},
			expectOK: false,
		},
		{
			tag: "other_data",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{},
					},
				},
			},
			expectOK: false,
		},
		{
			tag: "vector_data",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Vectors{},
			},
			expectOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.tag, func(t *testing.T) {
			_, ok := getIntData(tc.fd)
			assert.Equal(t, tc.expectOK, ok)
		})
	}
}
