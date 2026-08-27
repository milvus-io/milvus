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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/entity"
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
		assert.NoError(t, err)
		_, err = IDColumns(strPKCol, nil, 0, -1)
		assert.NoError(t, err)

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

	t.Run("uuid ids", func(t *testing.T) {
		uuidPKCol := entity.NewSchema().WithField(
			entity.NewField().WithName("pk").WithIsPrimaryKey(true).WithDataType(entity.FieldTypeUUID),
		)

		uuids := []string{
			"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			"550e8400-e29b-41d4-a716-446655440000",
			"6ba7b810-9dad-11d1-80b4-00c04fd430c8",
		}
		idField := &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{
				StrId: &schemapb.StringArray{
					Data: uuids,
				},
			},
		}
		column, err := IDColumns(uuidPKCol, idField, 0, len(uuids))
		assert.Nil(t, err)
		assert.NotNil(t, column)
		assert.Equal(t, len(uuids), column.Len())
		uuidCol, ok := column.(*ColumnUUID)
		assert.True(t, ok)
		assert.Equal(t, uuids, uuidCol.Data())

		// test -1 method
		column, err = IDColumns(uuidPKCol, idField, 0, -1)
		assert.Nil(t, err)
		assert.NotNil(t, column)
		assert.Equal(t, len(uuids), column.Len())
	})
}

func TestFieldDataColumn_UUID(t *testing.T) {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_UUID,
		FieldName: "uuid_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{
							"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
							"550e8400-e29b-41d4-a716-446655440000",
						},
					},
				},
			},
		},
	}
	column, err := FieldDataColumn(fd, 0, -1)
	assert.NoError(t, err)
	assert.NotNil(t, column)
	assert.Equal(t, "uuid_field", column.Name())
	assert.Equal(t, entity.FieldTypeUUID, column.Type())
	assert.Equal(t, 2, column.Len())
	uuidCol, ok := column.(*ColumnUUID)
	if assert.True(t, ok) {
		assert.Equal(t, []string{
			"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
			"550e8400-e29b-41d4-a716-446655440000",
		}, uuidCol.Data())
	}
}

func TestFieldDataColumn_UUID_Nullable(t *testing.T) {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_UUID,
		FieldName: "uuid_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{
							"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
							"",
						},
					},
				},
			},
		},
		ValidData: []bool{true, false},
	}
	column, err := FieldDataColumn(fd, 0, -1)
	assert.NoError(t, err)
	assert.NotNil(t, column)
	assert.Equal(t, "uuid_field", column.Name())
	assert.Equal(t, entity.FieldTypeUUID, column.Type())
	assert.Equal(t, 2, column.Len())
}

func TestFieldDataColumn_UUID_Slice(t *testing.T) {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_UUID,
		FieldName: "uuid_field",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: []string{
							"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
							"550e8400-e29b-41d4-a716-446655440000",
						},
					},
				},
			},
		},
	}
	column, err := FieldDataColumn(fd, 0, 1)
	assert.NoError(t, err)
	assert.Equal(t, 1, column.Len())
	uuidCol, ok := column.(*ColumnUUID)
	if assert.True(t, ok) {
		assert.Equal(t, []string{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}, uuidCol.Data())
	}
}

func TestFieldDataColumnValidDataSources(t *testing.T) {
	validData := []bool{true, false}
	makeField := func(legacy, current []bool) *schemapb.FieldData {
		return &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: "value",
			ValidData: legacy,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					ValidData: current,
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 0}},
					},
				},
			},
		}
	}

	for _, test := range []struct {
		name    string
		legacy  []bool
		current []bool
		wantErr bool
	}{
		{name: "legacy fallback", legacy: validData},
		{name: "field-specific", current: validData},
		{name: "matching dual sources", legacy: validData, current: validData},
		{name: "mismatched dual sources", legacy: validData, current: []bool{false, true}, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			field := makeField(test.legacy, test.current)
			col, err := FieldDataColumn(field, 0, -1)
			if test.wantErr {
				assert.Error(t, err)
				assert.Nil(t, col)
				return
			}
			assert.NoError(t, err)
			assert.NotNil(t, col)
			assert.Nil(t, field.GetValidData())
			assert.Equal(t, validData, field.GetScalars().GetValidData())
		})
	}
}

func TestValidateAndNormalizeFieldDataValidDataRejectsNestedMismatch(t *testing.T) {
	legacy := []bool{true, false}
	current := []bool{false, true}
	subField := &schemapb.FieldData{
		ValidData: legacy,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{ValidData: current},
		},
	}
	field := &schemapb.FieldData{
		Field: &schemapb.FieldData_StructArrays{
			StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{subField}},
		},
	}

	assert.False(t, validateAndNormalizeFieldDataValidData(field))
	assert.Equal(t, legacy, subField.GetValidData())
	assert.Equal(t, current, subField.GetScalars().GetValidData())
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

func TestFieldDataColumnRejectsInvalidRange(t *testing.T) {
	fd := &schemapb.FieldData{
		Type:      schemapb.DataType_Int64,
		FieldName: "age",
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 20, 30}},
				},
			},
		},
	}

	column, err := FieldDataColumn(fd, 1, -1)
	assert.NoError(t, err)
	assert.Equal(t, 2, column.Len())

	for _, tc := range []struct {
		name       string
		begin, end int
	}{
		{name: "negative begin", begin: -1, end: 1},
		{name: "begin past rows", begin: 4, end: 4},
		{name: "unsupported negative end", begin: 0, end: -2},
		{name: "end past rows", begin: 0, end: 4},
		{name: "begin after end", begin: 2, end: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FieldDataColumn(fd, tc.begin, tc.end)
			assert.Error(t, err)
		})
	}
}
