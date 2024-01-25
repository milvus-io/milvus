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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestVarCharFieldValue(t *testing.T) {
	pk := NewVarCharFieldValue("milvus")

	testPk := NewVarCharFieldValue("milvus")

	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	err := testPk.SetValue(1.0)
	assert.Error(t, err)

	// test GT
	err = testPk.SetValue("bivlus")
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))

	// test LT
	err = testPk.SetValue("mivlut")
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &VarCharFieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestInt64FieldValue(t *testing.T) {
	pk := NewInt64FieldValue(100)

	testPk := NewInt64FieldValue(100)
	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, true, testPk.GE(pk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, true, testPk.LE(pk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	err := testPk.SetValue(1.0)
	assert.Error(t, err)

	// test GT
	err = testPk.SetValue(int64(10))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))
	assert.Equal(t, false, testPk.GT(pk))
	assert.Equal(t, true, pk.GE(testPk))
	assert.Equal(t, false, testPk.GE(pk))

	// test LT
	err = testPk.SetValue(int64(200))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))
	assert.Equal(t, false, testPk.LT(pk))
	assert.Equal(t, true, pk.LE(testPk))
	assert.Equal(t, false, testPk.LE(pk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &Int64FieldValue{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestParseFieldData2FieldValues(t *testing.T) {
	t.Run("int64 pk", func(t *testing.T) {
		pkValues := []int64{1, 2}
		var fieldData *schemapb.FieldData

		// test nil fieldData
		_, err := ParseFieldData2FieldValues(fieldData)
		assert.Error(t, err)

		// test nil scalar data
		fieldData = &schemapb.FieldData{
			FieldName: "int64Field",
		}
		_, err = ParseFieldData2FieldValues(fieldData)
		assert.Error(t, err)

		// test invalid pk type
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: pkValues,
					},
				},
			},
		}
		_, err = ParseFieldData2FieldValues(fieldData)
		assert.Error(t, err)

		// test parse success
		fieldData.Type = schemapb.DataType_Int64
		testPks := make([]ScalarFieldValue, len(pkValues))
		for index, v := range pkValues {
			testPks[index] = NewInt64FieldValue(v)
		}

		pks, err := ParseFieldData2FieldValues(fieldData)
		assert.NoError(t, err)

		assert.ElementsMatch(t, pks, testPks)
	})

	t.Run("varChar pk", func(t *testing.T) {
		pkValues := []string{"test1", "test2"}
		var fieldData *schemapb.FieldData

		// test nil fieldData
		_, err := ParseFieldData2FieldValues(fieldData)
		assert.Error(t, err)

		// test nil scalar data
		fieldData = &schemapb.FieldData{
			FieldName: "VarCharField",
		}
		_, err = ParseFieldData2FieldValues(fieldData)
		assert.Error(t, err)

		// test invalid pk type
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: pkValues,
					},
				},
			},
		}
		_, err = ParseFieldData2FieldValues(fieldData)
		assert.Error(t, err)

		// test parse success
		fieldData.Type = schemapb.DataType_VarChar
		testPks := make([]ScalarFieldValue, len(pkValues))
		for index, v := range pkValues {
			testPks[index] = NewVarCharFieldValue(v)
		}

		pks, err := ParseFieldData2FieldValues(fieldData)
		assert.NoError(t, err)

		assert.ElementsMatch(t, pks, testPks)
	})
}

func TestParseFieldValuesAndIDs(t *testing.T) {
	type testCase struct {
		pks []ScalarFieldValue
	}
	testCases := []testCase{
		{
			pks: []ScalarFieldValue{NewInt64FieldValue(1), NewInt64FieldValue(2)},
		},
		{
			pks: []ScalarFieldValue{NewVarCharFieldValue("test1"), NewVarCharFieldValue("test2")},
		},
	}

	for _, c := range testCases {
		ids := ParseFieldValues2IDs(c.pks)
		testPks := ParseIDs2FieldValues(ids)
		assert.ElementsMatch(t, c.pks, testPks)
	}
}

func TestGenFieldValue(t *testing.T) {
	_, err := GenInt64FieldValues(1, 2, 3)
	require.NoError(t, err)

	_, err2 := GenVarcharFieldValues("a", "b", "c")
	require.NoError(t, err2)
}

func TestFieldValueSize(t *testing.T) {
	pk := NewVarCharFieldValue("milvus")
	assert.Equal(t, int64(56), pk.Size())

	pk2 := NewInt64FieldValue(100)
	assert.Equal(t, int64(16), pk2.Size())
}
