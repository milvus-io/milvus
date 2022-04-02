package storage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

func TestVarCharPrimaryKey(t *testing.T) {
	pk := NewVarCharPrimaryKey("milvus")

	testPk := NewVarCharPrimaryKey("milvus")

	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	// test GT
	err := testPk.SetValue("bivlus")
	assert.Nil(t, err)
	assert.Equal(t, true, pk.GT(testPk))

	// test LT
	err = testPk.SetValue("mivlut")
	assert.Nil(t, err)
	assert.Equal(t, true, pk.LT(testPk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.Nil(t, err)

		unmarshalledPk := &VarCharPrimaryKey{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.Nil(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestInt64PrimaryKey(t *testing.T) {
	pk := NewInt64PrimaryKey(100)

	testPk := NewInt64PrimaryKey(100)
	// test GE
	assert.Equal(t, true, pk.GE(testPk))
	// test LE
	assert.Equal(t, true, pk.LE(testPk))
	// test EQ
	assert.Equal(t, true, pk.EQ(testPk))

	// test GT
	err := testPk.SetValue(int64(10))
	assert.Nil(t, err)
	assert.Equal(t, true, pk.GT(testPk))

	// test LT
	err = testPk.SetValue(int64(200))
	assert.Nil(t, err)
	assert.Equal(t, true, pk.LT(testPk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.Nil(t, err)

		unmarshalledPk := &Int64PrimaryKey{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.Nil(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestParseFieldData2PrimaryKeys(t *testing.T) {
	t.Run("int64 pk", func(t *testing.T) {
		pkValues := []int64{1, 2}
		var fieldData *schemapb.FieldData

		// test nil fieldData
		_, err := ParseFieldData2PrimaryKeys(fieldData)
		assert.Error(t, err)

		// test nil scalar data
		fieldData = &schemapb.FieldData{
			FieldName: "int64Field",
		}
		_, err = ParseFieldData2PrimaryKeys(fieldData)
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
		_, err = ParseFieldData2PrimaryKeys(fieldData)
		assert.Error(t, err)

		// test parse success
		fieldData.Type = schemapb.DataType_Int64
		testPks := make([]PrimaryKey, len(pkValues))
		for index, v := range pkValues {
			testPks[index] = NewInt64PrimaryKey(v)
		}

		pks, err := ParseFieldData2PrimaryKeys(fieldData)
		assert.Nil(t, err)

		assert.ElementsMatch(t, pks, testPks)
	})

	t.Run("varChar pk", func(t *testing.T) {
		pkValues := []string{"test1", "test2"}
		var fieldData *schemapb.FieldData

		// test nil fieldData
		_, err := ParseFieldData2PrimaryKeys(fieldData)
		assert.Error(t, err)

		// test nil scalar data
		fieldData = &schemapb.FieldData{
			FieldName: "VarCharField",
		}
		_, err = ParseFieldData2PrimaryKeys(fieldData)
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
		_, err = ParseFieldData2PrimaryKeys(fieldData)
		assert.Error(t, err)

		// test parse success
		fieldData.Type = schemapb.DataType_VarChar
		testPks := make([]PrimaryKey, len(pkValues))
		for index, v := range pkValues {
			testPks[index] = NewVarCharPrimaryKey(v)
		}

		pks, err := ParseFieldData2PrimaryKeys(fieldData)
		assert.Nil(t, err)

		assert.ElementsMatch(t, pks, testPks)
	})
}

func TestParsePrimaryKeysAndIDs(t *testing.T) {
	type testCase struct {
		pks []PrimaryKey
	}
	testCases := []testCase{
		{
			pks: []PrimaryKey{NewInt64PrimaryKey(1), NewInt64PrimaryKey(2)},
		},
		{
			pks: []PrimaryKey{NewVarCharPrimaryKey("test1"), NewVarCharPrimaryKey("test2")},
		},
	}

	for _, c := range testCases {
		ids := ParsePrimaryKeys2IDs(c.pks)
		testPks := ParseIDs2PrimaryKeys(ids)
		assert.ElementsMatch(t, c.pks, testPks)
	}
}
