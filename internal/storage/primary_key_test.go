package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
)

func TestVarCharPrimaryKey(t *testing.T) {
	t.Run("size", func(t *testing.T) {
		longString := "The High-Performance Vector Database Built for Scale"
		pk := NewVarCharPrimaryKey(longString)
		gotSize := pk.Size()
		expectSize := len(longString) + 8

		assert.EqualValues(t, expectSize, gotSize)
	})

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
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))

	// test LT
	err = testPk.SetValue("mivlut")
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &VarCharPrimaryKey{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
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
	assert.NoError(t, err)
	assert.Equal(t, true, pk.GT(testPk))

	// test LT
	err = testPk.SetValue(int64(200))
	assert.NoError(t, err)
	assert.Equal(t, true, pk.LT(testPk))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalledPk := &Int64PrimaryKey{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
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
		assert.NoError(t, err)

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
		assert.NoError(t, err)

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

type badPks struct {
	PrimaryKeys
}

func (pks *badPks) Type() schemapb.DataType {
	return schemapb.DataType_None
}

func TestParsePrimaryKeysBatch2IDs(t *testing.T) {
	t.Run("success_cases", func(t *testing.T) {
		intPks := NewInt64PrimaryKeys(3)
		intPks.AppendRaw(1, 2, 3)

		ids, err := ParsePrimaryKeysBatch2IDs(intPks)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []int64{1, 2, 3}, ids.GetIntId().GetData())

		strPks := NewVarcharPrimaryKeys(3)
		strPks.AppendRaw("1", "2", "3")

		ids, err = ParsePrimaryKeysBatch2IDs(strPks)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []string{"1", "2", "3"}, ids.GetStrId().GetData())
	})

	t.Run("unsupport_type", func(t *testing.T) {
		intPks := NewInt64PrimaryKeys(3)
		intPks.AppendRaw(1, 2, 3)

		_, err := ParsePrimaryKeysBatch2IDs(&badPks{PrimaryKeys: intPks})
		assert.Error(t, err)
	})
}
