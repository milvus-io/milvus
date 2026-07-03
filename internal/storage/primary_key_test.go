package storage

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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

func TestUUIDPrimaryKey(t *testing.T) {
	uuid1 := uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	uuid2 := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	uuid3 := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

	t.Run("comparisons", func(t *testing.T) {
		pk1 := &UUIDPrimaryKey{Value: uuid1}
		pk2 := &UUIDPrimaryKey{Value: uuid2}
		pk3 := &UUIDPrimaryKey{Value: uuid3}

		// Byte-order: uuid2 (0x55) < uuid3 (0x6b) < uuid1 (0xa0)

		// EQ
		assert.True(t, pk1.EQ(pk1))
		assert.True(t, pk2.EQ(pk2))
		assert.False(t, pk1.EQ(pk2))

		// GT
		assert.True(t, pk1.GT(pk3))
		assert.True(t, pk3.GT(pk2))
		assert.False(t, pk2.GT(pk3))
		assert.False(t, pk1.GT(pk1))

		// GE
		assert.True(t, pk1.GE(pk3))
		assert.True(t, pk1.GE(pk1))
		assert.False(t, pk2.GE(pk1))

		// LT
		assert.True(t, pk2.LT(pk3))
		assert.True(t, pk3.LT(pk1))
		assert.False(t, pk1.LT(pk3))
		assert.False(t, pk2.LT(pk2))

		// LE
		assert.True(t, pk2.LE(pk3))
		assert.True(t, pk2.LE(pk2))
		assert.False(t, pk1.LE(pk2))
	})

	t.Run("marshalJSON", func(t *testing.T) {
		pk := &UUIDPrimaryKey{Value: uuid1}
		blob, err := json.Marshal(pk)
		assert.NoError(t, err)

		unmarshalled := &UUIDPrimaryKey{}
		err = json.Unmarshal(blob, unmarshalled)
		assert.NoError(t, err)
		assert.Equal(t, pk.Value, unmarshalled.Value)
	})

	t.Run("setValue", func(t *testing.T) {
		// From string
		pk := &UUIDPrimaryKey{}
		err := pk.SetValue("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
		assert.NoError(t, err)
		assert.Equal(t, uuid1, pk.Value)

		// From uuid.UUID
		pk2 := &UUIDPrimaryKey{}
		err = pk2.SetValue(uuid1)
		assert.NoError(t, err)
		assert.Equal(t, uuid1, pk2.Value)

		// From []byte
		pk3 := &UUIDPrimaryKey{}
		bin, _ := uuid1.MarshalBinary()
		err = pk3.SetValue(bin)
		assert.NoError(t, err)
		assert.Equal(t, uuid1, pk3.Value)

		// Invalid type
		pk4 := &UUIDPrimaryKey{}
		err = pk4.SetValue(123)
		assert.Error(t, err)
	})

	t.Run("type", func(t *testing.T) {
		pk := &UUIDPrimaryKey{Value: uuid1}
		assert.Equal(t, schemapb.DataType_UUID, pk.Type())
	})

	t.Run("size", func(t *testing.T) {
		pk := &UUIDPrimaryKey{Value: uuid1}
		assert.EqualValues(t, 16, pk.Size())
	})

	t.Run("genByRawData", func(t *testing.T) {
		// From string
		pk, err := GenPrimaryKeyByRawData("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", schemapb.DataType_UUID)
		assert.NoError(t, err)
		assert.Equal(t, uuid1, pk.(*UUIDPrimaryKey).Value)

		// From []byte
		bin, _ := uuid1.MarshalBinary()
		pk2, err := GenPrimaryKeyByRawData(bin, schemapb.DataType_UUID)
		assert.NoError(t, err)
		assert.Equal(t, uuid1, pk2.(*UUIDPrimaryKey).Value)

		// Invalid raw data type
		_, err = GenPrimaryKeyByRawData(123, schemapb.DataType_UUID)
		assert.Error(t, err)
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
