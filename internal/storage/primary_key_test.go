package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

	t.Run("uuid pk", func(t *testing.T) {
		u1, _ := typeutil.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
		u2, _ := typeutil.ParseUUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
		pkValues := [][]byte{u1[:], u2[:]}
		var fieldData *schemapb.FieldData

		// test nil fieldData
		_, err := ParseFieldData2PrimaryKeys(fieldData)
		assert.Error(t, err)

		// test nil scalar data
		fieldData = &schemapb.FieldData{
			FieldName: "UUIDField",
		}
		_, err = ParseFieldData2PrimaryKeys(fieldData)
		assert.Error(t, err)

		// test parse success
		fieldData.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BytesData{
					BytesData: &schemapb.BytesArray{
						Data: pkValues,
					},
				},
			},
		}
		fieldData.Type = schemapb.DataType_UUID
		testPks := []PrimaryKey{
			NewUUIDPrimaryKey(u1),
			NewUUIDPrimaryKey(u2),
		}

		pks, err := ParseFieldData2PrimaryKeys(fieldData)
		assert.NoError(t, err)
		assert.ElementsMatch(t, pks, testPks)
	})
}

func TestUUIDPrimaryKey(t *testing.T) {
	u1, _ := typeutil.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	u2, _ := typeutil.ParseUUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
	pk1 := NewUUIDPrimaryKey(u1)
	pk2 := NewUUIDPrimaryKey(u2)

	assert.EqualValues(t, 24, pk1.Size())

	// test EQ, GE, LE with same value
	testPk1 := NewUUIDPrimaryKey(u1)
	assert.True(t, pk1.EQ(testPk1))
	assert.True(t, pk1.GE(testPk1))
	assert.True(t, pk1.LE(testPk1))

	// test LT / GT across u1 and u2 (u1 < u2 in byte order)
	assert.True(t, pk1.LT(pk2))
	assert.True(t, pk2.GT(pk1))
	assert.False(t, pk1.GT(pk2))
	assert.False(t, pk2.LT(pk1))

	t.Run("unmarshal", func(t *testing.T) {
		blob, err := json.Marshal(pk1)
		assert.NoError(t, err)

		unmarshalledPk := &UUIDPrimaryKey{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.NoError(t, err)
		assert.Equal(t, pk1.Value, unmarshalledPk.Value)
	})
}

func TestParsePrimaryKeysAndIDs(t *testing.T) {
	u1, _ := typeutil.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	u2, _ := typeutil.ParseUUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")

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
		{
			pks: []PrimaryKey{NewUUIDPrimaryKey(u1), NewUUIDPrimaryKey(u2)},
		},
	}

	for _, c := range testCases {
		ids, err := ParsePrimaryKeys2IDs(c.pks)
		assert.NoError(t, err)
		testPks, err := ParseIDs2PrimaryKeys(ids)
		assert.NoError(t, err)
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

		u1, _ := typeutil.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
		u2, _ := typeutil.ParseUUID("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")
		uuidPks := NewUUIDPrimaryKeys(2)
		uuidPks.AppendRaw(u1, u2)

		ids, err = ParsePrimaryKeysBatch2IDs(uuidPks)
		assert.NoError(t, err)
		assert.ElementsMatch(t, [][]byte{u1[:], u2[:]}, ids.GetUuidId().GetData())

		parsedPks := ParseIDs2PrimaryKeysBatch(ids)
		assert.Equal(t, 2, parsedPks.Len())
	})

	t.Run("unsupport_type", func(t *testing.T) {
		intPks := NewInt64PrimaryKeys(3)
		intPks.AppendRaw(1, 2, 3)

		_, err := ParsePrimaryKeysBatch2IDs(&badPks{PrimaryKeys: intPks})
		assert.Error(t, err)
	})
}

func TestParseIDs2PrimaryKeys_LengthPreservation(t *testing.T) {
	validU, _ := typeutil.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	ids := &schemapb.IDs{
		IdField: &schemapb.IDs_UuidId{
			UuidId: &schemapb.UUIDArray{
				Data: [][]byte{validU[:], {0x01, 0x02}, validU[:]},
			},
		},
	}
	pks, err := ParseIDs2PrimaryKeys(ids)
	assert.Error(t, err)
	assert.Nil(t, pks)
	ids2 := &schemapb.IDs{
		IdField: &schemapb.IDs_UuidId{
			UuidId: &schemapb.UUIDArray{
				Data: [][]byte{validU[:], validU[:]},
			},
		},
	}
	pks2, err := ParseIDs2PrimaryKeys(ids2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(pks2))
}
