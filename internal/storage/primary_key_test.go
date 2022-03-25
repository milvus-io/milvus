package storage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringPrimaryKey(t *testing.T) {
	pk := &StringPrimaryKey{
		Value: "milvus",
	}

	testPk := &StringPrimaryKey{
		Value: "milvus",
	}

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

		unmarshalledPk := &StringPrimaryKey{}
		err = json.Unmarshal(blob, unmarshalledPk)
		assert.Nil(t, err)
		assert.Equal(t, pk.Value, unmarshalledPk.Value)
	})
}

func TestInt64PrimaryKey(t *testing.T) {
	pk := &Int64PrimaryKey{
		Value: 100,
	}

	testPk := &Int64PrimaryKey{
		Value: 100,
	}

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
