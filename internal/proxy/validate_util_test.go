package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateCollectionName(t *testing.T) {
	Params.Init()
	assert.Nil(t, ValidateCollectionName("abc"))
	assert.Nil(t, ValidateCollectionName("_123abc"))
	assert.Nil(t, ValidateCollectionName("abc123_$"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, ValidateCollectionName(name))
	}
}

func TestValidatePartitionTag(t *testing.T) {
	Params.Init()
	assert.Nil(t, ValidatePartitionTag("abc", true))
	assert.Nil(t, ValidatePartitionTag("_123abc", true))
	assert.Nil(t, ValidatePartitionTag("abc123_$", true))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, ValidatePartitionTag(name, true))
	}

	assert.Nil(t, ValidatePartitionTag("ab cd", false))
	assert.Nil(t, ValidatePartitionTag("ab*", false))
}

func TestValidateFieldName(t *testing.T) {
	Params.Init()
	assert.Nil(t, ValidateFieldName("abc"))
	assert.Nil(t, ValidateFieldName("_123abc"))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
		"123abc",
		"$abc",
		"_12 ac",
		" ",
		"",
		string(longName),
		"中文",
	}

	for _, name := range invalidNames {
		assert.NotNil(t, ValidateFieldName(name))
	}
}

func TestValidateDimension(t *testing.T) {
	Params.Init()
	assert.Nil(t, ValidateDimension(1, false))
	assert.Nil(t, ValidateDimension(Params.MaxDimension(), false))
	assert.Nil(t, ValidateDimension(8, true))
	assert.Nil(t, ValidateDimension(Params.MaxDimension(), true))

	// invalid dim
	assert.NotNil(t, ValidateDimension(-1, false))
	assert.NotNil(t, ValidateDimension(Params.MaxDimension()+1, false))
	assert.NotNil(t, ValidateDimension(9, true))
}
