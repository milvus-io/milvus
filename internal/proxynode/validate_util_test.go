package proxynode

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

func TestValidateCollectionName(t *testing.T) {
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
	assert.Nil(t, ValidatePartitionTag("abc", true))
	assert.Nil(t, ValidatePartitionTag("123abc", true))
	assert.Nil(t, ValidatePartitionTag("_123abc", true))
	assert.Nil(t, ValidatePartitionTag("abc123_$", true))

	longName := make([]byte, 256)
	for i := 0; i < len(longName); i++ {
		longName[i] = 'a'
	}
	invalidNames := []string{
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
	assert.Nil(t, ValidateDimension(1, false))
	assert.Nil(t, ValidateDimension(Params.MaxDimension(), false))
	assert.Nil(t, ValidateDimension(8, true))
	assert.Nil(t, ValidateDimension(Params.MaxDimension(), true))

	// invalid dim
	assert.NotNil(t, ValidateDimension(-1, false))
	assert.NotNil(t, ValidateDimension(Params.MaxDimension()+1, false))
	assert.NotNil(t, ValidateDimension(9, true))
}

func TestValidateVectorFieldMetricType(t *testing.T) {
	field1 := &schemapb.FieldSchema{
		Name:         "",
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_INT64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	assert.Nil(t, ValidateVectorFieldMetricType(field1))
	field1.DataType = schemapb.DataType_VECTOR_FLOAT
	assert.NotNil(t, ValidateVectorFieldMetricType(field1))
	field1.IndexParams = []*commonpb.KeyValuePair{
		{
			Key:   "abcdefg",
			Value: "",
		},
	}
	assert.NotNil(t, ValidateVectorFieldMetricType(field1))
	field1.IndexParams = append(field1.IndexParams, &commonpb.KeyValuePair{
		Key:   "metric_type",
		Value: "",
	})
	assert.Nil(t, ValidateVectorFieldMetricType(field1))
}

func TestValidateDuplicatedFieldName(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{Name: "abc"},
		{Name: "def"},
	}
	assert.Nil(t, ValidateDuplicatedFieldName(fields))
	fields = append(fields, &schemapb.FieldSchema{
		Name: "abc",
	})
	assert.NotNil(t, ValidateDuplicatedFieldName(fields))
}

func TestValidatePrimaryKey(t *testing.T) {
	coll := schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      true,
		Fields:      nil,
	}
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:         "f1",
		IsPrimaryKey: false,
		Description:  "",
		DataType:     0,
		TypeParams:   nil,
		IndexParams:  nil,
	})
	assert.Nil(t, ValidatePrimaryKey(&coll))
	pf := &schemapb.FieldSchema{
		Name:         "f2",
		IsPrimaryKey: true,
		Description:  "",
		DataType:     0,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	coll.Fields = append(coll.Fields, pf)
	assert.NotNil(t, ValidatePrimaryKey(&coll))
	coll.AutoID = false
	assert.NotNil(t, ValidatePrimaryKey(&coll))
	pf.DataType = schemapb.DataType_BOOL
	assert.NotNil(t, ValidatePrimaryKey(&coll))
	pf.DataType = schemapb.DataType_INT64
	assert.Nil(t, ValidatePrimaryKey(&coll))
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:         "",
		IsPrimaryKey: true,
		Description:  "",
		DataType:     0,
		TypeParams:   nil,
		IndexParams:  nil,
	})
	assert.NotNil(t, ValidatePrimaryKey(&coll))
}
