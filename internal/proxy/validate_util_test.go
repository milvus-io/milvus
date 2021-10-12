// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxy

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
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
		assert.NotNil(t, validateCollectionNameOrAlias(name, "name"))
		assert.NotNil(t, validateCollectionNameOrAlias(name, "alias"))
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
	assert.Nil(t, ValidateDimension(Params.MaxDimension, false))
	assert.Nil(t, ValidateDimension(8, true))
	assert.Nil(t, ValidateDimension(Params.MaxDimension, true))

	// invalid dim
	assert.NotNil(t, ValidateDimension(-1, false))
	assert.NotNil(t, ValidateDimension(Params.MaxDimension+1, false))
	assert.NotNil(t, ValidateDimension(9, true))
}

func TestValidateVectorFieldMetricType(t *testing.T) {
	field1 := &schemapb.FieldSchema{
		Name:         "",
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	assert.Nil(t, ValidateVectorFieldMetricType(field1))
	field1.DataType = schemapb.DataType_FloatVector
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
		FieldID:      100,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     1,
		TypeParams:   nil,
		IndexParams:  nil,
	})
	assert.Nil(t, ValidateSchema(&coll))
	pf := &schemapb.FieldSchema{
		Name:         "f2",
		FieldID:      101,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     1,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	coll.Fields = append(coll.Fields, pf)
	assert.NotNil(t, ValidateSchema(&coll))
	coll.AutoID = false
	assert.NotNil(t, ValidateSchema(&coll))
	pf.DataType = schemapb.DataType_Bool
	assert.NotNil(t, ValidateSchema(&coll))
	pf.DataType = schemapb.DataType_Int64
	assert.Nil(t, ValidateSchema(&coll))
	coll.Fields = append(coll.Fields, &schemapb.FieldSchema{
		Name:         "",
		FieldID:      102,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     1,
		TypeParams:   nil,
		IndexParams:  nil,
	})
	assert.NotNil(t, ValidateSchema(&coll))
}

func TestValidateSchema(t *testing.T) {
	coll := &schemapb.CollectionSchema{
		Name:        "coll1",
		Description: "",
		AutoID:      false,
		Fields:      nil,
	}
	assert.NotNil(t, ValidateSchema(coll))

	pf1 := &schemapb.FieldSchema{
		Name:         "f1",
		FieldID:      100,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	coll.Fields = append(coll.Fields, pf1)
	assert.NotNil(t, ValidateSchema(coll))

	pf1.IsPrimaryKey = true
	assert.Nil(t, ValidateSchema(coll))

	pf1.DataType = schemapb.DataType_Int32
	assert.NotNil(t, ValidateSchema(coll))

	pf1.DataType = schemapb.DataType_Int64
	assert.Nil(t, ValidateSchema(coll))

	pf2 := &schemapb.FieldSchema{
		Name:         "f2",
		FieldID:      101,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
	}
	coll.Fields = append(coll.Fields, pf2)
	assert.NotNil(t, ValidateSchema(coll))

	pf2.IsPrimaryKey = false
	assert.Nil(t, ValidateSchema(coll))

	pf2.Name = "f1"
	assert.NotNil(t, ValidateSchema(coll))
	pf2.Name = "f2"
	assert.Nil(t, ValidateSchema(coll))

	pf2.FieldID = 100
	assert.NotNil(t, ValidateSchema(coll))

	pf2.FieldID = 101
	assert.Nil(t, ValidateSchema(coll))

	pf2.DataType = -1
	assert.NotNil(t, ValidateSchema(coll))

	pf2.DataType = schemapb.DataType_FloatVector
	assert.NotNil(t, ValidateSchema(coll))

	pf2.DataType = schemapb.DataType_Int64
	assert.Nil(t, ValidateSchema(coll))

	tp3Good := []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "128",
		},
	}

	tp3Bad1 := []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "asdfa",
		},
	}

	tp3Bad2 := []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "-1",
		},
	}

	tp3Bad3 := []*commonpb.KeyValuePair{
		{
			Key:   "dimX",
			Value: "128",
		},
	}

	tp3Bad4 := []*commonpb.KeyValuePair{
		{
			Key:   "dim",
			Value: "128",
		},
		{
			Key:   "dim",
			Value: "64",
		},
	}

	ip3Good := []*commonpb.KeyValuePair{
		{
			Key:   "metric_type",
			Value: "IP",
		},
	}

	ip3Bad1 := []*commonpb.KeyValuePair{
		{
			Key:   "metric_type",
			Value: "JACCARD",
		},
	}

	ip3Bad2 := []*commonpb.KeyValuePair{
		{
			Key:   "metric_type",
			Value: "xxxxxx",
		},
	}

	ip3Bad3 := []*commonpb.KeyValuePair{
		{
			Key:   "metric_type",
			Value: "L2",
		},
		{
			Key:   "metric_type",
			Value: "IP",
		},
	}

	pf3 := &schemapb.FieldSchema{
		Name:         "f3",
		FieldID:      102,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams:   tp3Good,
		IndexParams:  ip3Good,
	}

	coll.Fields = append(coll.Fields, pf3)
	assert.Nil(t, ValidateSchema(coll))

	pf3.TypeParams = tp3Bad1
	assert.NotNil(t, ValidateSchema(coll))

	pf3.TypeParams = tp3Bad2
	assert.NotNil(t, ValidateSchema(coll))

	pf3.TypeParams = tp3Bad3
	assert.NotNil(t, ValidateSchema(coll))

	pf3.TypeParams = tp3Bad4
	assert.NotNil(t, ValidateSchema(coll))

	pf3.TypeParams = tp3Good
	assert.Nil(t, ValidateSchema(coll))

	pf3.IndexParams = ip3Bad1
	assert.NotNil(t, ValidateSchema(coll))

	pf3.IndexParams = ip3Bad2
	assert.NotNil(t, ValidateSchema(coll))

	pf3.IndexParams = ip3Bad3
	assert.NotNil(t, ValidateSchema(coll))

	pf3.IndexParams = ip3Good
	assert.Nil(t, ValidateSchema(coll))
}
