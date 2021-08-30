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
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
)

func Test_parseDummyRequestType(t *testing.T) {
	var err error

	// not in json format
	notInJSONFormatStr := "not in json format string"
	_, err = parseDummyRequestType(notInJSONFormatStr)
	assert.NotNil(t, err)

	// only contain other field, in json format
	otherField := "other_field"
	otherFieldValue := "not important"
	m1 := make(map[string]string)
	m1[otherField] = otherFieldValue
	bs1, err := json.Marshal(m1)
	assert.Nil(t, err)
	log.Info("Test_parseDummyRequestType",
		zap.String("json", string(bs1)))
	ret1, err := parseDummyRequestType(string(bs1))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ret1.RequestType))

	// normal case
	key := "request_type"
	value := "value"
	m2 := make(map[string]string)
	m2[key] = value
	bs2, err := json.Marshal(m2)
	assert.Nil(t, err)
	log.Info("Test_parseDummyRequestType",
		zap.String("json", string(bs2)))
	ret2, err := parseDummyRequestType(string(bs2))
	assert.Nil(t, err)
	assert.Equal(t, value, ret2.RequestType)

	// contain other field and request_type
	m3 := make(map[string]string)
	m3[key] = value
	m3[otherField] = otherFieldValue
	bs3, err := json.Marshal(m3)
	assert.Nil(t, err)
	log.Info("Test_parseDummyRequestType",
		zap.String("json", string(bs3)))
	ret3, err := parseDummyRequestType(string(bs3))
	assert.Nil(t, err)
	assert.Equal(t, value, ret3.RequestType)
}

func Test_parseDummyQueryRequest(t *testing.T) {
	var err error

	// not in json format
	notInJSONFormatStr := "not in json format string"
	_, err = parseDummyQueryRequest(notInJSONFormatStr)
	assert.NotNil(t, err)

	// only contain other field, in json format
	otherField := "other_field"
	otherFieldValue := "not important"
	m1 := make(map[string]interface{})
	m1[otherField] = otherFieldValue
	bs1, err := json.Marshal(m1)
	log.Info("Test_parseDummyQueryRequest",
		zap.String("json", string(bs1)))
	assert.Nil(t, err)
	ret1, err := parseDummyQueryRequest(string(bs1))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ret1.RequestType))
	assert.Equal(t, 0, len(ret1.DbName))
	assert.Equal(t, 0, len(ret1.CollectionName))
	assert.Equal(t, 0, len(ret1.PartitionNames))
	assert.Equal(t, 0, len(ret1.Expr))
	assert.Equal(t, 0, len(ret1.OutputFields))

	requestTypeKey := "request_type"
	requestTypeValue := "request_type"
	dbNameKey := "dbname"
	dbNameValue := "dbname"
	collectionNameKey := "collection_name"
	collectionNameValue := "collection_name"
	partitionNamesKey := "partition_names"
	partitionNamesValue := []string{"partition_names"}
	exprKey := "expr"
	exprValue := "expr"
	outputFieldsKey := "output_fields"
	outputFieldsValue := []string{"output_fields"}

	// all fields
	m2 := make(map[string]interface{})
	m2[requestTypeKey] = requestTypeValue
	m2[dbNameKey] = dbNameValue
	m2[collectionNameKey] = collectionNameValue
	m2[partitionNamesKey] = partitionNamesValue
	m2[exprKey] = exprValue
	m2[outputFieldsKey] = outputFieldsValue
	bs2, err := json.Marshal(m2)
	log.Info("Test_parseDummyQueryRequest",
		zap.String("json", string(bs2)))
	assert.Nil(t, err)
	ret2, err := parseDummyQueryRequest(string(bs2))
	assert.Nil(t, err)
	assert.Equal(t, requestTypeValue, ret2.RequestType)
	assert.Equal(t, dbNameValue, ret2.DbName)
	assert.Equal(t, collectionNameValue, ret2.CollectionName)
	assert.Equal(t, partitionNamesValue, ret2.PartitionNames)
	assert.Equal(t, exprValue, ret2.Expr)
	assert.Equal(t, outputFieldsValue, ret2.OutputFields)

	// all fields and other field
	m3 := make(map[string]interface{})
	m3[otherField] = otherFieldValue
	m3[requestTypeKey] = requestTypeValue
	m3[dbNameKey] = dbNameValue
	m3[collectionNameKey] = collectionNameValue
	m3[partitionNamesKey] = partitionNamesValue
	m3[exprKey] = exprValue
	m3[outputFieldsKey] = outputFieldsValue
	bs3, err := json.Marshal(m3)
	log.Info("Test_parseDummyQueryRequest",
		zap.String("json", string(bs3)))
	assert.Nil(t, err)
	ret3, err := parseDummyQueryRequest(string(bs3))
	assert.Nil(t, err)
	assert.Equal(t, requestTypeValue, ret3.RequestType)
	assert.Equal(t, dbNameValue, ret3.DbName)
	assert.Equal(t, collectionNameValue, ret3.CollectionName)
	assert.Equal(t, partitionNamesValue, ret3.PartitionNames)
	assert.Equal(t, exprValue, ret3.Expr)
	assert.Equal(t, outputFieldsValue, ret3.OutputFields)

	// partial fields
	m4 := make(map[string]interface{})
	m4[requestTypeKey] = requestTypeValue
	m4[dbNameKey] = dbNameValue
	bs4, err := json.Marshal(m4)
	log.Info("Test_parseDummyQueryRequest",
		zap.String("json", string(bs4)))
	assert.Nil(t, err)
	ret4, err := parseDummyQueryRequest(string(bs4))
	assert.Nil(t, err)
	assert.Equal(t, requestTypeValue, ret4.RequestType)
	assert.Equal(t, dbNameValue, ret4.DbName)
	assert.Equal(t, collectionNameValue, ret2.CollectionName)
	assert.Equal(t, partitionNamesValue, ret2.PartitionNames)
	assert.Equal(t, exprValue, ret2.Expr)
	assert.Equal(t, outputFieldsValue, ret2.OutputFields)

	// partial fields and other field
	m5 := make(map[string]interface{})
	m5[otherField] = otherFieldValue
	m5[requestTypeKey] = requestTypeValue
	m5[dbNameKey] = dbNameValue
	bs5, err := json.Marshal(m5)
	log.Info("Test_parseDummyQueryRequest",
		zap.String("json", string(bs5)))
	assert.Nil(t, err)
	ret5, err := parseDummyQueryRequest(string(bs5))
	assert.Nil(t, err)
	assert.Equal(t, requestTypeValue, ret5.RequestType)
	assert.Equal(t, dbNameValue, ret5.DbName)
	assert.Equal(t, collectionNameValue, ret2.CollectionName)
	assert.Equal(t, partitionNamesValue, ret2.PartitionNames)
	assert.Equal(t, exprValue, ret2.Expr)
	assert.Equal(t, outputFieldsValue, ret2.OutputFields)
}

// func TestParseDummyQueryRequest(t *testing.T) {
// 	invalidStr := `{"request_type":"query"`
// 	_, err := parseDummyQueryRequest(invalidStr)
// 	assert.NotNil(t, err)

// 	onlytypeStr := `{"request_type":"query"}`
// 	drr, err := parseDummyQueryRequest(onlytypeStr)
// 	assert.Nil(t, err)
// 	assert.Equal(t, drr.RequestType, "query")
// 	assert.Equal(t, len(drr.DbName), 0)

// 	fulltypeStr := `{
// 	"request_type":"query",
// 	"dbname":"",
// 	"collection_name":"test",
// 	"partition_names": [],
// 	"expr": "_id in [ 100 ,101 ]",
// 	"output_fields": ["_id", "age"]
// 	}`
// 	drr2, err := parseDummyQueryRequest(fulltypeStr)
// 	assert.Nil(t, err)
// 	assert.Equal(t, drr2.RequestType, "retrieve")
// 	assert.Equal(t, len(drr2.DbName), 0)
// 	assert.Equal(t, drr2.CollectionName, "test")
// 	assert.Equal(t, len(drr2.PartitionNames), 0)
// 	assert.Equal(t, drr2.OutputFields, []string{"_id", "age"})
// }
