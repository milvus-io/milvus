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

package autoindex

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestAutoIndexMethodParse(t *testing.T) {
	var err error
	json1 := `{
		          "methodID": 1,
		          "function": "{\"funcID\": 2, \"cof1\": 3,\"cof2\": 4,\"cof3\": 5}"
			  }`
	valueMap1 := make(map[string]interface{})
	err = json.Unmarshal([]byte(json1), &valueMap1)
	assert.NoError(t, err)
	m1, err := parseMethodNormal(valueMap1)
	assert.NoError(t, err)
	assert.NotNil(t, m1)
	assert.Equal(t, 1, m1.MethodID)
	assert.NotNil(t, m1.function)

	json2 := `{
		          "methodID": 2,
		          "bp": [10, 200],
		          "functions": [
			    	"{\"funcID\": 1, \"cof1\": 1,\"cof2\": 0}",
			    	"{\"funcID\": 1, \"cof1\": 4,\"cof2\": 5}",
			    	"{\"funcID\": 2, \"cof1\": 4,\"cof2\": 5,\"cof3\": 6}"
		          ]
		        }`
	valueMap2 := make(map[string]interface{})
	err = json.Unmarshal([]byte(json2), &valueMap2)
	assert.NoError(t, err)
	m2, err := parseMethodPieceWise(valueMap2)
	assert.NoError(t, err)
	assert.NotNil(t, m2)
	assert.Equal(t, 2, m2.MethodID)
	assert.NotNil(t, m2.functions)
	assert.Equal(t, 2, len(m2.bp))
	assert.Equal(t, 3, len(m2.functions))

	invalid1JSONS := []string{
		`{}`,
		`{"methodID": 1}`,
		`{"methodID": 1, "func": ""}`,
		`{"methodID": 1, "function": ""}`,
		`{"methodID": 1, "function": "xxx"}`,
	}

	invalid2JSONS := []string{
		`{}`,
		`{"methodID": 2}`,
		`{"methodID": 2, "func": ""}`,
		`{"methodID": 2, "functions": ""}`,
		`{"methodID": 2, "functions": "xxx"}`,
	}

	var m Calculator

	for _, jsonStr := range invalid1JSONS {
		valueMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(jsonStr), &valueMap)
		assert.NoError(t, err)

		m, err = parseMethodNormal(valueMap)
		assert.NotNil(t, err)
		assert.Nil(t, m)
	}

	for _, jsonStr := range invalid2JSONS {
		valueMap := make(map[string]interface{})
		err = json.Unmarshal([]byte(jsonStr), &valueMap)
		assert.NoError(t, err)
		m, err = parseMethodPieceWise(valueMap)
		assert.NotNil(t, err)
		assert.Nil(t, m)
	}

}

func TestAutoIndexMethodCalculate(t *testing.T) {
	var err error

	var params []*commonpb.KeyValuePair
	params = append(params, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "10",
	})
	var method Calculator

	json1 := `{
		          "methodID": 1,
		          "function": "{\"funcID\": 2, \"cof1\": 3,\"cof2\": 2,\"cof3\": 5}"
			  }`
	valueMap1 := make(map[string]interface{})
	err = json.Unmarshal([]byte(json1), &valueMap1)
	assert.NoError(t, err)
	method, err = parseMethodNormal(valueMap1)
	assert.NoError(t, err)

	targetV := int64(3*math.Pow(10, 2) + 5)
	targetMap := make(map[string]interface{})
	targetMap["ef"] = targetV
	targetJSON, err := json.Marshal(targetMap)
	targetJSONStr := string(targetJSON)
	assert.NoError(t, err)
	ret, err := method.Calculate(params)
	assert.NoError(t, err)
	assert.Equal(t, targetJSONStr, ret)

	json2 := `{
		          "methodID": 2,
		          "bp": [10, 50],
		          "functions": [
					"{\"funcID\": 1, \"cof1\": 1,\"cof2\": 0}",
			    	"{\"funcID\": 1, \"cof1\": 4,\"cof2\": 5}",
			    	"{\"funcID\": 2, \"cof1\": 4,\"cof2\": 2,\"cof3\": 6}"
		          ]
		        }`
	valueMap2 := make(map[string]interface{})
	err = json.Unmarshal([]byte(json2), &valueMap2)
	assert.NoError(t, err)
	method, err = parseMethodPieceWise(valueMap2)
	assert.NoError(t, err)
	assert.NotNil(t, method)

	var params1 []*commonpb.KeyValuePair
	params1 = append(params1, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "5",
	})

	targetV1 := int64(5)
	targetMap1 := make(map[string]interface{})
	targetMap1["ef"] = targetV1
	targetJSON1, err := json.Marshal(targetMap1)
	assert.NoError(t, err)

	targetJSONStr1 := string(targetJSON1)
	assert.NoError(t, err)
	ret1, err := method.Calculate(params1)
	assert.NoError(t, err)

	assert.Equal(t, targetJSONStr1, ret1)

	var params2 []*commonpb.KeyValuePair
	params2 = append(params2, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "10",
	})

	targetV2 := int64(4*10 + 5)
	targetMap2 := make(map[string]interface{})
	targetMap2["ef"] = targetV2
	targetJSON2, err := json.Marshal(targetMap2)
	targetJSONStr2 := string(targetJSON2)
	assert.NoError(t, err)
	ret2, err := method.Calculate(params2)
	assert.NoError(t, err)
	assert.Equal(t, targetJSONStr2, ret2)

	var params3 []*commonpb.KeyValuePair
	params3 = append(params3, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "50",
	})

	targetV3 := int64(4*math.Pow(50, 2) + 6)
	targetMap3 := make(map[string]interface{})
	targetMap3["ef"] = targetV3
	targetJSON3, err := json.Marshal(targetMap3)
	targetJSONStr3 := string(targetJSON3)
	assert.NoError(t, err)
	ret3, err := method.Calculate(params3)
	assert.NoError(t, err)
	assert.Equal(t, targetJSONStr3, ret3)

}
