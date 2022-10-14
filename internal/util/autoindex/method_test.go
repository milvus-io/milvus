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

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestAutoIndexMethodParse(t *testing.T) {
	var err error
	json1 := `{
		          "function": "__output = __input * 10 + 5"
			  }`
	m1, err := newMethodNormal(json1)
	assert.NoError(t, err)
	assert.NotNil(t, m1)
	assert.NotNil(t, m1.function)

	json2 := `{
		          "bp": [10, 200],
		          "functions": [
			    	"__output = __input",
			    	"__output = 10 * __input + 5",
			    	"__output = pow(__input, 1)"
		          ]
		        }`
	m2, err := newMethodPieceWise(json2)
	assert.NoError(t, err)
	assert.NotNil(t, m2)
	assert.NotNil(t, m2.functions)
	assert.Equal(t, 2, len(m2.bp))
	assert.Equal(t, 3, len(m2.functions))
	assert.Equal(t, "input", m2.bpKey)

	invalid1JSONS := []string{
		"",
		`{}`,
		`{"": 1, "func": ""}`,
		`{"bp": [1,2], "function": ""}`,
		`{"bp": 1, "function": "xxx"}`,
	}

	invalid2JSONS := []string{
		"",
		`{}`,
		`{"bp": 2}`,
		`{"bp": [2], "func": ""}`,
		`{"bp": [1,2], "function": ""}`,
		`{"functions": "xxx"}`,
		`{
		          "bp": [10, 200],
		          "functions": [
			    	"__output = __input",
			    	"__output = 10 * __input + 5",
		          ]
		        }`,
	}

	var m Calculator

	for _, jsonStr := range invalid1JSONS {
		m, err = newMethodNormal(jsonStr)
		assert.NotNil(t, err)
		assert.Nil(t, m)
	}

	for _, jsonStr := range invalid2JSONS {
		m, err = newMethodPieceWise(jsonStr)
		assert.NotNil(t, err)
		assert.Nil(t, m)
	}
}

func TestAutoIndexMethodCalculate(t *testing.T) {
	var err error

	inputKey := "input"
	outputKey := "output"

	var params []*commonpb.KeyValuePair
	params = append(params, &commonpb.KeyValuePair{
		Key:   inputKey,
		Value: "10",
	})
	var method Calculator

	t.Run("methodNormal", func(t *testing.T) {
		jsonStr := `{
				  "function": "__output = 3 * pow(__input, 2) + 5"
				  }`
		method, err = newMethodNormal(jsonStr)
		assert.NoError(t, err)

		targetV := int64(3*math.Pow(10, 2) + 5)
		expMap := make(map[string]interface{})
		expMap[outputKey] = targetV
		expJSON, err := json.Marshal(expMap)
		expJSONStr := string(expJSON)
		assert.NoError(t, err)

		ret, err := method.Calculate(params)
		assert.NoError(t, err)
		targetJSON, err := json.Marshal(ret)
		assert.NoError(t, err)
		targetJSONStr := string(targetJSON)
		assert.Equal(t, expJSONStr, targetJSONStr)
	})

	t.Run("methodPieceWise", func(t *testing.T) {
		jsonStr := `{
			  "bp": [10, 50],
		          "functions": [
			    	"__output = __input",
			    	"__output = 3.0*pow(__input,2) + 5",
			    	"__output = 10 * __input + 5"
		          ]
		}`
		method, err = newMethodPieceWise(jsonStr)
		assert.NoError(t, err)

		targetV := int64(3*math.Pow(10, 2) + 5)
		expMap := make(map[string]interface{})
		expMap[outputKey] = targetV
		expJSON, err := json.Marshal(expMap)
		expJSONStr := string(expJSON)
		assert.NoError(t, err)

		ret, err := method.Calculate(params)
		assert.NoError(t, err)
		targetJSON, err := json.Marshal(ret)
		assert.NoError(t, err)
		targetJSONStr := string(targetJSON)
		assert.Equal(t, expJSONStr, targetJSONStr)
	})
}
