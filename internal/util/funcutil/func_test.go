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

package funcutil

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckPortAvailable(t *testing.T) {
	num := 10

	for i := 0; i < num; i++ {
		port := GetAvailablePort()
		assert.Equal(t, CheckPortAvailable(port), true)
	}
}

func TestParseIndexParamsMap(t *testing.T) {
	num := 10
	keys := make([]string, 0)
	values := make([]string, 0)
	params := make(map[string]string)

	for i := 0; i < num; i++ {
		keys = append(keys, "key"+strconv.Itoa(i))
		values = append(values, "value"+strconv.Itoa(i))
		params[keys[i]] = values[i]
	}

	paramsBytes, err := json.Marshal(params)
	assert.Equal(t, err, nil)
	paramsStr := string(paramsBytes)

	parsedParams, err := ParseIndexParamsMap(paramsStr)
	assert.Equal(t, err, nil)
	assert.Equal(t, parsedParams, params)

	invalidStr := "invalid string"
	_, err = ParseIndexParamsMap(invalidStr)
	assert.NotEqual(t, err, nil)
}
