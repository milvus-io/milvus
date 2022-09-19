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
	"math"
	"testing"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestAutoIndexFunctionParse1(t *testing.T) {
	var f calculateFunc
	var err error

	t.Run("function1", func(t *testing.T) {
		json := "{\"funcID\": 1, \"cof1\": 4,\"cof2\": 5}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		function, ok := f.(*function1)
		assert.Equal(t, true, ok)
		assert.Equal(t, 1, function.FuncID)
		assert.Equal(t, float64(4), function.Cof1)
		assert.Equal(t, float64(5), function.Cof2)
	})

	t.Run("function2", func(t *testing.T) {
		json := "{\"funcID\": 2, \"cof1\": 4,\"cof2\": 5,\"cof3\": 6}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		function, ok := f.(*function2)
		assert.Equal(t, true, ok)
		assert.Equal(t, 2, function.FuncID)
		assert.Equal(t, float64(4), function.Cof1)
		assert.Equal(t, float64(5), function.Cof2)
		assert.Equal(t, float64(6), function.Cof3)
	})

	t.Run("function3", func(t *testing.T) {
		json := "{\"funcID\": 3, \"cof1\": 4,\"cof2\": 5}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		function, ok := f.(*function3)
		assert.Equal(t, true, ok)
		assert.Equal(t, 3, function.FuncID)
		assert.Equal(t, float64(4), function.Cof1)
		assert.Equal(t, float64(5), function.Cof2)
	})

	t.Run("function4", func(t *testing.T) {
		json := "{\"funcID\": 4, \"cof1\": 4,\"cof2\": 5, \"cof3\": 6}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		function, ok := f.(*function4)
		assert.Equal(t, true, ok)
		assert.Equal(t, 4, function.FuncID)
		assert.Equal(t, float64(4), function.Cof1)
		assert.Equal(t, float64(5), function.Cof2)
		assert.Equal(t, float64(6), function.Cof3)
	})

	t.Run("function5", func(t *testing.T) {
		json := "{\"funcID\": 5, \"cof1\": 4,\"cof2\": 5, \"cof3\": 6}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		function, ok := f.(*function5)
		assert.Equal(t, true, ok)
		assert.Equal(t, 5, function.FuncID)
		assert.Equal(t, float64(4), function.Cof1)
		assert.Equal(t, float64(5), function.Cof2)
		assert.Equal(t, float64(6), function.Cof3)
	})

	invalidJSONS := []string{
		"",
		"{",
		"{}",
		"{\"funcID\": }",
		"{\"funcID\": x, \"cof1\": 4,\"cof2\": 5}",
		"{\"funcID\": 1000, \"cof1\": 4,\"cof2\": 5}",
	}

	for _, jsonStr := range invalidJSONS {
		f, err = parseFunc(jsonStr)
		assert.NotNil(t, err)
		assert.Nil(t, f)
	}

}

func TestAutoIndexFunctionCalculate1(t *testing.T) {
	var params []*commonpb.KeyValuePair
	params = append(params, &commonpb.KeyValuePair{
		Key:   TopKKey,
		Value: "10",
	})

	var f calculateFunc
	var err error

	t.Run("function1", func(t *testing.T) {
		json := "{\"funcID\": 1, \"cof1\": 4,\"cof2\": 5}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		assert.Equal(t, int64(4*10+5), tValue["ef"].(int64))
	})

	t.Run("function2", func(t *testing.T) {
		json := "{\"funcID\": 2, \"cof1\": 4,\"cof2\": 2,\"cof3\": 6}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		targetV := int64(4*math.Pow(10, 2) + 6)
		assert.Equal(t, targetV, tValue["ef"].(int64))
	})

	t.Run("function3", func(t *testing.T) {
		json := "{\"funcID\": 3, \"cof1\": 4,\"cof2\": 5}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		assert.Equal(t, int64(4*10+5), tValue["search_list_size"].(int64))
	})

	t.Run("function4", func(t *testing.T) {
		json := "{\"funcID\": 4, \"cof1\": 4,\"cof2\": 2,\"cof3\": 6}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		targetV := int64(4*math.Pow(10, 2) + 6)
		assert.Equal(t, targetV, tValue["search_list_size"].(int64))
	})

	t.Run("function5", func(t *testing.T) {
		json := "{\"funcID\": 5, \"cof1\": 1.5,\"cof2\": 0.1,\"cof3\": 3}"
		f, err = parseFunc(json)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		targetV := int64(1.5*math.Exp(10*0.1) + 3)
		assert.Equal(t, targetV, tValue["search_list_size"].(int64))
	})
}
