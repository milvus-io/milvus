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

func TestAutoIndexFunctionParse(t *testing.T) {
	var f calculateFunc
	var err error

	validFuncStrs := []string{
		" __output = __input * 2 + 1",
		" __output = __input",
		" __output = 1.5 * pow(__input, 1.2) + 40",
	}
	for _, funcStr := range validFuncStrs {
		f, err = newFunction(funcStr)
		assert.NoError(t, err)
		assert.NotNil(t, f)
		assert.Equal(t, "input", f.GetInputKey())
		assert.Equal(t, "output", f.GetOutputKey())
	}

	invalidFuncStrs := []string{
		"",
		"{",
		" output = __input * 2 + 1",
		" __output  __input * 2 + 1",
		" _output =  __input * 2 + 1",
	}

	for _, funcStr := range invalidFuncStrs {
		f, err = newFunction(funcStr)
		assert.Error(t, err)
		assert.Nil(t, f)
	}
}

func TestAutoIndexFunctionCalculate(t *testing.T) {
	var params []*commonpb.KeyValuePair
	inputKey := "input"
	outputKey := "output"
	params = append(params, &commonpb.KeyValuePair{
		Key:   inputKey,
		Value: "10",
	})

	var f calculateFunc
	var err error

	t.Run("function1", func(t *testing.T) {
		funcStr := "__output = 4 * __input + 5"
		f, err = newFunction(funcStr)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		assert.Equal(t, int64(4*10+5), tValue[outputKey].(int64))
	})

	t.Run("function2", func(t *testing.T) {
		funcStr := "__output = 4 * pow(__input,2) + 6"
		f, err = newFunction(funcStr)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		targetV := int64(4*math.Pow(10, 2) + 6)
		assert.Equal(t, targetV, tValue[outputKey].(int64))
	})

	t.Run("function3", func(t *testing.T) {
		funcStr := "__output = __input"
		f, err = newFunction(funcStr)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), tValue[outputKey].(int64))
	})

	t.Run("function4", func(t *testing.T) {
		funcStr := "__output_2 = 4 * pow(__input, 2) + 6"
		f, err = newFunction(funcStr)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		targetV := int64(4*math.Pow(10, 2) + 6)
		assert.Equal(t, targetV, tValue["output_2"].(int64))
	})

	t.Run("function5", func(t *testing.T) {
		funcStr := "__output_3 = 1.5 * exp(__input*0.1) + 3"
		f, err = newFunction(funcStr)
		assert.NoError(t, err)
		tValue, err := f.calculate(params)
		assert.NoError(t, err)
		targetV := int64(1.5*math.Exp(10*0.1) + 3)
		assert.Equal(t, targetV, tValue["output_3"].(int64))
	})
}
