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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
)

var _ Calculator = (*methodPieceWise)(nil)
var _ Calculator = (*methodNormal)(nil)

type Calculator interface {
	Calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error)
}

type methodPieceWise struct {
	bp        []float64
	functions []calculateFunc
	bpKey     string
}

func (m *methodPieceWise) Calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
	bpValue, err := getInt64FromParams(params, m.bpKey)
	if err != nil {
		return nil, err
	}
	idx := 0
	for _, p := range m.bp {
		if bpValue < int64(p) {
			break
		}
		idx++
	}
	if idx >= len(m.functions) {
		// can not happen
		return nil, fmt.Errorf("calculate failed, methodPeiceWise functions size not match")
	}
	f := m.functions[idx]
	retMap, err := f.calculate(params)
	if err != nil {
		return nil, err
	}
	return retMap, nil
}

func newMethodPieceWise(jsonStr string) (*methodPieceWise, error) {
	valueMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &valueMap)
	if err != nil {
		return nil, fmt.Errorf("newMethodPieceWise failed:%w", err)
	}
	return newMethodPieceWiseFromMap(valueMap)
}

func newMethodPieceWiseFromMap(values map[string]interface{}) (*methodPieceWise, error) {
	var err error
	bpValue, ok := values["bp"]
	if !ok {
		return nil, fmt.Errorf("parse piecewise function failed, bp not specified")
	}
	bpSlice, ok := bpValue.([]interface{})
	if !ok {
		return nil, fmt.Errorf("parse piecewise bp failed, bp in wrong format")
	}
	var bpValues []float64
	for _, bpV := range bpSlice {
		bpFloat, ok := bpV.(float64)
		if !ok {
			return nil, fmt.Errorf("parse piecewise bp failed, bp in wrong format")
		}
		bpValues = append(bpValues, bpFloat)
	}

	funcs, ok := values["functions"]
	if !ok {
		return nil, fmt.Errorf("parse piecewise function failed, functions not specified")
	}
	funcStrSlice, ok := funcs.([]interface{})
	if !ok {
		return nil, fmt.Errorf("parse piecewise function failed, functions in wrong format")
	}
	var functions []calculateFunc
	for _, funcValue := range funcStrSlice {
		funcStr, ok := funcValue.(string)
		if !ok {
			return nil, fmt.Errorf("parse piecewise function failed, function in wrong format")
		}
		var f calculateFunc
		f, err = newFunction(funcStr)
		if err != nil {
			return nil, err
		}
		functions = append(functions, f)
	}

	if len(bpValues)+1 != len(functions) {
		return nil, fmt.Errorf("parse piecewise function failed, function size not match to bp size")
	}
	ret := &methodPieceWise{
		bp:        bpValues,
		functions: functions,
		bpKey:     functions[0].GetInputKey(),
	}
	return ret, nil
}

type methodNormal struct {
	function calculateFunc
}

func (m *methodNormal) Calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
	retMap, err := m.function.calculate(params)
	if err != nil {
		return nil, err
	}
	return retMap, nil
}

func newMethodNormal(jsonStr string) (*methodNormal, error) {
	valueMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(jsonStr), &valueMap)
	if err != nil {
		return nil, fmt.Errorf("newMethodNormal failed:%w", err)
	}
	return newMethodNormalFromMap(valueMap)
}

func newMethodNormalFromMap(values map[string]interface{}) (*methodNormal, error) {
	var err error
	funcValue, ok := values["function"]
	if !ok {
		return nil, fmt.Errorf("parse normal method failed, function not specified")
	}
	funcStr, ok := funcValue.(string)
	if !ok {
		return nil, fmt.Errorf("parse normal method failed, function in wrong format")
	}
	var f calculateFunc
	f, err = newFunction(funcStr)
	if err != nil {
		return nil, err
	}
	ret := &methodNormal{
		function: f,
	}
	return ret, nil
}
