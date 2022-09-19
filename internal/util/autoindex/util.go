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
	"fmt"
	"strconv"

	"encoding/json"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

func parseMethodID(methodV interface{}) int {
	var methodID int
	var err error
	switch mValue := methodV.(type) {
	case float64: // for numeric values, json unmarshal will interpret it as float64
		methodID = int(mValue)
	case string:
		methodID, err = strconv.Atoi(mValue)
		if err != nil {
			return -1
		}
	default:
		return -1
	}
	return methodID
}

func parseFunc(jsonStr string) (calculateFunc, error) {
	var objMap map[string]*json.RawMessage
	var err error
	err = json.Unmarshal([]byte(jsonStr), &objMap)
	if err != nil || objMap == nil {
		return nil, err
	}
	var funcID int
	funcIDV := objMap["funcID"]
	if funcIDV == nil || *funcIDV == nil {
		return nil, fmt.Errorf("parse function failed, funcID not specified")
	}
	err = json.Unmarshal(*funcIDV, &funcID)
	if err != nil {
		return nil, err
	}
	var f calculateFunc
	switch funcID {
	case 1:
		var f1 function1
		err = json.Unmarshal([]byte(jsonStr), &f1)
		if err != nil {
			return nil, err
		}
		f = &f1
	case 2:
		var f2 function2
		err = json.Unmarshal([]byte(jsonStr), &f2)
		if err != nil {
			return nil, err
		}
		f = &f2
	case 3:
		var f3 function3
		err = json.Unmarshal([]byte(jsonStr), &f3)
		if err != nil {
			return nil, err
		}
		f = &f3
	case 4:
		var f4 function4
		err = json.Unmarshal([]byte(jsonStr), &f4)
		if err != nil {
			return nil, err
		}
		f = &f4
	case 5:
		var f5 function5
		err = json.Unmarshal([]byte(jsonStr), &f5)
		if err != nil {
			return nil, err
		}
		f = &f5

	default:
		return nil, fmt.Errorf("parse function failed funcID not match")
	}
	return f, nil
}

func parseMethodPieceWise(values map[string]interface{}) (*methodPieceWise, error) {

	methodIDV, ok := values["methodID"]
	if !ok {
		return nil, fmt.Errorf("parse piecewise method failed, methodID not specified")
	}
	methodID := parseMethodID(methodIDV)
	if methodIDV == -1 {
		return nil, fmt.Errorf("parse piecewise method failed, methodID in wrong format")
	}

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

	funcJSONs, ok := values["functions"]
	if !ok {
		return nil, fmt.Errorf("parse piecewise function failed, functions not specified")
	}
	jsonSlice, ok := funcJSONs.([]interface{})
	if !ok {
		return nil, fmt.Errorf("parse piecewise function failed, functions in wrong format")
	}

	var functions []calculateFunc
	for _, funcValue := range jsonSlice {
		funcStr, ok := funcValue.(string)
		if !ok {
			return nil, fmt.Errorf("parse piecewise function failed, function in wrong format")
		}
		var f calculateFunc
		f, err = parseFunc(funcStr)
		if err != nil {
			return nil, err
		}
		functions = append(functions, f)
	}

	if len(bpValues)+1 != len(functions) {
		return nil, fmt.Errorf("parse piecewise function failed, function size not match to bp size")
	}

	ret := &methodPieceWise{
		MethodID:  methodID,
		bp:        bpValues,
		functions: functions,
	}
	return ret, nil
}

func getTopK(params []*commonpb.KeyValuePair) (int64, error) {
	topKStr, err := funcutil.GetAttrByKeyFromRepeatedKV(TopKKey, params)
	if err != nil {
		return 0, fmt.Errorf("topk not found in search_params")
	}
	topK, err := strconv.ParseInt(topKStr, 0, 64)
	if err != nil {
		return 0, fmt.Errorf("%s [%s] is invalid", TopKKey, topKStr)
	}
	return topK, nil
}

func parseMethodNormal(values map[string]interface{}) (*methodNormal, error) {
	var err error

	methodIDV, ok := values["methodID"]
	if !ok {
		return nil, fmt.Errorf("parse normal method failed, methodID not specified")
	}
	methodID := parseMethodID(methodIDV)
	if methodIDV == -1 {
		return nil, fmt.Errorf("parse normal method failed, methodID in wrong format")
	}

	funcValue, ok := values["function"]
	if !ok {
		return nil, fmt.Errorf("parse normal method failed, function not specified")
	}
	funcStr, ok := funcValue.(string)
	if !ok {
		return nil, fmt.Errorf("parse normal method failed, function in wrong format")
	}
	var f calculateFunc
	f, err = parseFunc(funcStr)
	if err != nil {
		return nil, err
	}
	ret := &methodNormal{
		MethodID: methodID,
		function: f,
	}
	return ret, nil
}
