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
	"strings"

	//"strconv"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/sandertv/go-formula/v2"
)

const (
	TopKKey = "topk"
)

var _ calculateFunc = (*function)(nil)

type calculateFunc interface {
	calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error)
	GetInputKey() string
	GetOutputKey() string
}

type function struct {
	f         *formula.Formula
	input     string
	inputKey  string
	outputKey string
}

func newFunction(stmt string) (*function, error) {
	input, output, expr, err := parseAssignment(stmt)
	if err != nil {
		return nil, fmt.Errorf("parse function failed, wrong format:%w", err)
	}
	f, err := formula.New(expr)
	if err != nil {
		return nil, fmt.Errorf("parse function failed, wrong format:%w", err)
	}
	ret := &function{}
	ret.f = f
	ret.input = input
	ret.inputKey = strings.TrimPrefix(input, VariablePrefix)
	ret.outputKey = strings.TrimPrefix(output, VariablePrefix)
	return ret, nil
}

func (f *function) calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
	inputValue, err := getInt64FromParams(params, f.inputKey)
	if err != nil {
		return nil, err
	}
	inputVar := formula.Var(f.input, inputValue)
	outputValue, err := f.f.Eval(inputVar)
	if err != nil {
		return nil, fmt.Errorf("calculate failed:%w", err)
	}
	ret := make(map[string]interface{})
	ret[f.outputKey] = int64(outputValue)
	return ret, nil
}

func (f *function) GetInputKey() string {
	return f.inputKey
}

func (f *function) GetOutputKey() string {
	return f.outputKey
}
