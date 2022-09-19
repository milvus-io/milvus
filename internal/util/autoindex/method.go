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

	"github.com/milvus-io/milvus/api/commonpb"
)

var _ Calculator = (*methodPieceWise)(nil)
var _ Calculator = (*methodNormal)(nil)

type Calculator interface {
	Calculate(params []*commonpb.KeyValuePair) (string, error)
}

type methodPieceWise struct {
	MethodID  int
	bp        []float64
	functions []calculateFunc
}

func (m *methodPieceWise) Calculate(params []*commonpb.KeyValuePair) (string, error) {
	topK, err := getTopK(params)
	if err != nil {
		return "", err
	}
	idx := 0
	for _, p := range m.bp {
		if topK < int64(p) {
			break
		}
		idx++
	}
	if idx >= len(m.functions) {
		// can not happen
		return "", fmt.Errorf("calculate failed, methodPeiceWise functions size not match")
	}
	f := m.functions[idx]
	retMap, err := f.calculate(params)
	if err != nil {
		return "", err
	}
	ret, err := json.Marshal(retMap)
	if err != nil {
		return "", err
	}

	return string(ret), nil
}

type methodNormal struct {
	MethodID int
	function calculateFunc
}

func (m *methodNormal) Calculate(params []*commonpb.KeyValuePair) (string, error) {
	retMap, err := m.function.calculate(params)
	if err != nil {
		return "", err
	}
	ret, err := json.Marshal(retMap)
	if err != nil {
		return "", err
	}
	return string(ret), nil
}
