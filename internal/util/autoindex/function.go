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

	"github.com/milvus-io/milvus/api/commonpb"
)

const (
	TopKKey = "topk"
)

var _ calculateFunc = (*function1)(nil)
var _ calculateFunc = (*function2)(nil)
var _ calculateFunc = (*function3)(nil)

type calculateFunc interface {
	calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error)
}

//type function1 struct {
//	FuncID int `json:"funcID"`
//}
//
//func (f *function1) calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
//	topK, err := getTopK(params)
//	if err != nil {
//		return nil, err
//	}
//	ret := make(map[string]interface{})
//	ret["ef"] = topK
//	return ret, nil
//}

type function1 struct {
	FuncID int     `json:"funcID"`
	Cof1   float64 `json:"cof1"`
	Cof2   float64 `json:"cof2"`
}

func (f *function1) calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
	topK, err := getTopK(params)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]interface{})
	ret["ef"] = int64(f.Cof1*float64(topK) + f.Cof2)
	return ret, nil
}

type function2 struct {
	FuncID int     `json:"funcID"`
	Cof1   float64 `json:"cof1"`
	Cof2   float64 `json:"cof2"`
	Cof3   float64 `json:"cof3"`
}

func (f *function2) calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
	topK, err := getTopK(params)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]interface{})
	ret["ef"] = int64(f.Cof1*math.Pow(float64(topK), f.Cof2) + f.Cof3)
	return ret, nil
}

type function3 struct {
	FuncID int     `json:"funcID"`
	Cof1   float64 `json:"cof1"`
	Cof2   float64 `json:"cof2"`
}

func (f *function3) calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
	topK, err := getTopK(params)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]interface{})
	ret["search_list_size"] = int64(f.Cof1*float64(topK) + f.Cof2)
	return ret, nil
}

type function4 struct {
	FuncID int     `json:"funcID"`
	Cof1   float64 `json:"cof1"`
	Cof2   float64 `json:"cof2"`
	Cof3   float64 `json:"cof3"`
}

func (f *function4) calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
	topK, err := getTopK(params)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]interface{})
	ret["search_list_size"] = int64(f.Cof1*math.Pow(float64(topK), f.Cof2) + f.Cof3)
	return ret, nil
}

type function5 struct {
	FuncID int     `json:"funcID"`
	Cof1   float64 `json:"cof1"`
	Cof2   float64 `json:"cof2"`
	Cof3   float64 `json:"cof3"`
}

func (f *function5) calculate(params []*commonpb.KeyValuePair) (map[string]interface{}, error) {
	topK, err := getTopK(params)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]interface{})
	ret["search_list_size"] = int64(f.Cof1*math.Exp(f.Cof2*float64(topK)) + f.Cof3)
	return ret, nil
}
