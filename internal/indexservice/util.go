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

package indexservice

import (
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

func CompareAddress(a *commonpb.Address, b *commonpb.Address) bool {
	if a == b {
		return true
	}
	return a.Ip == b.Ip && a.Port == b.Port
}

func compare2Array(arr1, arr2 interface{}) bool {
	p1, ok := arr1.([]*commonpb.KeyValuePair)
	if ok {
		p2, ok1 := arr2.([]*commonpb.KeyValuePair)
		if ok1 {
			for _, param1 := range p1 {
				sameParams := false
				for _, param2 := range p2 {
					if param1.Key == param2.Key && param1.Value == param2.Value {
						sameParams = true
					}
				}
				if !sameParams {
					return false
				}
			}
			return true
		}
		log.Error("IndexService compare2Array arr2 should be commonpb.KeyValuePair")
		return false
	}
	v1, ok2 := arr1.([]string)
	if ok2 {
		v2, ok3 := arr2.([]string)
		if ok3 {
			for _, s1 := range v1 {
				sameParams := false
				for _, s2 := range v2 {
					if s1 == s2 {
						sameParams = true
					}
				}
				if !sameParams {
					return false
				}
			}
			return true
		}
		log.Error("IndexService compare2Array arr2 type should be string array")
		return false
	}
	log.Error("IndexService compare2Array param type should be commonpb.KeyValuePair or string array")
	return false
}
