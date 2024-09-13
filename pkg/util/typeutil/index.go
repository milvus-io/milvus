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

package typeutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

// CompareIndexParams compares indexParam1 with indexParam2. When all keys of indexParam1 exist in indexParam2,
// and the corresponding value are the same as in indexParam2, return true
// Otherwise return false
func CompareIndexParams(indexParam1, indexParam2 []*commonpb.KeyValuePair) bool {
	if indexParam1 == nil && indexParam2 == nil {
		return true
	}
	if indexParam1 == nil || indexParam2 == nil {
		return false
	}
	if len(indexParam1) != len(indexParam2) {
		return false
	}
	paramMap1 := make(map[string]string)
	paramMap2 := make(map[string]string)

	for _, kv := range indexParam1 {
		paramMap1[kv.Key] = kv.Value
	}
	for _, kv := range indexParam2 {
		paramMap2[kv.Key] = kv.Value
	}

	for k, v := range paramMap1 {
		if _, ok := paramMap2[k]; !ok || v != paramMap2[k] {
			return false
		}
	}

	return true
}
