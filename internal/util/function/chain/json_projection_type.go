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

package chain

import json "github.com/milvus-io/milvus/internal/json"

func convertJSONBool(value any) (bool, bool) {
	converted, ok := value.(bool)
	return converted, ok
}

func convertJSONInt64(value any) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	converted, err := number.Int64()
	return converted, err == nil
}

func convertJSONDouble(value any) (float64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	converted, err := number.Float64()
	return converted, err == nil
}

func convertJSONString(value any) (string, bool) {
	converted, ok := value.(string)
	return converted, ok
}
