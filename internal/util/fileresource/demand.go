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

package fileresource

import "github.com/milvus-io/milvus/internal/json"

// AnalyzerResourceNames extracts the remote resource names from analyzer configs.
// All remote file options use the same object shape, including nested tokenizer,
// filter and multi-analyzer configs. Walk objects/arrays without reproducing the
// analyzer grammar or interpreting local paths and built-in dictionary names.
func AnalyzerResourceNames(params []string) map[string]struct{} {
	names := make(map[string]struct{})
	var visit func(any)
	visit = func(value any) {
		switch value := value.(type) {
		case map[string]any:
			if kind, _ := value["type"].(string); kind == "remote" {
				if name, ok := value["resource_name"].(string); ok {
					names[name] = struct{}{}
				}
			}
			for _, child := range value {
				visit(child)
			}
		case []any:
			for _, child := range value {
				visit(child)
			}
		}
	}
	for _, params := range params {
		var value any
		if json.Unmarshal([]byte(params), &value) == nil {
			visit(value)
		}
		// Invalid JSON is rejected by the existing analyzer constructor. It
		// cannot require remote I/O merely to report the same parameter error.
	}
	return names
}
