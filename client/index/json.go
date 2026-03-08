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

package index

import (
	"github.com/milvus-io/milvus/pkg/v2/common"
)

var _ Index = &JSONPathIndex{}

// JSONPathIndex is the pre-defined index model for json path scalar index.
type JSONPathIndex struct {
	scalarIndex
	jsonCastType string
	jsonPath     string
}

// WithIndexName setup the index name of JsonPathIndex.
func (idx *JSONPathIndex) WithIndexName(name string) *JSONPathIndex {
	idx.name = name
	return idx
}

// Params implements Index interface
// returns the create index related parameters.
func (idx *JSONPathIndex) Params() map[string]string {
	return map[string]string{
		IndexTypeKey:           string(idx.indexType),
		common.JSONCastTypeKey: idx.jsonCastType,
		common.JSONPathKey:     idx.jsonPath,
	}
}

// NewJSONPathIndex creates a `JsonPathIndex` with provided parameters.
func NewJSONPathIndex(indexType IndexType, jsonCastType string, jsonPath string) *JSONPathIndex {
	return &JSONPathIndex{
		scalarIndex: scalarIndex{
			indexType: indexType,
		},
		jsonCastType: jsonCastType,
		jsonPath:     jsonPath,
	}
}
