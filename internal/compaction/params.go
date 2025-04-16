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

package compaction

import (
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type Params struct {
	EnableStorageV2     bool   `json:"enable_storage_v2,omitempty"`
	BinLogMaxSize       uint64 `json:"binlog_max_size,omitempty"`
	UseMergeSort        bool   `json:"use_merge_sort,omitempty"`
	MaxSegmentMergeSort int    `json:"max_segment_merge_sort,omitempty"`
}

func genParams() Params {
	return Params{
		EnableStorageV2:     paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool(),
		BinLogMaxSize:       paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64(),
		UseMergeSort:        paramtable.Get().DataNodeCfg.UseMergeSort.GetAsBool(),
		MaxSegmentMergeSort: paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.GetAsInt(),
	}
}

func GenerateJSONParams() (string, error) {
	compactionParams := genParams()
	params, err := json.Marshal(compactionParams)
	if err != nil {
		return "", err
	}
	return string(params), nil
}

func ParseParamsFromJSON(jsonStr string) (Params, error) {
	var compactionParams Params
	err := json.Unmarshal([]byte(jsonStr), &compactionParams)
	if err != nil && jsonStr == "" {
		// Ensure the compatibility with the legacy requests sent by the old datacoord.
		return genParams(), nil
	}
	return compactionParams, err
}
