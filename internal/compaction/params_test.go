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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestGetJSONParams(t *testing.T) {
	paramtable.Init()
	jsonStr, err := GenerateJSONParams()
	assert.NoError(t, err)

	var result Params
	err = json.Unmarshal([]byte(jsonStr), &result)
	assert.NoError(t, err)
	storageVersion := storage.StorageV1
	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}
	assert.Equal(t, Params{
		StorageVersion:            storageVersion,
		BinLogMaxSize:             paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64(),
		UseMergeSort:              paramtable.Get().DataNodeCfg.UseMergeSort.GetAsBool(),
		MaxSegmentMergeSort:       paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.GetAsInt(),
		PreferSegmentSizeRatio:    paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat(),
		BloomFilterApplyBatchSize: paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt(),
		StorageConfig:             CreateStorageConfig(),
	}, result)
}

func TestGetParamsFromJSON(t *testing.T) {
	input := `{
		"storage_version": 0,
		"binlog_max_size": 4096,
		"use_merge_sort": false,
		"max_segment_merge_sort": 2,
		"prefer_segment_size_ratio": 0.1,
		"bloom_filter_apply_batch_size": 1000
	}`

	expected := Params{
		StorageVersion:            storage.StorageV1,
		BinLogMaxSize:             4096,
		UseMergeSort:              false,
		MaxSegmentMergeSort:       2,
		PreferSegmentSizeRatio:    0.1,
		BloomFilterApplyBatchSize: 1000,
	}

	result, err := ParseParamsFromJSON(input)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetParamsFromJSON_InvalidJSON(t *testing.T) {
	invalidJSON := `{ this is not valid json }`
	_, err := ParseParamsFromJSON(invalidJSON)
	assert.Error(t, err)
}

func TestGetParamsFromJSON_EmptyJSON(t *testing.T) {
	// Test compatibility
	emptyJSON := ``
	result, err := ParseParamsFromJSON(emptyJSON)
	assert.NoError(t, err)
	storageVersion := storage.StorageV1
	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}
	assert.Equal(t, Params{
		StorageVersion:            storageVersion,
		BinLogMaxSize:             paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64(),
		UseMergeSort:              paramtable.Get().DataNodeCfg.UseMergeSort.GetAsBool(),
		MaxSegmentMergeSort:       paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.GetAsInt(),
		PreferSegmentSizeRatio:    paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat(),
		BloomFilterApplyBatchSize: paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt(),
		StorageConfig:             CreateStorageConfig(),
	}, result)
}
