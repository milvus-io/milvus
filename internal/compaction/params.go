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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type Params struct {
	StorageVersion            int64                  `json:"storage_version,omitempty"`
	BinLogMaxSize             uint64                 `json:"binlog_max_size,omitempty"`
	UseMergeSort              bool                   `json:"use_merge_sort,omitempty"`
	MaxSegmentMergeSort       int                    `json:"max_segment_merge_sort,omitempty"`
	PreferSegmentSizeRatio    float64                `json:"prefer_segment_size_ratio,omitempty"`
	BloomFilterApplyBatchSize int                    `json:"bloom_filter_apply_batch_size,omitempty"`
	StorageConfig             *indexpb.StorageConfig `json:"storage_config,omitempty"`
}

func GenParams() Params {
	storageVersion := storage.StorageV1
	if paramtable.Get().CommonCfg.EnableStorageV2.GetAsBool() {
		storageVersion = storage.StorageV2
	}
	return Params{
		StorageVersion:            storageVersion,
		BinLogMaxSize:             paramtable.Get().DataNodeCfg.BinLogMaxSize.GetAsUint64(),
		UseMergeSort:              paramtable.Get().DataNodeCfg.UseMergeSort.GetAsBool(),
		MaxSegmentMergeSort:       paramtable.Get().DataNodeCfg.MaxSegmentMergeSort.GetAsInt(),
		PreferSegmentSizeRatio:    paramtable.Get().DataCoordCfg.ClusteringCompactionPreferSegmentSizeRatio.GetAsFloat(),
		BloomFilterApplyBatchSize: paramtable.Get().CommonCfg.BloomFilterApplyBatchSize.GetAsInt(),
		StorageConfig:             CreateStorageConfig(),
	}
}

func GenerateJSONParams() (string, error) {
	compactionParams := GenParams()
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
		return GenParams(), nil
	}
	return compactionParams, err
}

func CreateStorageConfig() *indexpb.StorageConfig {
	var storageConfig *indexpb.StorageConfig

	if paramtable.Get().CommonCfg.StorageType.GetValue() == "local" {
		storageConfig = &indexpb.StorageConfig{
			RootPath:    paramtable.Get().LocalStorageCfg.Path.GetValue(),
			StorageType: paramtable.Get().CommonCfg.StorageType.GetValue(),
		}
	} else {
		storageConfig = &indexpb.StorageConfig{
			Address:           paramtable.Get().MinioCfg.Address.GetValue(),
			AccessKeyID:       paramtable.Get().MinioCfg.AccessKeyID.GetValue(),
			SecretAccessKey:   paramtable.Get().MinioCfg.SecretAccessKey.GetValue(),
			UseSSL:            paramtable.Get().MinioCfg.UseSSL.GetAsBool(),
			SslCACert:         paramtable.Get().MinioCfg.SslCACert.GetValue(),
			BucketName:        paramtable.Get().MinioCfg.BucketName.GetValue(),
			RootPath:          paramtable.Get().MinioCfg.RootPath.GetValue(),
			UseIAM:            paramtable.Get().MinioCfg.UseIAM.GetAsBool(),
			IAMEndpoint:       paramtable.Get().MinioCfg.IAMEndpoint.GetValue(),
			StorageType:       paramtable.Get().CommonCfg.StorageType.GetValue(),
			Region:            paramtable.Get().MinioCfg.Region.GetValue(),
			UseVirtualHost:    paramtable.Get().MinioCfg.UseVirtualHost.GetAsBool(),
			CloudProvider:     paramtable.Get().MinioCfg.CloudProvider.GetValue(),
			RequestTimeoutMs:  paramtable.Get().MinioCfg.RequestTimeoutMs.GetAsInt64(),
			GcpCredentialJSON: paramtable.Get().MinioCfg.GcpCredentialJSON.GetValue(),
		}
	}

	return storageConfig
}
