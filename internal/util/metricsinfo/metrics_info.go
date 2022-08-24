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

package metricsinfo

import (
	"encoding/json"
)

// ComponentInfos defines the interface of all component infos
type ComponentInfos interface {
}

// MarshalComponentInfos returns the json string of ComponentInfos
func MarshalComponentInfos(infos ComponentInfos) (string, error) {
	binary, err := json.Marshal(infos)
	return string(binary), err
}

// UnmarshalComponentInfos constructs a ComponentInfos object using a json string
func UnmarshalComponentInfos(s string, infos ComponentInfos) error {
	return json.Unmarshal([]byte(s), infos)
}

// HardwareMetrics records the hardware information of nodes.
type HardwareMetrics struct {
	IP           string  `json:"ip"`
	CPUCoreCount int     `json:"cpu_core_count"`
	CPUCoreUsage float64 `json:"cpu_core_usage"`
	Memory       uint64  `json:"memory"`
	MemoryUsage  uint64  `json:"memory_usage"`

	// how to metric disk & disk usage in distributed storage
	Disk      uint64 `json:"disk"`
	DiskUsage uint64 `json:"disk_usage"`
}

const (
	// GitCommitEnvKey defines the key to retrieve the commit corresponding to the current milvus version
	// from the metrics information
	GitCommitEnvKey = "MILVUS_GIT_COMMIT"

	// DeployModeEnvKey defines the key to retrieve the current milvus deployment mode
	// from the metrics information
	DeployModeEnvKey = "DEPLOY_MODE"

	// ClusterDeployMode represents distributed deployment mode
	ClusterDeployMode = "DISTRIBUTED"

	// StandaloneDeployMode represents the stand-alone deployment mode
	StandaloneDeployMode = "STANDALONE"

	// GitBuildTagsEnvKey build tag
	GitBuildTagsEnvKey = "MILVUS_GIT_BUILD_TAGS"

	// MilvusBuildTimeEnvKey build time
	MilvusBuildTimeEnvKey = "MILVUS_BUILD_TIME"

	// MilvusUsedGoVersion used go version
	MilvusUsedGoVersion = "MILVUS_USED_GO_VERSION"
)

// DeployMetrics records the deploy information of nodes.
type DeployMetrics struct {
	SystemVersion string `json:"system_version"`
	DeployMode    string `json:"deploy_mode"`
	BuildVersion  string `json:"build_version"`
	BuildTime     string `json:"build_time"`
	UsedGoVersion string `json:"used_go_version"`
}

// BaseComponentInfos contains basic information that all components should have.
type BaseComponentInfos struct {
	HasError      bool            `json:"has_error"`
	ErrorReason   string          `json:"error_reason"`
	Name          string          `json:"name"`
	HardwareInfos HardwareMetrics `json:"hardware_infos"`
	SystemInfo    DeployMetrics   `json:"system_info"`
	CreatedTime   string          `json:"created_time"`
	UpdatedTime   string          `json:"updated_time"`
	Type          string          `json:"type"`
	ID            int64           `json:"id"`
}

// QueryNodeConfiguration records the configuration of QueryNode.
type QueryNodeConfiguration struct {
	SimdType string `json:"simd_type"`
}

// QueryNodeInfos implements ComponentInfos
type QueryNodeInfos struct {
	BaseComponentInfos
	SystemConfigurations QueryNodeConfiguration `json:"system_configurations"`
	QuotaMetrics         *QueryNodeQuotaMetrics `json:"quota_metrics"`
}

// QueryCoordConfiguration records the configuration of QueryCoord.
type QueryCoordConfiguration struct {
	SearchChannelPrefix       string `json:"search_channel_prefix"`
	SearchResultChannelPrefix string `json:"search_result_channel_prefix"`
}

// QueryCoordInfos implements ComponentInfos
type QueryCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations QueryCoordConfiguration `json:"system_configurations"`
}

// ProxyConfiguration records the configuration of Proxy.
type ProxyConfiguration struct {
	DefaultPartitionName string `json:"default_partition_name"`
	DefaultIndexName     string `json:"default_index_name"`
}

// ProxyInfos implements ComponentInfos
type ProxyInfos struct {
	BaseComponentInfos
	SystemConfigurations ProxyConfiguration `json:"system_configurations"`
	QuotaMetrics         *ProxyQuotaMetrics `json:"quota_metrics"`
}

// IndexNodeConfiguration records the configuration of IndexNode.
type IndexNodeConfiguration struct {
	MinioBucketName string `json:"minio_bucket_name"`

	SimdType string `json:"simd_type"`
}

// IndexNodeInfos implements ComponentInfos
type IndexNodeInfos struct {
	BaseComponentInfos
	SystemConfigurations IndexNodeConfiguration `json:"system_configurations"`
}

// IndexCoordConfiguration records the configuration of IndexCoord.
type IndexCoordConfiguration struct {
	MinioBucketName string `json:"minio_bucket_name"`
}

// IndexCoordInfos implements ComponentInfos
type IndexCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations IndexCoordConfiguration `json:"system_configurations"`
}

// DataNodeConfiguration records the configuration of DataNode.
type DataNodeConfiguration struct {
	FlushInsertBufferSize int64 `json:"flush_insert_buffer_size"`
}

// DataNodeInfos implements ComponentInfos
type DataNodeInfos struct {
	BaseComponentInfos
	SystemConfigurations DataNodeConfiguration `json:"system_configurations"`
	QuotaMetrics         *DataNodeQuotaMetrics `json:"quota_metrics"`
}

// DataCoordConfiguration records the configuration of DataCoord.
type DataCoordConfiguration struct {
	SegmentMaxSize float64 `json:"segment_max_size"`
}

// DataCoordInfos implements ComponentInfos
type DataCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations DataCoordConfiguration `json:"system_configurations"`
}

// RootCoordConfiguration records the configuration of RootCoord.
type RootCoordConfiguration struct {
	MinSegmentSizeToEnableIndex int64 `json:"min_segment_size_to_enable_index"`
}

// RootCoordInfos implements ComponentInfos
type RootCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations RootCoordConfiguration `json:"system_configurations"`
}
