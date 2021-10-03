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
)

// DeployMetrics records the deploy information of nodes.
type DeployMetrics struct {
	SystemVersion string `json:"system_version"`
	DeployMode    string `json:"deploy_mode"`
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
}

// QueryNodeConfiguration records the configuration of query node.
type QueryNodeConfiguration struct {
	SearchReceiveBufSize       int64 `json:"search_receive_buf_size"`
	SearchPulsarBufSize        int64 `json:"search_pulsar_buf_size"`
	SearchResultReceiveBufSize int64 `json:"search_result_receive_buf_size"`

	RetrieveReceiveBufSize       int64 `json:"retrieve_receive_buf_size"`
	RetrievePulsarBufSize        int64 `json:"retrieve_pulsar_buf_size"`
	RetrieveResultReceiveBufSize int64 `json:"retrieve_result_receive_buf_size"`

	SimdType string `json:"simd_type"`
}

// QueryNodeInfos implements ComponentInfos
type QueryNodeInfos struct {
	BaseComponentInfos
	SystemConfigurations QueryNodeConfiguration `json:"system_configurations"`
}

// QueryCoordConfiguration records the configuration of query coordinator.
type QueryCoordConfiguration struct {
	SearchChannelPrefix       string `json:"search_channel_prefix"`
	SearchResultChannelPrefix string `json:"search_result_channel_prefix"`
}

// QueryCoordInfos implements ComponentInfos
type QueryCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations QueryCoordConfiguration `json:"system_configurations"`
}

// ProxyConfiguration records the configuration of proxy.
type ProxyConfiguration struct {
	DefaultPartitionName string `json:"default_partition_name"`
	DefaultIndexName     string `json:"default_index_name"`
}

// ProxyInfos implements ComponentInfos
type ProxyInfos struct {
	BaseComponentInfos
	SystemConfigurations ProxyConfiguration `json:"system_configurations"`
}

// IndexNodeConfiguration records the configuration of index node.
type IndexNodeConfiguration struct {
	MinioBucketName string `json:"minio_bucket_name"`

	SimdType string `json:"simd_type"`
}

// IndexNodeInfos implements ComponentInfos
type IndexNodeInfos struct {
	BaseComponentInfos
	SystemConfigurations IndexNodeConfiguration `json:"system_configurations"`
}

// IndexCoordConfiguration records the configuration of index coordinator.
type IndexCoordConfiguration struct {
	MinioBucketName string `json:"minio_bucket_name"`
}

// IndexCoordInfos implements ComponentInfos
type IndexCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations IndexCoordConfiguration `json:"system_configurations"`
}

// DataNodeConfiguration records the configuration of data node.
type DataNodeConfiguration struct {
	FlushInsertBufferSize int64 `json:"flush_insert_buffer_size"`
}

// DataNodeInfos implements ComponentInfos
type DataNodeInfos struct {
	BaseComponentInfos
	SystemConfigurations DataNodeConfiguration `json:"system_configurations"`
}

// DataCoordConfiguration records the configuration of data coordinator.
type DataCoordConfiguration struct {
	SegmentMaxSize float64 `json:"segment_max_size"`
}

// DataCoordInfos implements ComponentInfos
type DataCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations DataCoordConfiguration `json:"system_configurations"`
}

// RootCoordConfiguration records the configuration of root coordinator.
type RootCoordConfiguration struct {
	MinSegmentSizeToEnableIndex int64 `json:"min_segment_size_to_enable_index"`
}

// RootCoordInfos implements ComponentInfos
type RootCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations RootCoordConfiguration `json:"system_configurations"`
}
