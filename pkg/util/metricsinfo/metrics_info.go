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

	"github.com/milvus-io/milvus-proto/go-api/v2/rgpb"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ComponentInfos defines the interface of all component infos
type ComponentInfos interface{}

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

type SearchParams struct {
	DSL          []string `json:"dsl,omitempty"`
	SearchParams []string `json:"search_params,omitempty"`
	NQ           []int64  `json:"nq,omitempty"`
}

type QueryParams struct {
	SearchParams []*SearchParams `json:"search_params,omitempty"`
	Expr         string          `json:"expr,omitempty"`
	OutputFields string          `json:"output_fields,omitempty"`
}

type SlowQuery struct {
	Time                  string       `json:"time,omitempty"`
	Role                  string       `json:"role,omitempty"`
	Database              string       `json:"database,omitempty"`
	Collection            string       `json:"collection,omitempty"`
	Partitions            string       `json:"partitions,omitempty"`
	ConsistencyLevel      string       `json:"consistency_level,omitempty"`
	UseDefaultConsistency bool         `json:"use_default_consistency,omitempty"`
	GuaranteeTimestamp    uint64       `json:"guarantee_timestamp,omitempty"`
	Duration              string       `json:"duration,omitempty"`
	User                  string       `json:"user,omitempty"`
	QueryParams           *QueryParams `json:"query_params,omitempty"`
	Type                  string       `json:"type,omitempty"`
	TraceID               string       `json:"trace_id,omitempty"`
}

type DmChannel struct {
	NodeID                 int64           `json:"node_id,omitempty"`
	Version                int64           `json:"version,omitempty"`
	CollectionID           int64           `json:"collection_id,omitempty"`
	ChannelName            string          `json:"channel_name,omitempty"`
	UnflushedSegmentIds    []int64         `json:"unflushed_segment_ids,omitempty"`
	FlushedSegmentIds      []int64         `json:"flushed_segment_ids,omitempty"`
	DroppedSegmentIds      []int64         `json:"dropped_segment_ids,omitempty"`
	LevelZeroSegmentIds    []int64         `json:"level_zero_segment_ids,omitempty"`
	PartitionStatsVersions map[int64]int64 `json:"partition_stats_versions,omitempty"`
	WatchState             string          `json:"watch_state,omitempty"`
	StartWatchTS           int64           `json:"start_watch_ts,omitempty"`
}

type Segment struct {
	SegmentID    int64  `json:"segment_id,omitempty"`
	CollectionID int64  `json:"collection_id,omitempty"`
	PartitionID  int64  `json:"partition_id,omitempty"`
	Channel      string `json:"channel,omitempty"`
	NumOfRows    int64  `json:"num_of_rows,omitempty"`
	State        string `json:"state,omitempty"`
	IsImporting  bool   `json:"is_importing,omitempty"`
	Compacted    bool   `json:"compacted,omitempty"`
	Level        string `json:"level,omitempty"`
	IsSorted     bool   `json:"is_sorted,omitempty"`
	NodeID       int64  `json:"node_id,omitempty"`

	// load related
	IsInvisible          bool            `json:"is_invisible,omitempty"`
	LoadedTimestamp      int64           `json:"loaded_timestamp,omitempty"`
	Index                []*SegmentIndex `json:"index,omitempty"`
	ResourceGroup        string          `json:"resource_group,omitempty"`
	LoadedInsertRowCount int64           `json:"loaded_insert_row_count,omitempty"` // inert row count for growing segment that excludes the deleted row count in QueryNode
	MemSize              int64           `json:"mem_size,omitempty"`                // memory size of segment in QueryNode

	// flush related
	FlushedRows    int64 `json:"flushed_rows,omitempty"`
	SyncBufferRows int64 `json:"sync_buffer_rows,omitempty"`
	SyncingRows    int64 `json:"syncing_rows,omitempty"`
}

type SegmentIndex struct {
	IndexFieldID int64 `json:"field_id,omitempty"`
	IndexID      int64 `json:"index_id,omitempty"`
	BuildID      int64 `json:"build_id,omitempty"`
	IndexSize    int64 `json:"index_size,omitempty"`
	IsLoaded     bool  `json:"is_loaded,omitempty"`
}

type QueryCoordTarget struct {
	CollectionID int64        `json:"collection_id,omitempty"`
	Segments     []*Segment   `json:"segments,omitempty"`
	DMChannels   []*DmChannel `json:"dm_channels,omitempty"`
}

type LeaderView struct {
	LeaderID           int64      `json:"leader_id,omitempty"`
	CollectionID       int64      `json:"collection_id,omitempty"`
	NodeID             int64      `json:"node_id,omitempty"`
	Channel            string     `json:"channel,omitempty"`
	Version            int64      `json:"version,omitempty"`
	SealedSegments     []*Segment `json:"sealed_segments,omitempty"`
	GrowingSegments    []*Segment `json:"growing_segments,omitempty"`
	TargetVersion      int64      `json:"target_version,omitempty"`
	NumOfGrowingRows   int64      `json:"num_of_growing_rows,omitempty"`
	UnServiceableError string     `json:"unserviceable_error,omitempty"`
}

type QueryCoordDist struct {
	Segments    []*Segment    `json:"segments,omitempty"`
	DMChannels  []*DmChannel  `json:"dm_channels,omitempty"`
	LeaderViews []*LeaderView `json:"leader_views,omitempty"`
}

type ResourceGroup struct {
	Name  string                    `json:"name,omitempty"`
	Nodes []int64                   `json:"nodes,omitempty"`
	Cfg   *rgpb.ResourceGroupConfig `json:"cfg,omitempty"`
}

type Replica struct {
	ID               int64              `json:"ID,omitempty"`
	CollectionID     int64              `json:"collectionID,omitempty"`
	RWNodes          []int64            `json:"rw_nodes,omitempty"`
	ResourceGroup    string             `json:"resource_group,omitempty"`
	RONodes          []int64            `json:"ro_nodes,omitempty"`
	ChannelToRWNodes map[string][]int64 `json:"channel_to_rw_nodes,omitempty"`
}

// Channel is a subscribed channel of in querynode or datanode.
type Channel struct {
	Name           string `json:"name,omitempty"`
	WatchState     string `json:"watch_state,omitempty"`
	LatestTimeTick string `json:"latest_time_tick,omitempty"` // a time string that indicates the latest time tick of the channel is received
	NodeID         int64  `json:"node_id,omitempty"`
	CollectionID   int64  `json:"collection_id,omitempty"`
	CheckpointTS   string `json:"check_point_ts,omitempty"` // a time string, format like "2006-01-02 15:04:05"
}

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

type QueryNodeCollectionMetrics struct {
	CollectionRows map[int64]int64
}

// QueryNodeInfos implements ComponentInfos
type QueryNodeInfos struct {
	BaseComponentInfos
	SystemConfigurations QueryNodeConfiguration      `json:"system_configurations"`
	QuotaMetrics         *QueryNodeQuotaMetrics      `json:"quota_metrics"`
	CollectionMetrics    *QueryNodeCollectionMetrics `json:"collection_metrics"`
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

type SyncTask struct {
	SegmentID     int64              `json:"segment_id,omitempty"`
	BatchRows     int64              `json:"batch_rows,omitempty"`
	SegmentLevel  string             `json:"segment_level,omitempty"`
	TSFrom        typeutil.Timestamp `json:"ts_from,omitempty"`
	TSTo          typeutil.Timestamp `json:"ts_to,omitempty"`
	DeltaRowCount int64              `json:"delta_row_count,omitempty"`
	FlushSize     int64              `json:"flush_size,omitempty"`
	RunningTime   string             `json:"running_time,omitempty"`
	NodeID        int64              `json:"node_id,omitempty"`
}

// DataNodeInfos implements ComponentInfos
type DataNodeInfos struct {
	BaseComponentInfos
	SystemConfigurations DataNodeConfiguration `json:"system_configurations"`
	QuotaMetrics         *DataNodeQuotaMetrics `json:"quota_metrics"`
}

type DataCoordDist struct {
	Segments   []*Segment   `json:"segments,omitempty"`
	DMChannels []*DmChannel `json:"dm_channels,omitempty"`
}

// DataCoordConfiguration records the configuration of DataCoord.
type DataCoordConfiguration struct {
	SegmentMaxSize float64 `json:"segment_max_size"`
}

type DataCoordIndexInfo struct {
	NumEntitiesIndexed int64
	IndexName          string
	FieldID            int64
}

type DataCoordCollectionInfo struct {
	NumEntitiesTotal int64
	IndexInfo        []*DataCoordIndexInfo
}

type DataCoordCollectionMetrics struct {
	Collections map[int64]*DataCoordCollectionInfo
}

// DataCoordInfos implements ComponentInfos
type DataCoordInfos struct {
	BaseComponentInfos
	SystemConfigurations DataCoordConfiguration      `json:"system_configurations"`
	QuotaMetrics         *DataCoordQuotaMetrics      `json:"quota_metrics"`
	CollectionMetrics    *DataCoordCollectionMetrics `json:"collection_metrics"`
}

type ImportTask struct {
	JobID        int64  `json:"job_id,omitempty"`
	TaskID       int64  `json:"task_id,omitempty"`
	CollectionID int64  `json:"collection_id,omitempty"`
	NodeID       int64  `json:"node_id,omitempty"`
	State        string `json:"state,omitempty"`
	Reason       string `json:"reason,omitempty"`
	TaskType     string `json:"task_type,omitempty"`
	CreatedTime  string `json:"created_time,omitempty"`
	CompleteTime string `json:"complete_time,omitempty"`
}

type CompactionTask struct {
	PlanID         int64   `json:"plan_id,omitempty"`
	CollectionID   int64   `json:"collection_id,omitempty"`
	Type           string  `json:"type,omitempty"`
	State          string  `json:"state,omitempty"`
	FailReason     string  `json:"fail_reason,omitempty"`
	StartTime      int64   `json:"start_time,omitempty"`
	EndTime        int64   `json:"end_time,omitempty"`
	TotalRows      int64   `json:"total_rows,omitempty"`
	InputSegments  []int64 `json:"input_segments,omitempty"`
	ResultSegments []int64 `json:"result_segments,omitempty"`
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
