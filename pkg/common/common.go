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

package common

import "encoding/binary"

// system field id:
// 0: unique row id
// 1: timestamp
// 100: first user field id
// 101: second user field id
// 102: ...

const (
	// StartOfUserFieldID represents the starting ID of the user-defined field
	StartOfUserFieldID = 100

	// RowIDField is the ID of the RowID field reserved by the system
	RowIDField = 0

	// TimeStampField is the ID of the Timestamp field reserved by the system
	TimeStampField = 1

	// RowIDFieldName defines the name of the RowID field
	RowIDFieldName = "RowID"

	// TimeStampFieldName defines the name of the Timestamp field
	TimeStampFieldName = "Timestamp"

	// MetaFieldName is the field name of dynamic schema
	MetaFieldName = "$meta"

	// DefaultShardsNum defines the default number of shards when creating a collection
	DefaultShardsNum = int32(1)

	// DefaultPartitionsWithPartitionKey defines the default number of partitions when use partition key
	DefaultPartitionsWithPartitionKey = int64(64)

	// InvalidPartitionID indicates that the partition is not specified. It will be set when the partitionName is empty
	InvalidPartitionID = int64(-1)

	// InvalidFieldID indicates that the field does not exist . It will be set when the field is not found.
	InvalidFieldID = int64(-1)

	// NotRegisteredID means node is not registered into etcd.
	NotRegisteredID = int64(-1)

	// InvalidNodeID indicates that node is not valid in querycoord replica or shard cluster.
	InvalidNodeID = int64(-1)
)

// Endian is type alias of binary.LittleEndian.
// Milvus uses little endian by default.
var Endian = binary.LittleEndian

const (
	// SegmentInsertLogPath storage path const for segment insert binlog.
	SegmentInsertLogPath = `insert_log`

	// SegmentDeltaLogPath storage path const for segment delta log.
	SegmentDeltaLogPath = `delta_log`

	// SegmentStatslogPath storage path const for segment stats log.
	SegmentStatslogPath = `stats_log`

	// SegmentIndexPath storage path const for segment index files.
	SegmentIndexPath = `index_files`
)

// Search, Index parameter keys
const (
	TopKKey        = "topk"
	SearchParamKey = "search_param"
	SegmentNumKey  = "segment_num"
	WithFilterKey  = "with_filter"
	CollectionKey  = "collection"

	IndexParamsKey = "params"
	IndexTypeKey   = "index_type"
	MetricTypeKey  = "metric_type"
	DimKey         = "dim"
	MaxLengthKey   = "max_length"
)

//  Collection properties key

const (
	CollectionTTLConfigKey      = "collection.ttl.seconds"
	CollectionAutoCompactionKey = "collection.autocompaction.enabled"

	// rate limit
	CollectionInsertRateMaxKey   = "collection.insertRate.max.mb"
	CollectionInsertRateMinKey   = "collection.insertRate.min.mb"
	CollectionUpsertRateMaxKey   = "collection.upsertRate.max.mb"
	CollectionUpsertRateMinKey   = "collection.upsertRate.min.mb"
	CollectionDeleteRateMaxKey   = "collection.deleteRate.max.mb"
	CollectionDeleteRateMinKey   = "collection.deleteRate.min.mb"
	CollectionBulkLoadRateMaxKey = "collection.bulkLoadRate.max.mb"
	CollectionBulkLoadRateMinKey = "collection.bulkLoadRate.min.mb"
	CollectionQueryRateMaxKey    = "collection.queryRate.max.qps"
	CollectionQueryRateMinKey    = "collection.queryRate.min.qps"
	CollectionSearchRateMaxKey   = "collection.searchRate.max.vps"
	CollectionSearchRateMinKey   = "collection.searchRate.min.vps"
	CollectionDiskQuotaKey       = "collection.diskProtection.diskQuota.mb"
)

const (
	PropertiesKey string = "properties"
	TraceIDKey    string = "uber-trace-id"
)

func IsSystemField(fieldID int64) bool {
	return fieldID < StartOfUserFieldID
}

const (
	// LatestVerision is the magic number for watch latest revision
	LatestRevision = int64(-1)
)
