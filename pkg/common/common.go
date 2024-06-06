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

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

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
	DefaultPartitionsWithPartitionKey = int64(16)

	// InvalidPartitionID indicates that the partition is not specified. It will be set when the partitionName is empty
	InvalidPartitionID = int64(-1)

	// AllPartitionsID indicates data applies to all partitions.
	AllPartitionsID = int64(-1)

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

	// PartitionStatsPath storage path const for partition stats files
	PartitionStatsPath = `part_stats`

	// AnalyzeStatsPath storage path const for analyze.
	AnalyzeStatsPath = `analyze_stats`
	OffsetMapping    = `offset_mapping`
	Centroids        = "centroids"
)

// Search, Index parameter keys
const (
	TopKKey         = "topk"
	SearchParamKey  = "search_param"
	SegmentNumKey   = "segment_num"
	WithFilterKey   = "with_filter"
	WithOptimizeKey = "with_optimize"
	CollectionKey   = "collection"

	IndexParamsKey = "params"
	IndexTypeKey   = "index_type"
	MetricTypeKey  = "metric_type"
	DimKey         = "dim"
	MaxLengthKey   = "max_length"
	MaxCapacityKey = "max_capacity"

	DropRatioBuildKey = "drop_ratio_build"

	BitmapCardinalityLimitKey = "bitmap_cardinality_limit"
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

	PartitionDiskQuotaKey = "partition.diskProtection.diskQuota.mb"

	// database level properties
	DatabaseReplicaNumber  = "database.replica.number"
	DatabaseResourceGroups = "database.resource_groups"
)

// common properties
const (
	MmapEnabledKey    = "mmap.enabled"
	LazyLoadEnableKey = "lazyload.enabled"
)

const (
	PropertiesKey string = "properties"
	TraceIDKey    string = "uber-trace-id"
)

func IsSystemField(fieldID int64) bool {
	return fieldID < StartOfUserFieldID
}

func IsMmapEnabled(kvs ...*commonpb.KeyValuePair) bool {
	for _, kv := range kvs {
		if kv.Key == MmapEnabledKey && strings.ToLower(kv.Value) == "true" {
			return true
		}
	}
	return false
}

func IsFieldMmapEnabled(schema *schemapb.CollectionSchema, fieldID int64) bool {
	for _, field := range schema.GetFields() {
		if field.GetFieldID() == fieldID {
			return IsMmapEnabled(field.GetTypeParams()...)
		}
	}
	return false
}

func FieldHasMmapKey(schema *schemapb.CollectionSchema, fieldID int64) bool {
	for _, field := range schema.GetFields() {
		if field.GetFieldID() == fieldID {
			for _, kv := range field.GetTypeParams() {
				if kv.Key == MmapEnabledKey {
					return true
				}
			}
			return false
		}
	}
	return false
}

func HasLazyload(props []*commonpb.KeyValuePair) bool {
	for _, kv := range props {
		if kv.Key == LazyLoadEnableKey {
			return true
		}
	}
	return false
}

func IsCollectionLazyLoadEnabled(kvs ...*commonpb.KeyValuePair) bool {
	for _, kv := range kvs {
		if kv.Key == LazyLoadEnableKey && strings.ToLower(kv.Value) == "true" {
			return true
		}
	}
	return false
}

const (
	// LatestVerision is the magic number for watch latest revision
	LatestRevision = int64(-1)
)

func DatabaseLevelReplicaNumber(kvs []*commonpb.KeyValuePair) (int64, error) {
	for _, kv := range kvs {
		if kv.Key == DatabaseReplicaNumber {
			replicaNum, err := strconv.ParseInt(kv.Value, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid database property: [key=%s] [value=%s]", kv.Key, kv.Value)
			}

			return replicaNum, nil
		}
	}

	return 0, fmt.Errorf("database property not found: %s", DatabaseReplicaNumber)
}

func DatabaseLevelResourceGroups(kvs []*commonpb.KeyValuePair) ([]string, error) {
	for _, kv := range kvs {
		if kv.Key == DatabaseResourceGroups {
			invalidPropValue := fmt.Errorf("invalid database property: [key=%s] [value=%s]", kv.Key, kv.Value)
			if len(kv.Value) == 0 {
				return nil, invalidPropValue
			}

			rgs := strings.Split(kv.Value, ",")
			if len(rgs) == 0 {
				return nil, invalidPropValue
			}

			return rgs, nil
		}
	}

	return nil, fmt.Errorf("database property not found: %s", DatabaseResourceGroups)
}
