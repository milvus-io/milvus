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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
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

	// StartOfUserFunctionID represents the starting ID of the user-defined function
	StartOfUserFunctionID = 100
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

	SystemFieldsNum = int64(2)
)

const (
	MinimalScalarIndexEngineVersion = int32(0)
	CurrentScalarIndexEngineVersion = int32(2)
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

	// SegmentBm25LogPath storage path const for bm25 statistic
	SegmentBm25LogPath = `bm25_stats`

	// PartitionStatsPath storage path const for partition stats files
	PartitionStatsPath = `part_stats`

	// AnalyzeStatsPath storage path const for analyze.
	AnalyzeStatsPath = `analyze_stats`
	OffsetMapping    = `offset_mapping`
	Centroids        = "centroids"

	// TextIndexPath storage path const for text index
	TextIndexPath = "text_log"

	// JSONIndexPath storage path const for json index
	JSONIndexPath = "json_key_index_log"
)

// Search, Index parameter keys
const (
	TopKKey         = "topk"
	SearchParamKey  = "search_param"
	SegmentNumKey   = "segment_num"
	WithFilterKey   = "with_filter"
	DataTypeKey     = "data_type"
	ChannelNumKey   = "channel_num"
	WithOptimizeKey = "with_optimize"
	CollectionKey   = "collection"
	RecallEvalKey   = "recall_eval"

	ParamsKey      = "params"
	IndexTypeKey   = "index_type"
	MetricTypeKey  = "metric_type"
	DimKey         = "dim"
	MaxLengthKey   = "max_length"
	MaxCapacityKey = "max_capacity"

	DropRatioBuildKey = "drop_ratio_build"

	IsSparseKey               = "is_sparse"
	AutoIndexName             = "AUTOINDEX"
	BitmapCardinalityLimitKey = "bitmap_cardinality_limit"
	IgnoreGrowing             = "ignore_growing"
	ConsistencyLevel          = "consistency_level"
	HintsKey                  = "hints"

	JSONCastTypeKey     = "json_cast_type"
	JSONPathKey         = "json_path"
	JSONCastFunctionKey = "json_cast_function"
)

// Doc-in-doc-out
const (
	EnableAnalyzerKey = `enable_analyzer`
	AnalyzerParamKey  = `analyzer_params`
)

//  Collection properties key

const (
	CollectionTTLConfigKey      = "collection.ttl.seconds"
	CollectionAutoCompactionKey = "collection.autocompaction.enabled"
	CollectionDescription       = "collection.description"

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
	DatabaseReplicaNumber       = "database.replica.number"
	DatabaseResourceGroups      = "database.resource_groups"
	DatabaseDiskQuotaKey        = "database.diskQuota.mb"
	DatabaseMaxCollectionsKey   = "database.max.collections"
	DatabaseForceDenyWritingKey = "database.force.deny.writing"
	DatabaseForceDenyReadingKey = "database.force.deny.reading"

	DatabaseForceDenyDDLKey           = "database.force.deny.ddl" // all ddl
	DatabaseForceDenyCollectionDDLKey = "database.force.deny.collectionDDL"
	DatabaseForceDenyPartitionDDLKey  = "database.force.deny.partitionDDL"
	DatabaseForceDenyIndexDDLKey      = "database.force.deny.index"
	DatabaseForceDenyFlushDDLKey      = "database.force.deny.flush"
	DatabaseForceDenyCompactionDDLKey = "database.force.deny.compaction"

	// collection level load properties
	CollectionReplicaNumber  = "collection.replica.number"
	CollectionResourceGroups = "collection.resource_groups"
)

// common properties
const (
	MmapEnabledKey             = "mmap.enabled"
	LazyLoadEnableKey          = "lazyload.enabled"
	LoadPriorityKey            = "load_priority"
	PartitionKeyIsolationKey   = "partitionkey.isolation"
	FieldSkipLoadKey           = "field.skipLoad"
	IndexOffsetCacheEnabledKey = "indexoffsetcache.enabled"
	ReplicateIDKey             = "replicate.id"
	ReplicateEndTSKey          = "replicate.endTS"
	IndexNonEncoding           = "index.nonEncoding"
)

const (
	PropertiesKey string = "properties"
	TraceIDKey    string = "uber-trace-id"
)

func IsSystemField(fieldID int64) bool {
	return fieldID < StartOfUserFieldID
}

func IsMmapDataEnabled(kvs ...*commonpb.KeyValuePair) (bool, bool) {
	for _, kv := range kvs {
		if kv.Key == MmapEnabledKey {
			enable, _ := strconv.ParseBool(kv.Value)
			return enable, true
		}
	}
	return false, false
}

func IsMmapIndexEnabled(kvs ...*commonpb.KeyValuePair) (bool, bool) {
	for _, kv := range kvs {
		if kv.Key == MmapEnabledKey {
			enable, _ := strconv.ParseBool(kv.Value)
			return enable, true
		}
	}
	return false, false
}

func GetIndexType(indexParams []*commonpb.KeyValuePair) string {
	for _, param := range indexParams {
		if param.Key == IndexTypeKey {
			return param.Value
		}
	}
	log.Warn("IndexType not found in indexParams")
	return ""
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

func IsPartitionKeyIsolationKvEnabled(kvs ...*commonpb.KeyValuePair) (bool, error) {
	for _, kv := range kvs {
		if kv.Key == PartitionKeyIsolationKey {
			val, err := strconv.ParseBool(strings.ToLower(kv.Value))
			if err != nil {
				return false, errors.Wrap(err, "failed to parse partition key isolation")
			}
			return val, nil
		}
	}
	return false, nil
}

func IsPartitionKeyIsolationPropEnabled(props map[string]string) (bool, error) {
	val, ok := props[PartitionKeyIsolationKey]
	if !ok {
		return false, nil
	}
	iso, parseErr := strconv.ParseBool(val)
	if parseErr != nil {
		return false, errors.Wrap(parseErr, "failed to parse partition key isolation property")
	}
	return iso, nil
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

			return lo.Map(rgs, func(rg string, _ int) string { return strings.TrimSpace(rg) }), nil
		}
	}

	return nil, fmt.Errorf("database property not found: %s", DatabaseResourceGroups)
}

func CollectionLevelReplicaNumber(kvs []*commonpb.KeyValuePair) (int64, error) {
	for _, kv := range kvs {
		if kv.Key == CollectionReplicaNumber {
			replicaNum, err := strconv.ParseInt(kv.Value, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid collection property: [key=%s] [value=%s]", kv.Key, kv.Value)
			}

			return replicaNum, nil
		}
	}

	return 0, fmt.Errorf("collection property not found: %s", CollectionReplicaNumber)
}

func CollectionLevelResourceGroups(kvs []*commonpb.KeyValuePair) ([]string, error) {
	for _, kv := range kvs {
		if kv.Key == CollectionResourceGroups {
			invalidPropValue := fmt.Errorf("invalid collection property: [key=%s] [value=%s]", kv.Key, kv.Value)
			if len(kv.Value) == 0 {
				return nil, invalidPropValue
			}

			rgs := strings.Split(kv.Value, ",")
			if len(rgs) == 0 {
				return nil, invalidPropValue
			}

			return lo.Map(rgs, func(rg string, _ int) string { return strings.TrimSpace(rg) }), nil
		}
	}

	return nil, fmt.Errorf("collection property not found: %s", CollectionReplicaNumber)
}

// GetCollectionLoadFields returns the load field ids according to the type params.
func GetCollectionLoadFields(schema *schemapb.CollectionSchema, skipDynamicField bool) []int64 {
	filter := func(field *schemapb.FieldSchema, _ int) (int64, bool) {
		// skip system field
		if IsSystemField(field.GetFieldID()) {
			return field.GetFieldID(), false
		}
		// skip dynamic field if specified
		if field.IsDynamic && skipDynamicField {
			return field.GetFieldID(), false
		}

		v, err := ShouldFieldBeLoaded(field.GetTypeParams())
		if err != nil {
			log.Warn("type param parse skip load failed", zap.Error(err))
			// if configuration cannot be parsed, ignore it and load field
			return field.GetFieldID(), true
		}
		return field.GetFieldID(), v
	}
	fields := lo.FilterMap(schema.GetFields(), filter)

	fieldsNum := len(schema.GetFields())
	for _, structField := range schema.GetStructArrayFields() {
		fields = append(fields, lo.FilterMap(structField.GetFields(), filter)...)
		fieldsNum += len(structField.GetFields())
	}

	// empty fields list means all fields will be loaded
	if len(fields) == fieldsNum-int(SystemFieldsNum) {
		return []int64{}
	}
	return fields
}

func ShouldFieldBeLoaded(kvs []*commonpb.KeyValuePair) (bool, error) {
	for _, kv := range kvs {
		if kv.GetKey() == FieldSkipLoadKey {
			val, err := strconv.ParseBool(kv.GetValue())
			return !val, err
		}
	}
	return true, nil
}

func IsReplicateEnabled(kvs []*commonpb.KeyValuePair) (bool, bool) {
	replicateID, ok := GetReplicateID(kvs)
	return replicateID != "", ok
}

func GetReplicateID(kvs []*commonpb.KeyValuePair) (string, bool) {
	for _, kv := range kvs {
		if kv.GetKey() == ReplicateIDKey {
			return kv.GetValue(), true
		}
	}
	return "", false
}

func GetReplicateEndTS(kvs []*commonpb.KeyValuePair) (uint64, bool) {
	for _, kv := range kvs {
		if kv.GetKey() == ReplicateEndTSKey {
			ts, err := strconv.ParseUint(kv.GetValue(), 10, 64)
			if err != nil {
				log.Warn("parse replicate end ts failed", zap.Error(err), zap.Stack("stack"))
				return 0, false
			}
			return ts, true
		}
	}
	return 0, false
}

func ValidateAutoIndexMmapConfig(autoIndexConfigEnable, isVectorField bool, indexParams map[string]string) error {
	if !autoIndexConfigEnable {
		return nil
	}

	_, ok := indexParams[MmapEnabledKey]
	if ok && isVectorField {
		return errors.New("mmap index is not supported to config for the collection in auto index mode")
	}
	return nil
}
