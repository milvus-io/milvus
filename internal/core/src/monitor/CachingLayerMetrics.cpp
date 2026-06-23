// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include "cachinglayer/Metrics.h"

#include <map>
#include <string>

namespace milvus::cachinglayer::monitor {

DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(vector_field, memory)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(vector_index, memory)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(scalar_field, memory)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(scalar_index, memory)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(other, memory)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(vector_field, disk)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(vector_index, disk)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(scalar_field, disk)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(scalar_index, disk)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(other, disk)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(vector_field, mixed)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(vector_index, mixed)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(scalar_field, mixed)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(scalar_index, mixed)
DEFINE_LABEL_MAP_WITH_DATA_TYPE_AND_LOCATION(other, mixed)

DEFINE_LABEL_MAP_WITH_LOCATION(memory)
DEFINE_LABEL_MAP_WITH_LOCATION(disk)
DEFINE_LABEL_MAP_WITH_LOCATION(mixed)

DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_LOCATION(
    internal_cache_capacity_bytes, "[cpp]cache capacity in bytes")
DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_LOCATION(
    internal_cache_high_watermark_bytes,
    "[cpp]cache high watermark in bytes")
DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_LOCATION(
    internal_cache_low_watermark_bytes, "[cpp]cache low watermark in bytes")
DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_slot_count, "[cpp]cache slot count")
DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_cell_count, "[cpp]cache cell count")
DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_loaded_bytes, "[cpp]cache loaded bytes")
DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_loading_bytes, "[cpp]cache loading bytes")
DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_cell_loading_count, "[cpp]cache loading cell count")
DEFINE_PROMETHEUS_GAUGE_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_cell_loaded_count, "[cpp]cache loaded cell count")

DEFINE_PROMETHEUS_GAUGE_FAMILY(
    internal_cache_shard_disk_usage_bytes,
    "[cpp]cache loaded disk usage in bytes by shard")

DEFINE_PROMETHEUS_COUNTER_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_access_event_total, "[cpp]cache access event count")
DEFINE_PROMETHEUS_COUNTER_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_cell_access_hit_bytes_total,
    "[cpp]cache cell access hit bytes")
DEFINE_PROMETHEUS_COUNTER_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_cell_access_miss_bytes_total,
    "[cpp]cache cell access miss bytes")

DEFINE_PROMETHEUS_COUNTER_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_load_event_fail_total, "[cpp]cache load failure count")
DEFINE_PROMETHEUS_HISTOGRAM_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_load_latency_microseconds,
    milvus::monitor::buckets,
    "[cpp]cache load latency in microseconds")

DEFINE_PROMETHEUS_COUNTER_METRIC_WITH_LOCATION(
    internal_cache_eviction_event_total, "[cpp]cache eviction event count")
DEFINE_PROMETHEUS_COUNTER_METRIC_WITH_LOCATION(
    internal_cache_evicted_bytes_total, "[cpp]cache evicted bytes")
DEFINE_PROMETHEUS_HISTOGRAM_METRIC_WITH_DATA_TYPE_AND_LOCATION(
    internal_cache_cell_lifetime_seconds,
    milvus::monitor::secondsBuckets,
    "[cpp]cache cell lifetime in seconds")

namespace {

const char*
CellDataTypeLabel(CellDataType t) {
    switch (t) {
        case CellDataType::VECTOR_FIELD:
            return "vector_field";
        case CellDataType::VECTOR_INDEX:
            return "vector_index";
        case CellDataType::SCALAR_FIELD:
            return "scalar_field";
        case CellDataType::SCALAR_INDEX:
            return "scalar_index";
        case CellDataType::OTHER:
            return "other";
    }
    return "unknown";
}

}  // namespace

prometheus::Gauge&
cache_shard_disk_usage_bytes(CellDataType t, const std::string& shard) {
    return internal_cache_shard_disk_usage_bytes_family.Add(
        {{"data_type", CellDataTypeLabel(t)}, {"shard", shard}});
}

#ifdef MILVUS_UNIT_TEST
double
GetCacheShardDiskUsageBytesForTest(const std::string& shard, CellDataType t) {
    return cache_shard_disk_usage_bytes(t, shard).Value();
}
#endif

}  // namespace milvus::cachinglayer::monitor
