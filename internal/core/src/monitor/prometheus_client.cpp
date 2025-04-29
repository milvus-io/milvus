// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "prometheus_client.h"
#include <chrono>

namespace milvus::monitor {

const prometheus::Histogram::BucketBoundaries secondsBuckets = {
    std::chrono::duration<float>(std::chrono::microseconds(10)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(50)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(100)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(250)).count(),
    std::chrono::duration<float>(std::chrono::microseconds(500)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(1)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(5)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(10)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(20)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(50)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(100)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(200)).count(),
    std::chrono::duration<float>(std::chrono::milliseconds(500)).count(),
    std::chrono::duration<float>(std::chrono::seconds(1)).count(),
    std::chrono::duration<float>(std::chrono::seconds(2)).count(),
    std::chrono::duration<float>(std::chrono::seconds(5)).count(),
    std::chrono::duration<float>(std::chrono::seconds(10)).count(),
};

const prometheus::Histogram::BucketBoundaries buckets = {1,
                                                         2,
                                                         4,
                                                         8,
                                                         16,
                                                         32,
                                                         64,
                                                         128,
                                                         256,
                                                         512,
                                                         1024,
                                                         2048,
                                                         4096,
                                                         8192,
                                                         16384,
                                                         32768,
                                                         65536};

const prometheus::Histogram::BucketBoundaries bytesBuckets = {
    1024,         // 1k
    8192,         // 8k
    65536,        // 64k
    262144,       // 256k
    524288,       // 512k
    1048576,      // 1M
    4194304,      // 4M
    8388608,      // 8M
    16777216,     // 16M
    67108864,     // 64M
    134217728,    // 128M
    268435456,    // 256M
    536870912,    // 512M
    1073741824};  // 1G

const prometheus::Histogram::BucketBoundaries ratioBuckets = {
    0.0,  0.05, 0.1,  0.15, 0.2,  0.25, 0.3,  0.35, 0.4,  0.45, 0.5,
    0.55, 0.6,  0.65, 0.7,  0.75, 0.8,  0.85, 0.9,  0.95, 1.0};

const std::unique_ptr<PrometheusClient> prometheusClient =
    std::make_unique<PrometheusClient>();

/******************GetMetrics*************************************************************
 * !!! NOT use SUMMARY metrics here, because when parse SUMMARY metrics in Milvus,
 *     see following error:
 *
 *   An error has occurred while serving metrics:
 *   text format parsing error in line 50: expected float as value, got "=\"0.9\"}"
 ******************************************************************************/

std::map<std::string, std::string> getMap = {
    {"persistent_data_op_type", "get"}};
std::map<std::string, std::string> getSucMap = {
    {"persistent_data_op_type", "get"}, {"status", "success"}};
std::map<std::string, std::string> getFailMap = {
    {"persistent_data_op_type", "get"}};
std::map<std::string, std::string> putMap = {
    {"persistent_data_op_type", "put"}};
std::map<std::string, std::string> putSucMap = {
    {"persistent_data_op_type", "put"}, {"status", "success"}};
std::map<std::string, std::string> putFailMap = {
    {"persistent_data_op_type", "put"}, {"status", "fail"}};
std::map<std::string, std::string> statMap = {
    {"persistent_data_op_type", "stat"}};
std::map<std::string, std::string> statSucMap = {
    {"persistent_data_op_type", "stat"}, {"status", "success"}};
std::map<std::string, std::string> statFailMap = {
    {"persistent_data_op_type", "stat"}, {"status", "fail"}};
std::map<std::string, std::string> listMap = {
    {"persistent_data_op_type", "list"}};
std::map<std::string, std::string> listSucMap = {
    {"persistent_data_op_type", "list"}, {"status", "success"}};
std::map<std::string, std::string> listFailMap = {
    {"persistent_data_op_type", "list"}, {"status", "fail"}};
std::map<std::string, std::string> removeMap = {
    {"persistent_data_op_type", "remove"}};
std::map<std::string, std::string> removeSucMap = {
    {"persistent_data_op_type", "remove"}, {"status", "success"}};
std::map<std::string, std::string> removeFailMap = {
    {"persistent_data_op_type", "remove"}, {"status", "fail"}};

DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_storage_kv_size,
                                   "[cpp]kv size stats")
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_kv_size_get,
                            internal_storage_kv_size,
                            getMap)
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_kv_size_put,
                            internal_storage_kv_size,
                            putMap)
DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(
    internal_storage_request_latency,
    "[cpp]request latency(ms) on the client side")
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_get,
                            internal_storage_request_latency,
                            getMap)
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_put,
                            internal_storage_request_latency,
                            putMap)
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_stat,
                            internal_storage_request_latency,
                            statMap)
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_list,
                            internal_storage_request_latency,
                            listMap)
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_remove,
                            internal_storage_request_latency,
                            removeMap)
DEFINE_PROMETHEUS_COUNTER_FAMILY(internal_storage_op_count,
                                 "[cpp]count of persistent data operation")
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_get_suc,
                          internal_storage_op_count,
                          getSucMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_get_fail,
                          internal_storage_op_count,
                          getFailMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_put_suc,
                          internal_storage_op_count,
                          putSucMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_put_fail,
                          internal_storage_op_count,
                          putFailMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_stat_suc,
                          internal_storage_op_count,
                          statSucMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_stat_fail,
                          internal_storage_op_count,
                          statFailMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_list_suc,
                          internal_storage_op_count,
                          listSucMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_list_fail,
                          internal_storage_op_count,
                          listFailMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_remove_suc,
                          internal_storage_op_count,
                          removeSucMap)
DEFINE_PROMETHEUS_COUNTER(internal_storage_op_count_remove_fail,
                          internal_storage_op_count,
                          removeFailMap)

//load metrics
std::map<std::string, std::string> downloadDurationLabels{{"type", "download"}};
std::map<std::string, std::string> writeDiskDurationLabels{
    {"type", "write_disk"}};
std::map<std::string, std::string> deserializeDurationLabels{
    {"type", "deserialize"}};
DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_storage_load_duration,
                                   "[cpp]durations of load segment")
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_download_duration,
                            internal_storage_load_duration,
                            downloadDurationLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_write_disk_duration,
                            internal_storage_load_duration,
                            writeDiskDurationLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_storage_deserialize_duration,
                            internal_storage_load_duration,
                            deserializeDurationLabels)

// search latency metrics
std::map<std::string, std::string> scalarLatencyLabels{
    {"type", "scalar_latency"}};
std::map<std::string, std::string> vectorLatencyLabels{
    {"type", "vector_latency"}};
std::map<std::string, std::string> groupbyLatencyLabels{
    {"type", "groupby_latency"}};
std::map<std::string, std::string> iterativeFilterLatencyLabels{
    {"type", "iterative_filter_latency"}};
std::map<std::string, std::string> scalarProportionLabels{
    {"type", "scalar_proportion"}};
std::map<std::string, std::string> getVectorLatencyLabels{
    {"type", "get_vector_latency"}};
std::map<std::string, std::string> retrieveGetTargetEntryLatencyLabels{
    {"type", "retrieve_get_target_entry_latency"}};
std::map<std::string, std::string> searchGetTargetEntryLatencyLabels{
    {"type", "search_get_target_entry_latency"}};
std::map<std::string, std::string> randomSampleLatencyLabels{
    {"type", "random_sample_latency"}};
std::map<std::string, std::string> optimizeExprLatencyLabels{
    {"type", "optimize_expr_latency"}};
std::map<std::string, std::string> filterRatioLabels{
    {"type", "expr_filter_ratio"}};

DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_core_search_latency,
                                   "[cpp]latency(us) of search on segment")
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_scalar,
                            internal_core_search_latency,
                            scalarLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_vector,
                            internal_core_search_latency,
                            vectorLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_groupby,
                            internal_core_search_latency,
                            groupbyLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_iterative_filter,
                            internal_core_search_latency,
                            iterativeFilterLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_core_search_latency_scalar_proportion,
    internal_core_search_latency,
    scalarProportionLabels,
    ratioBuckets)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_get_vector_latency,
                            internal_core_search_latency,
                            getVectorLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_retrieve_get_target_entry_latency,
                            internal_core_search_latency,
                            retrieveGetTargetEntryLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_get_target_entry_latency,
                            internal_core_search_latency,
                            searchGetTargetEntryLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_random_sample,
                            internal_core_search_latency,
                            randomSampleLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_optimize_expr_latency,
                            internal_core_search_latency,
                            optimizeExprLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(internal_core_expr_filter_ratio,
                                         internal_core_search_latency,
                                         filterRatioLabels,
                                         ratioBuckets)
// mmap metrics
std::map<std::string, std::string> mmapAllocatedSpaceAnonLabel = {
    {"type", "anon"}};
std::map<std::string, std::string> mmapAllocatedSpaceFileLabel = {
    {"type", "file"}};
std::map<std::string, std::string> mmapAllocatedCountAnonLabel = {
    {"type", "anon"}};
std::map<std::string, std::string> mmapAllocatedCountFileLabel = {
    {"type", "file"}};

DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_mmap_allocated_space_bytes,
                                   "[cpp]mmap allocated space stats")
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_mmap_allocated_space_bytes_anon,
    internal_mmap_allocated_space_bytes,
    mmapAllocatedSpaceAnonLabel,
    bytesBuckets)
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_mmap_allocated_space_bytes_file,
    internal_mmap_allocated_space_bytes,
    mmapAllocatedSpaceFileLabel,
    bytesBuckets)

DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_mmap_in_used_space_bytes,
                               "[cpp]mmap in used space stats")
DEFINE_PROMETHEUS_GAUGE(internal_mmap_in_used_space_bytes_anon,
                        internal_mmap_in_used_space_bytes,
                        mmapAllocatedSpaceAnonLabel)
DEFINE_PROMETHEUS_GAUGE(internal_mmap_in_used_space_bytes_file,
                        internal_mmap_in_used_space_bytes,
                        mmapAllocatedSpaceFileLabel)
DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_mmap_in_used_count,
                               "[cpp]mmap in used count stats")
DEFINE_PROMETHEUS_GAUGE(internal_mmap_in_used_count_anon,
                        internal_mmap_in_used_count,
                        mmapAllocatedCountAnonLabel)
DEFINE_PROMETHEUS_GAUGE(internal_mmap_in_used_count_file,
                        internal_mmap_in_used_count,
                        mmapAllocatedCountFileLabel)

// async cgo metrics
DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cgo_queue_duration_seconds,
                                   "[cpp]async cgo queue duration");
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_cgo_queue_duration_seconds_all,
    internal_cgo_queue_duration_seconds,
    {},
    secondsBuckets);

DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cgo_execute_duration_seconds,
                                   "[cpp]async execute duration");
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_cgo_execute_duration_seconds_all,
    internal_cgo_execute_duration_seconds,
    {},
    secondsBuckets);

DEFINE_PROMETHEUS_COUNTER_FAMILY(internal_cgo_cancel_before_execute_total,
                                 "[cpp]async cgo cancel before execute count");
DEFINE_PROMETHEUS_COUNTER(internal_cgo_cancel_before_execute_total_all,
                          internal_cgo_cancel_before_execute_total,
                          {});

DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_pool_size,
                               "[cpp]async cgo pool size");
DEFINE_PROMETHEUS_GAUGE(internal_cgo_pool_size_all, internal_cgo_pool_size, {});

DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_inflight_task_total,
                               "[cpp]async cgo inflight task");
DEFINE_PROMETHEUS_GAUGE(internal_cgo_inflight_task_total_all,
                        internal_cgo_inflight_task_total,
                        {});

DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_executing_task_total,
                               "[cpp]async cgo executing task");
DEFINE_PROMETHEUS_GAUGE(internal_cgo_executing_task_total_all,
                        internal_cgo_executing_task_total,
                        {});

// --- caching layer metrics ---
// TODO(tiered storage 1): choose better buckets.

std::map<std::string, std::string> cacheMemoryLabel = {{"location", "memory"}};
std::map<std::string, std::string> cacheDiskLabel = {{"location", "disk"}};
std::map<std::string, std::string> cacheMixedLabel = {{"location", "mixed"}};

// Cache slot count
DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cache_slot_count,
                               "[cpp]cache slot count");
DEFINE_PROMETHEUS_GAUGE(internal_cache_slot_count_memory,
                        internal_cache_slot_count,
                        cacheMemoryLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_slot_count_disk,
                        internal_cache_slot_count,
                        cacheDiskLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_slot_count_mixed,
                        internal_cache_slot_count,
                        cacheMixedLabel);

// Cache cell count
DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cache_cell_count,
                               "[cpp]cache cell count");
DEFINE_PROMETHEUS_GAUGE(internal_cache_cell_count_memory,
                        internal_cache_cell_count,
                        cacheMemoryLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_cell_count_disk,
                        internal_cache_cell_count,
                        cacheDiskLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_cell_count_mixed,
                        internal_cache_cell_count,
                        cacheMixedLabel);

// Cache cell loaded count
DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cache_cell_loaded_count,
                               "[cpp]cache cell loaded count");
DEFINE_PROMETHEUS_GAUGE(internal_cache_cell_loaded_count_memory,
                        internal_cache_cell_loaded_count,
                        cacheMemoryLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_cell_loaded_count_disk,
                        internal_cache_cell_loaded_count,
                        cacheDiskLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_cell_loaded_count_mixed,
                        internal_cache_cell_loaded_count,
                        cacheMixedLabel);

// Cache load latency histogram
DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cache_load_latency,
                                   "[cpp]cache load latency histogram");
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(internal_cache_load_latency_memory,
                                         internal_cache_load_latency,
                                         cacheMemoryLabel,
                                         secondsBuckets);
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(internal_cache_load_latency_disk,
                                         internal_cache_load_latency,
                                         cacheDiskLabel,
                                         secondsBuckets);
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(internal_cache_load_latency_mixed,
                                         internal_cache_load_latency,
                                         cacheMixedLabel,
                                         secondsBuckets);

// Cache hit rate (represented by hit/miss counters)
std::map<std::string, std::string> cacheHitMemoryLabels = {
    {"result", "hit"}, {"location", "memory"}};
std::map<std::string, std::string> cacheHitDiskLabels = {{"result", "hit"},
                                                         {"location", "disk"}};
std::map<std::string, std::string> cacheHitMixedLabels = {
    {"result", "hit"}, {"location", "mixed"}};
std::map<std::string, std::string> cacheMissMemoryLabels = {
    {"result", "miss"}, {"location", "memory"}};
std::map<std::string, std::string> cacheMissDiskLabels = {{"result", "miss"},
                                                          {"location", "disk"}};
std::map<std::string, std::string> cacheMissMixedLabels = {
    {"result", "miss"}, {"location", "mixed"}};
DEFINE_PROMETHEUS_COUNTER_FAMILY(internal_cache_op_result_count,
                                 "[cpp]cache operation result count");
DEFINE_PROMETHEUS_COUNTER(internal_cache_op_result_count_hit_memory,
                          internal_cache_op_result_count,
                          cacheHitMemoryLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_op_result_count_hit_disk,
                          internal_cache_op_result_count,
                          cacheHitDiskLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_op_result_count_hit_mixed,
                          internal_cache_op_result_count,
                          cacheHitMixedLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_op_result_count_miss_memory,
                          internal_cache_op_result_count,
                          cacheMissMemoryLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_op_result_count_miss_disk,
                          internal_cache_op_result_count,
                          cacheMissDiskLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_op_result_count_miss_mixed,
                          internal_cache_op_result_count,
                          cacheMissMixedLabels);

// Cache usage (bytes)
DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cache_used_bytes,
                               "[cpp]currently used bytes in cache");
DEFINE_PROMETHEUS_GAUGE(internal_cache_used_bytes_memory,
                        internal_cache_used_bytes,
                        cacheMemoryLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_used_bytes_disk,
                        internal_cache_used_bytes,
                        cacheDiskLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_used_bytes_mixed,
                        internal_cache_used_bytes,
                        cacheMixedLabel);

DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cache_capacity_bytes,
                               "[cpp]total capacity bytes of cache");
DEFINE_PROMETHEUS_GAUGE(internal_cache_capacity_bytes_memory,
                        internal_cache_capacity_bytes,
                        cacheMemoryLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_capacity_bytes_disk,
                        internal_cache_capacity_bytes,
                        cacheDiskLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_capacity_bytes_mixed,
                        internal_cache_capacity_bytes,
                        cacheMixedLabel);

// Eviction count and resource size
DEFINE_PROMETHEUS_COUNTER_FAMILY(internal_cache_eviction_count,
                                 "[cpp]cache eviction count");
DEFINE_PROMETHEUS_COUNTER(internal_cache_eviction_count_memory,
                          internal_cache_eviction_count,
                          cacheMemoryLabel);
DEFINE_PROMETHEUS_COUNTER(internal_cache_eviction_count_disk,
                          internal_cache_eviction_count,
                          cacheDiskLabel);
DEFINE_PROMETHEUS_COUNTER(internal_cache_eviction_count_mixed,
                          internal_cache_eviction_count,
                          cacheMixedLabel);

DEFINE_PROMETHEUS_COUNTER_FAMILY(internal_cache_evicted_bytes,
                                 "[cpp]total bytes evicted from cache");
DEFINE_PROMETHEUS_COUNTER(internal_cache_evicted_bytes_memory,
                          internal_cache_evicted_bytes,
                          cacheMemoryLabel);
DEFINE_PROMETHEUS_COUNTER(internal_cache_evicted_bytes_disk,
                          internal_cache_evicted_bytes,
                          cacheDiskLabel);
DEFINE_PROMETHEUS_COUNTER(internal_cache_evicted_bytes_mixed,
                          internal_cache_evicted_bytes,
                          cacheMixedLabel);

// Cache item lifetime histogram
DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cache_item_lifetime_seconds,
                                   "[cpp]cache item lifetime histogram");
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_cache_item_lifetime_seconds_memory,
    internal_cache_item_lifetime_seconds,
    cacheMemoryLabel,
    secondsBuckets);
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_cache_item_lifetime_seconds_disk,
    internal_cache_item_lifetime_seconds,
    cacheDiskLabel,
    secondsBuckets);
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_cache_item_lifetime_seconds_mixed,
    internal_cache_item_lifetime_seconds,
    cacheMixedLabel,
    secondsBuckets);

// Load error rate (represented by success/fail counters)
std::map<std::string, std::string> cacheLoadSuccessMemoryLabels = {
    {"status", "success"}, {"location", "memory"}};
std::map<std::string, std::string> cacheLoadSuccessDiskLabels = {
    {"status", "success"}, {"location", "disk"}};
std::map<std::string, std::string> cacheLoadSuccessMixedLabels = {
    {"status", "success"}, {"location", "mixed"}};
std::map<std::string, std::string> cacheLoadFailMemoryLabels = {
    {"status", "fail"}, {"location", "memory"}};
std::map<std::string, std::string> cacheLoadFailDiskLabels = {
    {"status", "fail"}, {"location", "disk"}};
std::map<std::string, std::string> cacheLoadFailMixedLabels = {
    {"status", "fail"}, {"location", "mixed"}};
DEFINE_PROMETHEUS_COUNTER_FAMILY(internal_cache_load_count,
                                 "[cpp]cache load operation count");
DEFINE_PROMETHEUS_COUNTER(internal_cache_load_count_success_memory,
                          internal_cache_load_count,
                          cacheLoadSuccessMemoryLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_load_count_success_disk,
                          internal_cache_load_count,
                          cacheLoadSuccessDiskLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_load_count_success_mixed,
                          internal_cache_load_count,
                          cacheLoadSuccessMixedLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_load_count_fail_memory,
                          internal_cache_load_count,
                          cacheLoadFailMemoryLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_load_count_fail_disk,
                          internal_cache_load_count,
                          cacheLoadFailDiskLabels);
DEFINE_PROMETHEUS_COUNTER(internal_cache_load_count_fail_mixed,
                          internal_cache_load_count,
                          cacheLoadFailMixedLabels);

// Cache system memory overhead (bytes)
DEFINE_PROMETHEUS_GAUGE_FAMILY(internal_cache_memory_overhead_bytes,
                               "[cpp]cache system memory overhead in bytes");
DEFINE_PROMETHEUS_GAUGE(internal_cache_memory_overhead_bytes_memory,
                        internal_cache_memory_overhead_bytes,
                        cacheMemoryLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_memory_overhead_bytes_disk,
                        internal_cache_memory_overhead_bytes,
                        cacheDiskLabel);
DEFINE_PROMETHEUS_GAUGE(internal_cache_memory_overhead_bytes_mixed,
                        internal_cache_memory_overhead_bytes,
                        cacheMixedLabel);

// --- caching layer metrics end ---

}  // namespace milvus::monitor
