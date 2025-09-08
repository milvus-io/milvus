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

#include <chrono>
#include "monitor/Monitor.h"

namespace milvus::monitor {

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

// json stats metrics
std::map<std::string, std::string> invertedIndexLatencyLabels{
    {"type", "inverted_index_latency"}};
std::map<std::string, std::string> shreddingLatencyLabels{
    {"type", "shredding_latency"}};
std::map<std::string, std::string> sharedLatencyLabels{
    {"type", "shared_latency"}};
std::map<std::string, std::string> loadLatencyLabels{{"type", "load_latency"}};

DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_json_stats_latency,
                                   "[cpp]durations of json stats")
DEFINE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_term_query,
                            internal_json_stats_latency,
                            invertedIndexLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_shredding,
                            internal_json_stats_latency,
                            shreddingLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_shared,
                            internal_json_stats_latency,
                            sharedLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_load,
                            internal_json_stats_latency,
                            loadLatencyLabels)

// search latency metrics
std::map<std::string, std::string> scalarLatencyLabels{
    {"type", "scalar_latency"}};
std::map<std::string, std::string> vectorLatencyLabels{
    {"type", "vector_latency"}};
std::map<std::string, std::string> groupbyLatencyLabels{
    {"type", "groupby_latency"}};
std::map<std::string, std::string> iterativeFilterLatencyLabels{
    {"type", "iterative_filter_latency"}};
std::map<std::string, std::string> rescoreLatencyLabels{
    {"type", "rescore_latency"}};
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
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_rescore,
                            internal_core_search_latency,
                            rescoreLatencyLabels)
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

// --- file writer metrics ---

std::map<std::string, std::string> diskWriteModeBufferedLabel = {
    {"mode", "buffered"}};
std::map<std::string, std::string> diskWriteModeDirectLabel = {
    {"mode", "direct"}};

DEFINE_PROMETHEUS_COUNTER_FAMILY(disk_write_total_bytes,
                                 "[cpp]disk write total bytes");
DEFINE_PROMETHEUS_COUNTER(disk_write_total_bytes_buffered,
                          disk_write_total_bytes,
                          diskWriteModeBufferedLabel);
DEFINE_PROMETHEUS_COUNTER(disk_write_total_bytes_direct,
                          disk_write_total_bytes,
                          diskWriteModeDirectLabel);

// --- file writer metrics end ---

}  // namespace milvus::monitor
