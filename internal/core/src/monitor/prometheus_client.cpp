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

namespace milvus::monitor {

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
std::map<std::string, std::string> scalarProportionLabels{
    {"type", "scalar_proportion"}};
DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(internal_core_search_latency,
                                   "[cpp]latency(us) of search on segment")
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_scalar,
                            internal_core_search_latency,
                            scalarLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_vector,
                            internal_core_search_latency,
                            vectorLatencyLabels)
DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(
    internal_core_search_latency_scalar_proportion,
    internal_core_search_latency,
    scalarProportionLabels,
    ratioBuckets)

// mmap metrics
std::map<std::string, std::string> mmapAllocatedSpaceAnonLabel = {
    {"type", "anon"}};
std::map<std::string, std::string> mmapAllocatedSpaceFileLabel = {
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
}  // namespace milvus::monitor
