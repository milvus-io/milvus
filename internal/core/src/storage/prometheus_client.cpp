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

#include "storage/prometheus_client.h"

namespace milvus::storage {

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
}  // namespace milvus::storage
