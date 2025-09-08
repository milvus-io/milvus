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

#pragma once

#include <prometheus/collectable.h>
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#include <prometheus/summary.h>
#include <prometheus/text_serializer.h>

#include <memory>
#include <sstream>
#include <string>

#include "common/PrometheusClient.h"

namespace milvus::monitor {

DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_storage_kv_size);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_kv_size_get);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_kv_size_put);

DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_storage_request_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_get);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_put);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_stat);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_list);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_request_latency_remove);

DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_storage_op_count);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_get_suc);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_get_fail);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_put_suc);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_put_fail);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_stat_suc);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_stat_fail);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_list_suc);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_list_fail);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_remove_suc);
DECLARE_PROMETHEUS_COUNTER(internal_storage_op_count_remove_fail);

DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_storage_load_duration);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_download_duration);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_write_disk_duration);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_deserialize_duration);

// mmap metrics
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_mmap_allocated_space_bytes);
DECLARE_PROMETHEUS_HISTOGRAM(internal_mmap_allocated_space_bytes_anon);
DECLARE_PROMETHEUS_HISTOGRAM(internal_mmap_allocated_space_bytes_file);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_mmap_in_used_space_bytes);
DECLARE_PROMETHEUS_GAUGE(internal_mmap_in_used_space_bytes_anon);
DECLARE_PROMETHEUS_GAUGE(internal_mmap_in_used_space_bytes_file);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_mmap_in_used_count);
DECLARE_PROMETHEUS_GAUGE(internal_mmap_in_used_count_anon);
DECLARE_PROMETHEUS_GAUGE(internal_mmap_in_used_count_file);

// search metrics
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_core_search_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_scalar);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_vector);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_groupby);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_iterative_filter);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_rescore);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_scalar_proportion);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_get_vector_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_retrieve_get_target_entry_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_search_get_target_entry_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_search_latency_random_sample);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_optimize_expr_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_core_expr_filter_ratio);

// async cgo metrics
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cgo_queue_duration_seconds);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cgo_queue_duration_seconds_all);
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cgo_execute_duration_seconds);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cgo_execute_duration_seconds_all);
DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_cgo_cancel_before_execute_total)
DECLARE_PROMETHEUS_COUNTER(internal_cgo_cancel_before_execute_total_all);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_pool_size);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_pool_size_all);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_inflight_task_total);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_inflight_task_total_all);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_executing_task_total);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_executing_task_total_all);

// json stats metrics
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_json_stats_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_term_query);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_shredding);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_shared);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_load);

// --- file writer metrics ---

DECLARE_PROMETHEUS_COUNTER_FAMILY(disk_write_total_bytes);
DECLARE_PROMETHEUS_COUNTER(disk_write_total_bytes_buffered);
DECLARE_PROMETHEUS_COUNTER(disk_write_total_bytes_direct);

// --- file writer metrics end ---

}  // namespace milvus::monitor
