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

#include <cstdint>

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
// skip index effectiveness: how many chunks the skip index was consulted for
// vs how many it pruned, plus the prune ratio distribution.
//
// Both carry database and collection labels so a shared query node can be read
// per tenant -- the collection name alone is not unique, two databases may each
// hold a "documents" and their series would merge. Reading them together:
// a node-wide prune rate averages a collection whose filter field is clustered
// together with one whose is not, and says nothing about either. The name comes
// from Schema::collection_name() and is empty for schemas not parsed from a
// proto (test-only), which lands in a "" series rather than being dropped.
DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_core_skipindex_chunks);
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_core_skipindex_prune_ratio);

prometheus::Counter&
internal_core_skipindex_chunks_scanned(const std::string& db,
                                       const std::string& collection);
prometheus::Counter&
internal_core_skipindex_chunks_pruned(const std::string& db,
                                      const std::string& collection);
prometheus::Histogram&
internal_core_skipindex_prune_ratio_expr(const std::string& db,
                                         const std::string& collection);

// Per-segment-operation storage traffic: total = every cell touched, cold =
// the cells that actually had to be loaded (i.e. real IO). Reading both is what
// tells you whether pruning translated into IO saved, or only into skipped CPU.
//
// Labelled by collection and by operation class ("search", "query", "count",
// or "agg"), because they have different traffic shapes -- a search always
// reads the vector column while a filtered query, count, or aggregate may read
// nothing at all -- and mixing them into one histogram makes none readable.
// Samples are physical segment operations, not Proxy request samples;
// aggregate sums are comparable after grouping by db_name/collection_name, but
// their histogram distributions have different cardinality.
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_core_query_scanned_bytes);

prometheus::Histogram&
internal_core_query_scanned_bytes_total(const std::string& db,
                                        const std::string& collection,
                                        const std::string& op);
prometheus::Histogram&
internal_core_query_scanned_bytes_cold(const std::string& db,
                                       const std::string& collection,
                                       const std::string& op);

// Observe one completed segment operation after every storage-reading phase
// (including late materialization) has contributed to the final cost.
void
observe_core_query_scanned_bytes(const std::string& db_name,
                                 const std::string& collection_name,
                                 const std::string& op,
                                 int64_t scanned_total_bytes,
                                 int64_t scanned_cold_bytes);

// Dynamic series are scoped to a loaded collection. Remove them when the
// collection's final reference is released so a query node that loads many
// different collections does not retain stale label sets forever.
void
cleanup_core_collection_metrics(const std::string& db_name,
                                const std::string& collection_name);

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
DECLARE_PROMETHEUS_HISTOGRAM(internal_cgo_queue_duration_seconds_search);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cgo_queue_duration_seconds_load);
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cgo_execute_duration_seconds);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cgo_execute_duration_seconds_search);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cgo_execute_duration_seconds_load);
DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_cgo_cancel_before_execute_total)
DECLARE_PROMETHEUS_COUNTER(internal_cgo_cancel_before_execute_total_search);
DECLARE_PROMETHEUS_COUNTER(internal_cgo_cancel_before_execute_total_load);
DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_cgo_cancel_during_execute_total);
DECLARE_PROMETHEUS_COUNTER(internal_cgo_cancel_during_execute_total_search);
DECLARE_PROMETHEUS_COUNTER(internal_cgo_cancel_during_execute_total_load);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_pool_size);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_pool_size_search);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_pool_size_load);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_inflight_task_total);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_inflight_task_total_search);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_inflight_task_total_load);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cgo_executing_task_total);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_executing_task_total_search);
DECLARE_PROMETHEUS_GAUGE(internal_cgo_executing_task_total_load);

// storage thread pool metrics
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_storage_pool_capacity);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_capacity_high);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_capacity_middle);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_capacity_low);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_storage_pool_active_threads);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_active_threads_high);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_active_threads_middle);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_active_threads_low);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_storage_pool_idle_threads);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_idle_threads_high);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_idle_threads_middle);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_idle_threads_low);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_storage_pool_queue_depth);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_queue_depth_high);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_queue_depth_middle);
DECLARE_PROMETHEUS_GAUGE(internal_storage_pool_queue_depth_low);
DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_storage_pool_task_submitted_total);
DECLARE_PROMETHEUS_COUNTER(internal_storage_pool_task_submitted_total_high);
DECLARE_PROMETHEUS_COUNTER(internal_storage_pool_task_submitted_total_middle);
DECLARE_PROMETHEUS_COUNTER(internal_storage_pool_task_submitted_total_low);
DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_storage_pool_task_completed_total);
DECLARE_PROMETHEUS_COUNTER(internal_storage_pool_task_completed_total_high);
DECLARE_PROMETHEUS_COUNTER(internal_storage_pool_task_completed_total_middle);
DECLARE_PROMETHEUS_COUNTER(internal_storage_pool_task_completed_total_low);
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(
    internal_storage_pool_queue_duration_seconds);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_pool_queue_duration_seconds_high);
DECLARE_PROMETHEUS_HISTOGRAM(
    internal_storage_pool_queue_duration_seconds_middle);
DECLARE_PROMETHEUS_HISTOGRAM(internal_storage_pool_queue_duration_seconds_low);
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(
    internal_storage_pool_execute_duration_seconds);
DECLARE_PROMETHEUS_HISTOGRAM(
    internal_storage_pool_execute_duration_seconds_high);
DECLARE_PROMETHEUS_HISTOGRAM(
    internal_storage_pool_execute_duration_seconds_middle);
DECLARE_PROMETHEUS_HISTOGRAM(
    internal_storage_pool_execute_duration_seconds_low);

// arrow io thread pool metrics
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_arrow_io_pool_capacity);
DECLARE_PROMETHEUS_GAUGE(internal_arrow_io_pool_capacity_all);
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_arrow_io_pool_tasks_total);
DECLARE_PROMETHEUS_GAUGE(internal_arrow_io_pool_tasks_total_all);

// json stats metrics
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_json_stats_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_term_query);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_shredding);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_shared);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_stats_latency_load);

// json filter performance metrics
DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_json_filter_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_filter_latency_bruteforce);
DECLARE_PROMETHEUS_HISTOGRAM(internal_json_filter_latency_json_stats);

}  // namespace milvus::monitor
