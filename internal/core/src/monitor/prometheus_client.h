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

namespace milvus::monitor {

class PrometheusClient {
 public:
    PrometheusClient() = default;
    PrometheusClient(const PrometheusClient&) = delete;
    PrometheusClient&
    operator=(const PrometheusClient&) = delete;

    prometheus::Registry&
    GetRegistry() {
        return *registry_;
    }

    std::string
    GetMetrics() {
        std::ostringstream ss;
        prometheus::TextSerializer serializer;
        serializer.Serialize(ss, registry_->Collect());
        return ss.str();
    }

 private:
    std::shared_ptr<prometheus::Registry> registry_ =
        std::make_shared<prometheus::Registry>();
};

/*****************************************************************************/
// prometheus metrics
extern const prometheus::Histogram::BucketBoundaries buckets;
extern const std::unique_ptr<PrometheusClient> prometheusClient;

#define DEFINE_PROMETHEUS_GAUGE_FAMILY(name, desc)                \
    prometheus::Family<prometheus::Gauge>& name##_family =        \
        prometheus::BuildGauge().Name(#name).Help(desc).Register( \
            milvus::monitor::prometheusClient->GetRegistry());
#define DEFINE_PROMETHEUS_GAUGE(alias, name, labels) \
    prometheus::Gauge& alias = name##_family.Add(labels);

#define DEFINE_PROMETHEUS_COUNTER_FAMILY(name, desc)                \
    prometheus::Family<prometheus::Counter>& name##_family =        \
        prometheus::BuildCounter().Name(#name).Help(desc).Register( \
            milvus::monitor::prometheusClient->GetRegistry());
#define DEFINE_PROMETHEUS_COUNTER(alias, name, labels) \
    prometheus::Counter& alias = name##_family.Add(labels);

#define DEFINE_PROMETHEUS_HISTOGRAM_FAMILY(name, desc)                \
    prometheus::Family<prometheus::Histogram>& name##_family =        \
        prometheus::BuildHistogram().Name(#name).Help(desc).Register( \
            milvus::monitor::prometheusClient->GetRegistry());
#define DEFINE_PROMETHEUS_HISTOGRAM(alias, name, labels) \
    prometheus::Histogram& alias =                       \
        name##_family.Add(labels, milvus::monitor::buckets);
#define DEFINE_PROMETHEUS_HISTOGRAM_WITH_BUCKETS(alias, name, labels, buckets) \
    prometheus::Histogram& alias = name##_family.Add(labels, buckets);

#define DECLARE_PROMETHEUS_GAUGE_FAMILY(name_gauge_family) \
    extern prometheus::Family<prometheus::Gauge>& name_gauge_family;
#define DECLARE_PROMETHEUS_GAUGE(name_gauge) \
    extern prometheus::Gauge& name_gauge;
#define DECLARE_PROMETHEUS_COUNTER_FAMILY(name_counter_family) \
    extern prometheus::Family<prometheus::Counter>& name_counter_family;
#define DECLARE_PROMETHEUS_COUNTER(name_counter) \
    extern prometheus::Counter& name_counter;
#define DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(name_histogram_family) \
    extern prometheus::Family<prometheus::Histogram>& name_histogram_family;
#define DECLARE_PROMETHEUS_HISTOGRAM(name_histogram) \
    extern prometheus::Histogram& name_histogram;

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

// --- caching layer metrics ---

DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cache_slot_count);
DECLARE_PROMETHEUS_GAUGE(internal_cache_slot_count_memory);
DECLARE_PROMETHEUS_GAUGE(internal_cache_slot_count_disk);
DECLARE_PROMETHEUS_GAUGE(internal_cache_slot_count_mixed);

DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cache_cell_count);
DECLARE_PROMETHEUS_GAUGE(internal_cache_cell_count_memory);
DECLARE_PROMETHEUS_GAUGE(internal_cache_cell_count_disk);
DECLARE_PROMETHEUS_GAUGE(internal_cache_cell_count_mixed);

DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cache_cell_loaded_count);
DECLARE_PROMETHEUS_GAUGE(internal_cache_cell_loaded_count_memory);
DECLARE_PROMETHEUS_GAUGE(internal_cache_cell_loaded_count_disk);
DECLARE_PROMETHEUS_GAUGE(internal_cache_cell_loaded_count_mixed);

DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cache_load_latency);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cache_load_latency_memory);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cache_load_latency_disk);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cache_load_latency_mixed);

DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_cache_op_result_count);
DECLARE_PROMETHEUS_COUNTER(internal_cache_op_result_count_hit_memory);
DECLARE_PROMETHEUS_COUNTER(internal_cache_op_result_count_hit_disk);
DECLARE_PROMETHEUS_COUNTER(internal_cache_op_result_count_hit_mixed);
DECLARE_PROMETHEUS_COUNTER(internal_cache_op_result_count_miss_memory);
DECLARE_PROMETHEUS_COUNTER(internal_cache_op_result_count_miss_disk);
DECLARE_PROMETHEUS_COUNTER(internal_cache_op_result_count_miss_mixed);

DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cache_used_bytes);
DECLARE_PROMETHEUS_GAUGE(internal_cache_used_bytes_memory);
DECLARE_PROMETHEUS_GAUGE(internal_cache_used_bytes_disk);

DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cache_capacity_bytes);
DECLARE_PROMETHEUS_GAUGE(internal_cache_capacity_bytes_memory);
DECLARE_PROMETHEUS_GAUGE(internal_cache_capacity_bytes_disk);

DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_cache_eviction_count);
DECLARE_PROMETHEUS_COUNTER(internal_cache_eviction_count_memory);
DECLARE_PROMETHEUS_COUNTER(internal_cache_eviction_count_disk);
DECLARE_PROMETHEUS_COUNTER(internal_cache_eviction_count_mixed);

DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_cache_evicted_bytes);
DECLARE_PROMETHEUS_COUNTER(internal_cache_evicted_bytes_memory);
DECLARE_PROMETHEUS_COUNTER(internal_cache_evicted_bytes_disk);

DECLARE_PROMETHEUS_HISTOGRAM_FAMILY(internal_cache_item_lifetime_seconds);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cache_item_lifetime_seconds_memory);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cache_item_lifetime_seconds_disk);
DECLARE_PROMETHEUS_HISTOGRAM(internal_cache_item_lifetime_seconds_mixed);

DECLARE_PROMETHEUS_COUNTER_FAMILY(internal_cache_load_count);
DECLARE_PROMETHEUS_COUNTER(internal_cache_load_count_success_memory);
DECLARE_PROMETHEUS_COUNTER(internal_cache_load_count_success_disk);
DECLARE_PROMETHEUS_COUNTER(internal_cache_load_count_success_mixed);
DECLARE_PROMETHEUS_COUNTER(internal_cache_load_count_fail_memory);
DECLARE_PROMETHEUS_COUNTER(internal_cache_load_count_fail_disk);
DECLARE_PROMETHEUS_COUNTER(internal_cache_load_count_fail_mixed);

// TODO(tiered storage 1): not added
DECLARE_PROMETHEUS_GAUGE_FAMILY(internal_cache_memory_overhead_bytes);
DECLARE_PROMETHEUS_GAUGE(internal_cache_memory_overhead_bytes_memory);
DECLARE_PROMETHEUS_GAUGE(internal_cache_memory_overhead_bytes_disk);
DECLARE_PROMETHEUS_GAUGE(internal_cache_memory_overhead_bytes_mixed);

// --- caching layer metrics end ---

}  // namespace milvus::monitor
