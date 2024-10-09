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

}  // namespace milvus::monitor
