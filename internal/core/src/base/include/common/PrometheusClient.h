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
static const std::unique_ptr<PrometheusClient> prometheusClient =
    std::make_unique<PrometheusClient>();
extern const prometheus::Histogram::BucketBoundaries buckets;
extern const prometheus::Histogram::BucketBoundaries secondsBuckets;
extern const prometheus::Histogram::BucketBoundaries bytesBuckets;
extern const prometheus::Histogram::BucketBoundaries ratioBuckets;

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

}  // namespace milvus::monitor
