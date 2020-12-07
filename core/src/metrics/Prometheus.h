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

#include <chrono>
#include <functional>
#include <memory>
#include <ostream>
#include <sstream>
#include <string>
#include <vector>

#include <prometheus/collectable.h>
#include <prometheus/exposer.h>
#include <prometheus/registry.h>
#include <prometheus/text_serializer.h>

using Quantiles = std::vector<prometheus::detail::CKMSQuantiles::Quantile>;

#define PROMETHEUS_GAUGE(name_family, name_gauge, name, description)                           \
    prometheus::Family<prometheus::Gauge>& name_family =                                       \
        prometheus::BuildGauge().Name(name).Help(description).Register(prometheus.registry()); \
    prometheus::Gauge& name_gauge = name_family.Add({});

#define PROMETHEUS_COUNT(name_family, name_count, name, description)                             \
    prometheus::Family<prometheus::Counter>& name_family =                                       \
        prometheus::BuildCounter().Name(name).Help(description).Register(prometheus.registry()); \
    prometheus::Counter& rpc_requests_total_counter_ = rpc_requests_total_.Add({});

#define PROMETHEUS_SUMMARY(name_family, name_summary, name, description)                         \
    prometheus::Family<prometheus::Summary>& name_family =                                       \
        prometheus::BuildSummary().Name(name).Help(description).Register(prometheus.registry()); \
    prometheus::Summary& name_summary = name_family.Add({}, Quantiles{{0.95, 0.00}, {0.9, 0.05}, {0.8, 0.1}});

#define PROMETHEUS_HISTOGRAM(name_family, name, description) \
    prometheus::Family<prometheus::Histogram>& name_family = \
        prometheus::BuildHistogram().Name(name).Help(description).Register(prometheus.registry());

namespace milvus {

class ScopedTimer {
 public:
    explicit ScopedTimer(std::function<void(double)> callback) : callback_(callback) {
        start_ = std::chrono::system_clock::now();
    }

    ~ScopedTimer() {
        auto end = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start_).count();
        callback_(duration);
    }

 private:
    std::function<void(double)> callback_;
    std::chrono::time_point<std::chrono::system_clock> start_;
};

class Prometheus {
 public:
    prometheus::Registry&
    registry() {
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
    std::vector<std::weak_ptr<prometheus::Collectable>> collectables_;
    std::shared_ptr<prometheus::Registry> registry_ = std::make_shared<prometheus::Registry>();
};

extern Prometheus prometheus;

}  // namespace milvus
