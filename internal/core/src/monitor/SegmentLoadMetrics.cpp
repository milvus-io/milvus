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

#include "monitor/SegmentLoadMetrics.h"

#include <exception>
#include <prometheus/histogram.h>

#include "common/PrometheusClient.h"

namespace milvus::monitor {
namespace {
constexpr std::array phase_names = {"lock_wait",
                                    "prepare",
                                    "clone_state",
                                    "indexes",
                                    "reload_columns",
                                    "column_groups",
                                    "text_lob",
                                    "field_data",
                                    "text_indexes",
                                    "json_stats",
                                    "default_fields",
                                    "create_text_indexes",
                                    "finalize",
                                    "publish",
                                    "total"};
static_assert(phase_names.size() ==
              static_cast<std::size_t>(SegmentLoadPhase::Count) + 1);

const auto&
Metrics() {
    static const auto metrics = [] {
        auto& family =
            prometheus::BuildHistogram()
                .Name("internal_core_segment_load_duration_seconds")
                .Help(
                    "Completed Load attempts: disjoint phase wall times "
                    "and total, with zero for skipped phases.")
                .Register(getPrometheusClient().GetRegistry());
        const prometheus::Histogram::BucketBoundaries buckets = {
            0.000001, 0.00001, 0.0001, 0.0005, 0.001, 0.0025, 0.005,
            0.01,     0.025,   0.05,   0.1,    0.25,  0.5,    1,
            2.5,      5,       10,     30,     120,   600};
        std::array<std::array<prometheus::Histogram*, 2>, phase_names.size()>
            result{};
        for (std::size_t i = 0; i < phase_names.size(); ++i) {
            result[i] = {
                &family.Add({{"stage", phase_names[i]}, {"result", "success"}},
                            buckets),
                &family.Add({{"stage", phase_names[i]}, {"result", "error"}},
                            buckets)};
        }
        return result;
    }();
    return metrics;
}
}  // namespace

SegmentLoadTiming::SegmentLoadTiming()
    : started_(Clock::now()),
      phase_started_(started_),
      uncaught_exceptions_(std::uncaught_exceptions()) {
}

SegmentLoadTiming::~SegmentLoadTiming() {
    End(std::uncaught_exceptions() > uncaught_exceptions_);
}

void
SegmentLoadTiming::SwitchTo(SegmentLoadTiming* timing, SegmentLoadPhase phase) {
    if (timing == nullptr || !timing->active_) {
        return;
    }
    const auto now = Clock::now();
    timing->durations_[static_cast<std::size_t>(timing->phase_)] +=
        now - timing->phase_started_;
    timing->phase_ = phase;
    timing->phase_started_ = now;
}

void
SegmentLoadTiming::End(bool failed) {
    if (!active_) {
        return;
    }
    const auto now = Clock::now();
    durations_[static_cast<std::size_t>(phase_)] += now - phase_started_;
    const auto& metrics = Metrics();
    for (std::size_t i = 0; i < durations_.size(); ++i) {
        metrics[i][failed]->Observe(
            std::chrono::duration<double>(durations_[i]).count());
    }
    metrics.back()[failed]->Observe(
        std::chrono::duration<double>(now - started_).count());
    active_ = false;
}
}  // namespace milvus::monitor
