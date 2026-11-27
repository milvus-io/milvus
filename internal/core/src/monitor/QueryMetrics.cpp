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

#include "monitor/QueryMetrics.h"

#include <array>
#include <exception>

#include "common/PrometheusClient.h"

namespace milvus::monitor {
namespace {

constexpr std::array stage_names = {
    "manifest_reader_open",     "manifest_translator",
    "manifest_cache_slot",      "manifest_group_wait",
    "field_prefetch_prepare",   "field_prefetch_load",
    "vector_prefetch_queue",    "vector_prefetch_run",
    "vector_prefetch_wait",     "mvcc_prefetch_queue",
    "mvcc_prefetch_run",        "mvcc_prefetch_wait",
    "manifest_load_cells",      "manifest_read_batch",
    "manifest_build_chunk",     "load_batch_budget_wait",
    "load_batch_queue",         "search_prepare",
    "search_execute",           "fill_primary_keys",
    "load_indexes_batch",       "load_indexes_wait",
    "load_index_queue",         "load_index_run",
    "load_column_groups_batch", "load_column_groups_wait",
    "load_column_group_queue",  "load_column_group_run",
};
static_assert(stage_names.size() ==
              static_cast<std::size_t>(QueryStage::Count));

struct StageMetrics {
    prometheus::Histogram* success;
    prometheus::Histogram* error;
    prometheus::Gauge* inflight;
};

const StageMetrics&
MetricsFor(QueryStage stage) {
    // Initialize the buckets and handles together on first use. In particular,
    // do not depend on another translation unit's global bucket initialization.
    static const auto metrics = [] {
        auto& registry = getPrometheusClient().GetRegistry();
        auto& durations =
            prometheus::BuildHistogram()
                .Name("internal_core_query_stage_duration_seconds")
                .Help(
                    "Native query/load stage wall time in seconds; nested "
                    "stages and parallel batches overlap.")
                .Register(registry);
        auto& inflight = prometheus::BuildGauge()
                             .Name("internal_core_query_stage_inflight")
                             .Help(
                                 "Operations currently inside a native "
                                 "query/load stage, including waits.")
                             .Register(registry);
        const prometheus::Histogram::BucketBoundaries buckets = {
            0.000001, 0.00001, 0.0001, 0.0005, 0.001, 0.0025, 0.005,
            0.01,     0.025,   0.05,   0.1,    0.25,  0.5,    1,
            2.5,      5,       10,     30,     120,   600};
        std::array<StageMetrics, stage_names.size()> result{};
        for (std::size_t i = 0; i < stage_names.size(); ++i) {
            result[i] = {
                &durations.Add(
                    {{"stage", stage_names[i]}, {"result", "success"}},
                    buckets),
                &durations.Add({{"stage", stage_names[i]}, {"result", "error"}},
                               buckets),
                &inflight.Add({{"stage", stage_names[i]}}),
            };
        }
        return result;
    }();
    return metrics[static_cast<std::size_t>(stage)];
}

}  // namespace

void
ObserveQueryStage(QueryStage stage,
                  QueryStageClock::duration elapsed,
                  bool failed) {
    const auto& metrics = MetricsFor(stage);
    (failed ? metrics.error : metrics.success)
        ->Observe(std::chrono::duration<double>(elapsed).count());
}

QueryStageTimer::QueryStageTimer(QueryStage stage)
    : stage_(stage), uncaught_exceptions_(std::uncaught_exceptions()) {
    MetricsFor(stage_).inflight->Increment();
    start_ = QueryStageClock::now();
}

QueryStageTimer::~QueryStageTimer() {
    End(std::uncaught_exceptions() > uncaught_exceptions_);
}

void
QueryStageTimer::End(bool failed) {
    if (!active_) {
        return;
    }
    ObserveQueryStage(stage_, QueryStageClock::now() - start_, failed);
    MetricsFor(stage_).inflight->Decrement();
    active_ = false;
}

QueryStageTaskTimer::QueryStageTaskTimer(QueryStage queue,
                                         QueryStage run,
                                         QueryStageClock::time_point submitted)
    : queue_(queue),
      queued_(QueryStageClock::now() - submitted),
      uncaught_exceptions_(std::uncaught_exceptions()),
      run_(run) {
}

QueryStageTaskTimer::~QueryStageTaskTimer() {
    ObserveQueryStage(
        queue_, queued_, std::uncaught_exceptions() > uncaught_exceptions_);
}

}  // namespace milvus::monitor
