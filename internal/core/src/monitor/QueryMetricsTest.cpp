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
#include "monitor/SegmentLoadMetrics.h"

#include <gtest/gtest.h>
#include <chrono>
#include <cmath>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "common/PrometheusClient.h"

namespace milvus::monitor {
namespace {

prometheus::ClientMetric
Snapshot(const std::string& family_name,
         const std::string& stage,
         const std::string& result = "") {
    for (const auto& family : getPrometheusClient().GetRegistry().Collect()) {
        if (family.name != family_name) {
            continue;
        }
        for (const auto& metric : family.metric) {
            bool matches_stage = false;
            bool matches_result = result.empty();
            for (const auto& label : metric.label) {
                if (label.name == "stage" && label.value == stage) {
                    matches_stage = true;
                }
                if (label.name == "result" && label.value == result) {
                    matches_result = true;
                }
            }
            if (matches_stage && matches_result) {
                return metric;
            }
        }
    }
    return {};
}

constexpr auto duration_family = "internal_core_query_stage_duration_seconds";
constexpr auto inflight_family = "internal_core_query_stage_inflight";

TEST(QueryMetrics, ExportsFiniteBucketsAndSeconds) {
    const auto before =
        Snapshot(duration_family, "manifest_reader_open", "success");
    ObserveQueryStage(QueryStage::ManifestReaderOpen,
                      std::chrono::milliseconds(10));
    const auto after =
        Snapshot(duration_family, "manifest_reader_open", "success");
    EXPECT_EQ(after.histogram.sample_count, before.histogram.sample_count + 1);
    EXPECT_NEAR(
        after.histogram.sample_sum - before.histogram.sample_sum, 0.01, 1e-9);
    ASSERT_EQ(after.histogram.bucket.size(), 21);
    EXPECT_DOUBLE_EQ(after.histogram.bucket.front().upper_bound, 0.000001);
    EXPECT_DOUBLE_EQ(after.histogram.bucket[19].upper_bound, 600);
    EXPECT_TRUE(std::isinf(after.histogram.bucket.back().upper_bound));
    // Check the actual exported text too: a histogram with only +Inf cannot
    // identify the cold-read tail with histogram_quantile.
    const auto text = getPrometheusClient().GetMetrics();
    EXPECT_NE(text.find("internal_core_query_stage_duration_seconds_bucket"),
              std::string::npos);
    EXPECT_NE(text.find("le=\"0.01\""), std::string::npos);
    EXPECT_EQ(after.label.size(), 2);
}

TEST(QueryMetrics, EarlyEndRecordsOnceAndClearsInflight) {
    const auto before =
        Snapshot(duration_family, "manifest_translator", "success");
    {
        QueryStageTimer timer(QueryStage::ManifestTranslator);
        EXPECT_DOUBLE_EQ(
            Snapshot(inflight_family, "manifest_translator").gauge.value, 1);
        timer.End();
        EXPECT_DOUBLE_EQ(
            Snapshot(inflight_family, "manifest_translator").gauge.value, 0);
        timer.End();
    }
    EXPECT_EQ(Snapshot(duration_family, "manifest_translator", "success")
                  .histogram.sample_count,
              before.histogram.sample_count + 1);
}

TEST(QueryMetrics, ExceptionAndReturnedFailureAreRecorded) {
    const auto before =
        Snapshot(duration_family, "manifest_read_batch", "error");
    const auto success_before =
        Snapshot(duration_family, "manifest_read_batch", "success");
    EXPECT_THROW(
        {
            QueryStageTimer timer(QueryStage::ManifestReadBatch);
            throw std::runtime_error("injected read failure");
        },
        std::runtime_error);
    {
        QueryStageTimer timer(QueryStage::ManifestReadBatch);
        timer.End(true);
    }
    EXPECT_EQ(Snapshot(duration_family, "manifest_read_batch", "error")
                  .histogram.sample_count,
              before.histogram.sample_count + 2);
    EXPECT_EQ(Snapshot(duration_family, "manifest_read_batch", "success")
                  .histogram.sample_count,
              success_before.histogram.sample_count);
    EXPECT_DOUBLE_EQ(
        Snapshot(inflight_family, "manifest_read_batch").gauge.value, 0);
}

TEST(QueryMetrics, ConcurrentTimersDoNotLoseSamplesOrLeakInflight) {
    const auto before =
        Snapshot(duration_family, "manifest_group_wait", "success");
    std::vector<std::thread> workers;
    for (int i = 0; i < 8; ++i) {
        workers.emplace_back([] {
            for (int j = 0; j < 100; ++j) {
                QueryStageTimer timer(QueryStage::ManifestGroupWait);
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(Snapshot(duration_family, "manifest_group_wait", "success")
                  .histogram.sample_count,
              before.histogram.sample_count + 800);
    EXPECT_DOUBLE_EQ(
        Snapshot(inflight_family, "manifest_group_wait").gauge.value, 0);
}

TEST(QueryMetrics, LoadPhasesPartitionEachAttemptIncludingSkippedStages) {
    constexpr auto family = "internal_core_segment_load_duration_seconds";
    const std::vector<std::string> phases = {"lock_wait",
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
                                             "publish"};
    for (bool failed : {false, true}) {
        const auto result = failed ? "error" : "success";
        std::vector<prometheus::ClientMetric> before;
        for (const auto& phase : phases) {
            before.push_back(Snapshot(family, phase, result));
        }
        const auto total_before = Snapshot(family, "total", result);
        try {
            SegmentLoadTiming timing;
            SegmentLoadTiming::SwitchTo(&timing, SegmentLoadPhase::Indexes);
            if (failed) {
                throw std::runtime_error("load failure");
            }
            SegmentLoadTiming::SwitchTo(&timing, SegmentLoadPhase::Publish);
            timing.End();
            timing.End();
        } catch (const std::runtime_error&) {
        }
        double sum = 0;
        for (std::size_t i = 0; i < phases.size(); ++i) {
            const auto after = Snapshot(family, phases[i], result);
            EXPECT_EQ(after.histogram.sample_count,
                      before[i].histogram.sample_count + 1);
            const auto elapsed =
                after.histogram.sample_sum - before[i].histogram.sample_sum;
            if (phases[i] == "column_groups") {
                EXPECT_DOUBLE_EQ(elapsed, 0);
            }
            sum += elapsed;
        }
        const auto total = Snapshot(family, "total", result);
        EXPECT_EQ(total.histogram.sample_count,
                  total_before.histogram.sample_count + 1);
        EXPECT_NEAR(
            sum,
            total.histogram.sample_sum - total_before.histogram.sample_sum,
            1e-9);
    }
}

TEST(QueryMetrics, QueuedCanceledTaskRecordsQueueAndRunWithSameOutcome) {
    const auto queue = Snapshot(duration_family, "load_index_queue", "error");
    const auto run = Snapshot(duration_family, "load_index_run", "error");
    EXPECT_THROW(
        {
            QueryStageTaskTimer timing(
                QueryStage::LoadIndexQueue,
                QueryStage::LoadIndexRun,
                QueryStageClock::now() - std::chrono::milliseconds(5));
            throw std::runtime_error("canceled before loading");
        },
        std::runtime_error);
    const auto after_queue =
        Snapshot(duration_family, "load_index_queue", "error");
    const auto after_run = Snapshot(duration_family, "load_index_run", "error");
    EXPECT_EQ(after_queue.histogram.sample_count,
              queue.histogram.sample_count + 1);
    EXPECT_EQ(after_run.histogram.sample_count, run.histogram.sample_count + 1);
    EXPECT_GE(after_queue.histogram.sample_sum - queue.histogram.sample_sum,
              0.005);
    EXPECT_DOUBLE_EQ(Snapshot(inflight_family, "load_index_run").gauge.value,
                     0);
}

}  // namespace
}  // namespace milvus::monitor
