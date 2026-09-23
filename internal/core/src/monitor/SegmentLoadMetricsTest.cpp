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

TEST(SegmentLoadMetrics, PhasesPartitionEachAttemptIncludingUnreachedStages) {
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

}  // namespace
}  // namespace milvus::monitor
