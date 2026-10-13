// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <functional>
#include <limits>

#include "common/RegexIndexCostModel.h"
#include "common/RegexQuery.h"

namespace milvus {

// Prepared once for one expression and one immutable index. The existing
// selection policy chooses the least-Count AND child and retains every OR
// branch. Subsequent batch decisions use these scalar statistics only;
// execution consumes condition directly, without repeating Count/select.
struct RegexIndexPlan {
    RegexIndexCostModel model;
    RegexLiteralCondition condition;
    double occurrences = std::numeric_limits<double>::infinity();
    double estimated_rows = 0;
    double bitmap_cost = 0;
    size_t literal_count = 0;
    size_t bitmap_operations = 0;
    bool empty_result = false;

    bool
    Usable() const {
        return !condition.IsTrue() && std::isfinite(occurrences);
    }
};

namespace regex_index_detail {

inline void
CountBitmapWork(const RegexLiteralCondition& condition,
                size_t& literals,
                size_t& operations) {
    if (condition.op == RegexLiteralCondition::Op::Literal) {
        ++literals;
        return;
    }
    if (!condition.children.empty()) {
        operations += condition.children.size() - 1;
    }
    for (const auto& child : condition.children) {
        CountBitmapWork(child, literals, operations);
    }
}

}  // namespace regex_index_detail

inline RegexIndexPlan
PrepareRegexIndexPlan(const RegexLiteralCondition& requirements,
                      const std::function<size_t(const std::string&)>& count,
                      const RegexIndexCostModel& model) {
    RegexIndexPlan plan;
    plan.model = model;
    plan.condition = requirements.Select(count, plan.occurrences);
    if (!plan.Usable()) {
        return plan;
    }
    // Count is occurrences, not distinct rows. min(C,N) is deliberately a
    // rough, conservative candidate estimate. The actual bitmap determines
    // which rows are read; estimates cannot remove a row or an OR branch.
    plan.estimated_rows =
        std::min(static_cast<double>(model.rows), plan.occurrences);
    plan.empty_result = plan.occurrences == 0;
    regex_index_detail::CountBitmapWork(
        plan.condition, plan.literal_count, plan.bitmap_operations);
    const double bitmap_bytes =
        std::ceil(static_cast<double>(model.rows) / 64) * sizeof(uint64_t);
    // One zeroed bitmap per located literal; each boolean operation reads
    // two bitmaps and writes one. Three more passes price cloning/flipping
    // the index's validity bitmap. Duplicate OR literals remain priced as
    // separate work, matching the current executor.
    plan.bitmap_cost =
        bitmap_bytes * static_cast<double>(plan.literal_count +
                                           3 * plan.bitmap_operations + 3);
    return plan;
}

inline double
RegexIndexFixedCost(const RegexIndexPlan& plan) {
    if (!plan.Usable()) {
        return std::numeric_limits<double>::infinity();
    }
    if (plan.empty_result) {
        return 0;
    }
    const auto& model = plan.model;
    if (model.rows == 0 || !regex_index_detail::ValidCosts(model) ||
        !std::isfinite(model.cost_ratio) || model.cost_ratio <= 0) {
        return std::numeric_limits<double>::infinity();
    }
    // This entire term is paid once over the complete index, regardless of
    // upstream selectivity or how many physical scan rows remain.
    return plan.occurrences * std::max<uint32_t>(1, model.sa_sample_rate) /
               model.cost_ratio +
           plan.bitmap_cost;
}

// Prefilter-aware routing adds a rejection gate to the existing full-index
// Locate upper limit. View/dispatch weights must not accidentally broaden its
// accepted Count range, particularly for short strings. A finite zero Count
// remains an exact empty-result proof independent of heuristic cost settings.
inline bool
RegexIndexPassesLocateGuard(const RegexIndexPlan& plan) {
    if (!plan.Usable()) {
        return false;
    }
    if (plan.empty_result) {
        return true;
    }
    const auto& model = plan.model;
    return std::isfinite(model.cost_ratio) && model.cost_ratio > 0 &&
           plan.occurrences * static_cast<double>(model.sa_sample_rate) <
               model.cost_ratio * static_cast<double>(model.text_bytes);
}

// Permanently decline when the unchanged Locate limit fails, or the fixed
// Locate/bitmap work alone exceeds the most expensive (all-active) scan of
// the current remaining window. All later windows are smaller and candidate
// recheck costs are nonnegative. A sparse-batch rejection is not such a proof.
inline bool
CanRegexIndexPlanEverHelp(const RegexIndexPlan& plan,
                          size_t remaining_rows,
                          size_t batch_rows) {
    if (!RegexIndexPassesLocateGuard(plan) || remaining_rows == 0 ||
        batch_rows == 0) {
        return false;
    }
    return plan.empty_result ||
           RegexIndexFixedCost(plan) <
               RegexIndexScanCost(
                   plan.model, remaining_rows, batch_rows, batch_rows);
}

inline double
RegexIndexExecutionCost(const RegexIndexPlan& plan,
                        size_t remaining_rows,
                        size_t active_batch_rows,
                        size_t batch_rows) {
    if (!plan.Usable()) {
        return std::numeric_limits<double>::infinity();
    }
    if (plan.empty_result) {
        // A finite selected Count of zero is a proof, unlike TRUE/no usable
        // condition. It requires no Locate and no raw string reads.
        return 0;
    }
    const auto& model = plan.model;
    if (model.rows == 0 || !regex_index_detail::ValidCosts(model) ||
        !std::isfinite(model.cost_ratio) || model.cost_ratio <= 0) {
        return std::numeric_limits<double>::infinity();
    }
    const auto window = regex_index_detail::EstimateScanWindow(
        model, remaining_rows, active_batch_rows, batch_rows);
    const double surviving_candidates =
        plan.estimated_rows * window.active_rows / model.rows;
    const double bytes_per_row =
        static_cast<double>(model.text_bytes) / model.rows;
    // Locate and construction of its full-segment bitmap are NOT scaled by
    // prefilter density. Only candidate reads/rechecks and remaining batch
    // slicing are reduced. Bitmap intersection still uses exact row IDs.
    return RegexIndexFixedCost(plan) +
           2 * std::ceil(window.remaining_rows / 8) +
           surviving_candidates *
               (bytes_per_row + model.index_active_row_bytes);
}

inline bool
ShouldUseRegexIndexPlan(const RegexIndexPlan& plan,
                        size_t remaining_rows,
                        size_t active_batch_rows,
                        size_t batch_rows) {
    if (remaining_rows == 0 || active_batch_rows == 0 || batch_rows == 0 ||
        !RegexIndexPassesLocateGuard(plan)) {
        return false;
    }
    return plan.empty_result ||
           RegexIndexExecutionCost(
               plan, remaining_rows, active_batch_rows, batch_rows) <
               RegexIndexScanCost(
                   plan.model, remaining_rows, active_batch_rows, batch_rows);
}

}  // namespace milvus
