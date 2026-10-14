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
#include <cstdint>

namespace milvus {

// Byte-equivalent routing weights, not latency predictions. rows/text_bytes
// and the SA rate describe the complete immutable index. A batch's upstream
// bitmap supplies its active row count separately. These estimates never
// remove candidates or change the canonical RE2 recheck.
struct RegexIndexCostModel {
    size_t rows = 0;
    size_t text_bytes = 0;
    uint32_t sa_sample_rate = 8;
    double cost_ratio = 0.001;

    // Constructing string views and traversing their masks is work even when
    // most rows are filtered out. Offset gathering on the candidate path has
    // a higher per-row cost than a sequential scan. Initial weights price
    // these fixed operations; profiling can refine them without changing
    // result semantics or the existing fmindexCostRatio's locate units.
    double scan_view_bytes = 32;
    double scan_active_row_bytes = 64;
    double index_active_row_bytes = 128;
    double scan_batch_bytes = 1024;

    // Do not start AST preparation/Count for a scan window too small to
    // amortize them. This is a conservative initial preparation budget, in
    // the same relative units as the scan estimate, not actual bytes read.
    double preparation_floor_bytes = 128 * 1024;
};

// Exact upstream work in a contiguous remaining row window. Visited rows and
// batches describe normal physical scan batches, not the logical result size
// of an all-at-once expression. These statistics only affect routing.
struct RegexIndexWorkStats {
    size_t remaining_rows = 0;
    size_t active_rows = 0;
    size_t visited_rows = 0;
    size_t visited_batches = 0;
};

namespace regex_index_detail {

inline bool
ValidCosts(const RegexIndexCostModel& model) {
    return std::isfinite(model.scan_view_bytes) && model.scan_view_bytes >= 0 &&
           std::isfinite(model.scan_active_row_bytes) &&
           model.scan_active_row_bytes >= 0 &&
           std::isfinite(model.index_active_row_bytes) &&
           model.index_active_row_bytes >= 0 &&
           std::isfinite(model.scan_batch_bytes) && model.scan_batch_bytes >= 0;
}

struct ScanWindow {
    double remaining_rows = 0;
    double batch_rows = 0;
    double active_rows = 0;
    double visited_rows = 0;
    double visited_batches = 0;
};

// active_batch_rows is the CURRENT batch's upstream bitmap popcount. Density
// extrapolated over the remaining physical window is only a routing estimate:
// future batches and correlated predicates can have different selectivity.
// A rejected batch therefore must not permanently disable the FM route.
inline ScanWindow
EstimateScanWindow(const RegexIndexCostModel& model,
                   size_t remaining_rows,
                   size_t active_batch_rows,
                   size_t batch_rows,
                   const RegexIndexWorkStats* exact = nullptr) {
    ScanWindow window;
    window.remaining_rows =
        static_cast<double>(std::min(remaining_rows, model.rows));
    window.batch_rows =
        std::min(window.remaining_rows, static_cast<double>(batch_rows));
    const auto max_batches =
        batch_rows == 0
            ? size_t{0}
            : std::min(remaining_rows, model.rows) / batch_rows +
                  (std::min(remaining_rows, model.rows) % batch_rows != 0);
    if (exact != nullptr && batch_rows != 0 &&
        exact->remaining_rows == remaining_rows &&
        remaining_rows <= model.rows &&
        exact->active_rows <= exact->visited_rows &&
        exact->visited_rows <= remaining_rows &&
        exact->visited_batches <= max_batches &&
        exact->visited_batches <= exact->visited_rows &&
        (exact->active_rows != 0
             ? exact->visited_batches != 0
             : (exact->visited_rows == 0 && exact->visited_batches == 0))) {
        window.active_rows = static_cast<double>(exact->active_rows);
        window.visited_rows = static_cast<double>(exact->visited_rows);
        window.visited_batches = static_cast<double>(exact->visited_batches);
        return window;
    }
    const double active =
        std::min(window.batch_rows, static_cast<double>(active_batch_rows));
    if (active == 0 || window.batch_rows == 0) {
        return window;
    }
    const double density = active / window.batch_rows;
    window.active_rows = window.remaining_rows * density;

    // Upstream AND evaluation can skip completely empty future batches. The
    // current batch is known to be nonempty, so its views are certain work.
    // For scattered masks, most batches still construct views even at a low
    // density. Callers that do not skip empty batches may scan more than this
    // estimate; that bias only makes this routing gate more conservative.
    const double nonempty_probability =
        density >= 1 ? 1
                     : -std::expm1(window.batch_rows * std::log1p(-density));
    window.visited_rows =
        window.batch_rows +
        (window.remaining_rows - window.batch_rows) * nonempty_probability;
    // A full-active window certainly pays for the real short tail batch too.
    // This also keeps its cost an upper bound for permanent-rejection proofs
    // when an exact bitmap window supplies integer visited-batch statistics.
    window.visited_batches = density >= 1
                                 ? static_cast<double>(max_batches)
                                 : window.visited_rows / window.batch_rows;
    return window;
}

inline double
ScanCost(const RegexIndexCostModel& model, const ScanWindow& window) {
    if (model.rows == 0 || !ValidCosts(model) || window.active_rows == 0) {
        return 0;
    }
    const double bytes_per_row =
        static_cast<double>(model.text_bytes) / model.rows;
    return window.visited_rows * model.scan_view_bytes +
           window.visited_batches * model.scan_batch_bytes +
           window.active_rows * (bytes_per_row + model.scan_active_row_bytes);
}

}  // namespace regex_index_detail

inline double
RegexIndexScanCost(const RegexIndexCostModel& model,
                   size_t remaining_rows,
                   size_t active_batch_rows,
                   size_t batch_rows,
                   const RegexIndexWorkStats* exact = nullptr) {
    return regex_index_detail::ScanCost(
        model,
        regex_index_detail::EstimateScanWindow(
            model, remaining_rows, active_batch_rows, batch_rows, exact));
}

// This gate needs neither AST analysis nor Count. Later, denser batches can
// cross the gate and initialize a plan; the execution layer must keep its
// normal scan cursor/batch behavior while preparation is deferred.
inline bool
ShouldPrepareRegexIndexPlan(const RegexIndexCostModel& model,
                            size_t remaining_rows,
                            size_t active_batch_rows,
                            size_t batch_rows,
                            const RegexIndexWorkStats* exact = nullptr) {
    return std::isfinite(model.cost_ratio) && model.cost_ratio > 0 &&
           std::isfinite(model.preparation_floor_bytes) &&
           model.preparation_floor_bytes >= 0 &&
           RegexIndexScanCost(
               model, remaining_rows, active_batch_rows, batch_rows, exact) >
               model.preparation_floor_bytes;
}

}  // namespace milvus
