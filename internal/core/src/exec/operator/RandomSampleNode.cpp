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

#include "RandomSampleNode.h"

#include <assert.h>
#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <ratio>
#include <unordered_set>
#include <utility>
#include <vector>

#include "bitset/bitset.h"
#include "common/EasyAssert.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "exec/QueryContext.h"
#include "exec/expression/Utils.h"
#include "fmt/core.h"
#include "monitor/Monitor.h"
#include "plan/PlanNode.h"
#include "prometheus/histogram.h"

namespace {
std::mt19937&
GetThreadLocalGen() {
    thread_local std::mt19937 gen(std::random_device{}());
    return gen;
}
}  // namespace

namespace milvus {
namespace exec {
PhyRandomSampleNode::PhyRandomSampleNode(
    int32_t operator_id,
    DriverContext* ctx,
    const std::shared_ptr<const plan::RandomSampleNode>& random_sample_node)
    : Operator(ctx,
               random_sample_node->output_type(),
               operator_id,
               random_sample_node->id(),
               "PhyRandomSampleNode"),
      factor_(random_sample_node->factor()) {
    // We should intercept unexpected number of factor at proxy level, so it's impossible to trigger
    // the panic here theoretically.
    AssertInfo(
        factor_ > 0.0 && factor_ < 1.0, "Unexpected sample factor {}", factor_);
    active_count_ = operator_context_->get_exec_context()
                        ->get_query_context()
                        ->get_active_count();
    is_source_node_ = random_sample_node->sources().size() == 0;
}

void
PhyRandomSampleNode::AddInput(RowVectorPtr& input) {
    input_ = std::move(input);
}

FixedVector<size_t>
PhyRandomSampleNode::FloydSample(const size_t N,
                                 const size_t M,
                                 std::mt19937& gen) {
    std::unordered_set<size_t> selected;
    selected.reserve(M);
    for (size_t j = N - M; j < N; ++j) {
        std::uniform_int_distribution<size_t> dis(0, j);
        size_t t = dis(gen);
        if (!selected.insert(t).second) {
            selected.insert(j);
        }
    }
    return FixedVector<size_t>(selected.begin(), selected.end());
}

RowVectorPtr
PhyRandomSampleNode::GetOutput() {
    auto* query_context =
        operator_context_->get_exec_context()->get_query_context();
    milvus::exec::checkCancellation(query_context);

    if (is_finished_) {
        return nullptr;
    }

    tracer::AutoSpan span(
        "PhyRandomSampleNode::Execute", tracer::GetRootSpan(), true);

    if (!is_source_node_ && input_ == nullptr) {
        return nullptr;
    }

    if (active_count_ == 0) {
        is_finished_ = true;
        return nullptr;
    }

    tracer::AddEvent(fmt::format(
        "sample_factor: {}, active_count: {}", factor_, active_count_));

    std::chrono::high_resolution_clock::time_point start =
        std::chrono::high_resolution_clock::now();

    RowVectorPtr result = nullptr;
    if (!is_source_node_) {
        auto input_col = GetColumnVector(input_);
        TargetBitmapView input_data(input_col->GetRawData(), input_col->size());
        // note: false means the elemnt is hit
        size_t input_false_count = input_data.size() - input_data.count();

        if (input_false_count > 0) {
            size_t remaining = input_false_count;
            size_t needed = std::max(
                static_cast<size_t>(input_false_count * factor_),
                static_cast<size_t>(1));
            auto& gen = GetThreadLocalGen();

            auto value = input_data.find_first(false);
            while (value.has_value()) {
                auto offset = value.value();
                auto next = input_data.find_next(offset, false);
                if (needed > 0 &&
                    std::uniform_int_distribution<size_t>(
                        0, remaining - 1)(gen) < needed) {
                    --needed;  // keep as false (selected)
                } else {
                    input_data[offset] = true;  // not selected
                }
                --remaining;
                value = next;
            }
        }

        result = std::make_shared<RowVector>(std::vector<VectorPtr>{input_col});
    } else {
        auto sample_output = std::make_shared<ColumnVector>(
            TargetBitmap(active_count_), TargetBitmap(active_count_));
        TargetBitmapView data(sample_output->GetRawData(),
                              sample_output->size());
        // true in TargetBitmap means we don't want this row.
        // We sample the minority set (min(factor, 1-factor)) and mark those
        // bits true, then flip if needed so that exactly factor*N bits are
        // false (wanted).
        bool need_flip = true;
        float factor = factor_;
        if (factor > 0.5) {
            need_flip = false;
            factor = 1.0 - factor;
        }

        size_t N = active_count_;
        size_t M = std::max(static_cast<size_t>(N * factor),
                            static_cast<size_t>(1));
        auto& gen = GetThreadLocalGen();

        float epsilon = std::numeric_limits<float>::epsilon();
        if (factor <= 0.02 + epsilon ||
            (N <= 10000000 && factor <= 0.045 + epsilon) ||
            (N <= 60000000 && factor <= 0.025 + epsilon)) {
            // Floyd: small M, use vector + scatter
            auto sampled = FloydSample(N, M, gen);
            for (auto n : sampled) {
                data[n] = true;
            }
        } else {
            // Selection sampling: write directly into bitmap
            size_t remaining = N, needed = M;
            for (size_t i = 0; i < N && needed > 0; ++i) {
                if (std::uniform_int_distribution<size_t>(
                        0, remaining - 1)(gen) < needed) {
                    data[i] = true;
                    --needed;
                }
                --remaining;
            }
        }

        if (need_flip) {
            data.flip();
        }

        result =
            std::make_shared<RowVector>(std::vector<VectorPtr>{sample_output});
    }

    std::chrono::high_resolution_clock::time_point end =
        std::chrono::high_resolution_clock::now();
    double duration =
        std::chrono::duration<double, std::micro>(end - start).count();
    milvus::monitor::internal_core_search_latency_random_sample.Observe(
        duration / 1000);

    if (result) {
        auto result_col = GetColumnVector(result);
        TargetBitmapView result_data(result_col->GetRawData(),
                                     result_col->size());
        auto sampled_count = result_col->size() - result_data.count();
        tracer::AddEvent(fmt::format("sampled_count: {}, total_count: {}",
                                     sampled_count,
                                     active_count_));
    }

    is_finished_ = true;

    return result;
}

bool
PhyRandomSampleNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus