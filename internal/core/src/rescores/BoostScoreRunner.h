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

#include <memory>
#include <optional>
#include <vector>

#include "common/OpContext.h"
#include "common/Types.h"
#include "pb/plan.pb.h"
#include "rescores/Scorer.h"
#include "segcore/SegmentInterface.h"

namespace milvus::exec {
class ExecContext;
}

namespace milvus::rescores {

// Evaluate the scorer's filter over the whole segment when the filter cannot
// consume offset input (text match, GIS, ...). Returns std::nullopt when the
// scorer has no filter or when every filter expression supports offset input
// natively -- ComputeScorerScores then evaluates the filter against the
// offsets it receives. UNKNOWN (NULL) rows are folded to FALSE.
//
// Callers that score multiple offset chunks against one scorer must compute
// this once and pass it to every ComputeScorerScores call; evaluating inside
// the per-chunk call would re-scan the whole segment once per chunk.
std::optional<TargetBitmap>
ComputeNonNativeFilterBitset(exec::ExecContext* exec_context,
                             const std::shared_ptr<Scorer>& scorer);

// filter_bitset: whole-segment filter result from
// ComputeNonNativeFilterBitset(); pass nullptr to let this call evaluate the
// filter itself (single-shot callers).
void
ComputeScorerScores(exec::ExecContext* exec_context,
                    OpContext* op_context,
                    const segcore::SegmentInternalInterface* segment,
                    const std::shared_ptr<Scorer>& scorer,
                    FixedVector<int32_t>& offsets,
                    std::vector<std::optional<float>>& output_scores,
                    const TargetBitmap* filter_bitset = nullptr);

void
ComputeScorerScores(exec::ExecContext* exec_context,
                    OpContext* op_context,
                    const segcore::SegmentInternalInterface* segment,
                    const std::shared_ptr<Scorer>& scorer,
                    FixedVector<int32_t>& offsets,
                    float* output_scores,
                    bool* output_has_score,
                    const TargetBitmap* filter_bitset = nullptr);

void
ComputeFunctionScores(exec::ExecContext* exec_context,
                      OpContext* op_context,
                      const segcore::SegmentInternalInterface* segment,
                      const std::vector<std::shared_ptr<Scorer>>& scorers,
                      proto::plan::FunctionMode function_mode,
                      FixedVector<int32_t>& offsets,
                      std::vector<std::optional<float>>& output_scores);

void
ComputeFunctionScores(exec::ExecContext* exec_context,
                      OpContext* op_context,
                      const segcore::SegmentInternalInterface* segment,
                      const std::vector<std::shared_ptr<Scorer>>& scorers,
                      proto::plan::FunctionMode function_mode,
                      FixedVector<int32_t>& offsets,
                      float* output_scores,
                      bool* output_has_score);

}  // namespace milvus::rescores
