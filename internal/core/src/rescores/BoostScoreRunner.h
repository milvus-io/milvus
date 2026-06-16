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

void
ComputeScorerScores(exec::ExecContext* exec_context,
                    OpContext* op_context,
                    const segcore::SegmentInternalInterface* segment,
                    const std::shared_ptr<Scorer>& scorer,
                    FixedVector<int32_t>& offsets,
                    std::vector<std::optional<float>>& output_scores);

void
ComputeScorerScores(exec::ExecContext* exec_context,
                    OpContext* op_context,
                    const segcore::SegmentInternalInterface* segment,
                    const std::shared_ptr<Scorer>& scorer,
                    FixedVector<int32_t>& offsets,
                    float* output_scores,
                    bool* output_has_score);

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
