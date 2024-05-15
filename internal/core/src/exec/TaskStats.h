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

#include <cstdint>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "exec/Driver.h"

namespace milvus {
namespace exec {

struct OperatorStats;

struct PipelineStats {
    // std::vector<OperatorStats> operators_stats_;

    bool input_pipeline_;

    bool output_pipeline_;

    PipelineStats(bool input_pipeline, bool output_pipeline)
        : input_pipeline_(input_pipeline), output_pipeline_(output_pipeline) {
    }
};

struct TaskStats {
    int32_t num_total_splits_;
    int32_t num_finished_splits_;
    int32_t num_running_splits_;
    int32_t num_queued_splits_;
    std::unordered_set<int32_t> completed_split_groups_;

    std::vector<PipelineStats> pipeline_stats_;

    int64_t num_total_drivers_;
    int64_t num_completed_drivers_;
    int64_t num_terminated_drivers_;
    int64_t num_running_drivers_;

    std::unordered_map<BlockingReason, int64_t> num_blocked_drivers_;
};

}  // namespace exec
}  // namespace milvus