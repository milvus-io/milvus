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

#include <random>

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
    active_count_ = operator_context_->get_exec_context()
                        ->get_query_context()
                        ->get_active_count();
}

void
PhyRandomSampleNode::AddInput(RowVectorPtr& input) {
    PanicInfo(UnexpectedError,
              "PhyRandomSampleNode should be the leaf source node");
}

FixedVector<uint32_t>
PhyRandomSampleNode::sample(const uint32_t N, const uint32_t M) {
    FixedVector<uint32_t> sampled(N);
    std::iota(sampled.begin(), sampled.end(), 0);
    std::random_device rd;
    std::mt19937 gen(rd());

    std::shuffle(sampled.begin(), sampled.end(), gen);
    sampled.resize(M);
    return sampled;
}

RowVectorPtr
PhyRandomSampleNode::GetOutput() {
    if (is_finished_) {
        return nullptr;
    }

    if (active_count_ == 0) {
        is_finished_ = true;
        return nullptr;
    }

    auto start = std::chrono::high_resolution_clock::now();

    auto col_input = std::make_shared<ColumnVector>(
        TargetBitmap(active_count_), TargetBitmap(active_count_));
    TargetBitmapView data(col_input->GetRawData(), col_input->size());

    auto sampled = sample(active_count_, active_count_ * factor_);
    for (auto n : sampled) {
        data[n] = true;
    }
    is_finished_ = true;

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> dur = end - start;
    LOG_INFO("debug_for_sample: sample duration {} seconds, sampled size {}", dur.count(), sampled.size());

    data.flip();
    return std::make_shared<RowVector>(std::vector<VectorPtr>{col_input});
}

bool
PhyRandomSampleNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus