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
PhyRandomSampleNode::HashsetSample(const uint32_t N,
                                   const uint32_t M,
                                   std::mt19937& gen) {
    LOG_INFO("debug_for_sample: HashsetSample");
    FixedVector<uint32_t> sampled(N);
    std::iota(sampled.begin(), sampled.end(), 0);

    std::shuffle(sampled.begin(), sampled.end(), gen);
    sampled.resize(M);
    return sampled;
}

FixedVector<uint32_t>
PhyRandomSampleNode::StandardSample(const uint32_t N,
                                    const uint32_t M,
                                    std::mt19937& gen) {
    LOG_INFO("debug_for_sample: StandardSample");
    FixedVector<uint32_t> inputs(N);
    FixedVector<uint32_t> outputs(M);
    std::iota(inputs.begin(), inputs.end(), 0);

    std::sample(inputs.begin(), inputs.end(), outputs.begin(), M, gen);
    return outputs;
}

FixedVector<uint32_t>
PhyRandomSampleNode::Sample(const uint32_t N, const float factor) {
    const uint32_t M = N * factor;
    std::random_device rd;
    std::mt19937 gen(rd());
    if (factor <= 0.03 + std::numeric_limits<float>::epsilon() ||
        N <= 10000000 &&
            factor <= 0.05 + std::numeric_limits<float>::epsilon()) {
        return HashsetSample(N, M, gen);
    }
    return StandardSample(N, M, gen);
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
    // True in TargetBitmap means we don't want this row, while for readability, we set the relevant row be true
    // if it's sampled. So we need to flip the bits at last.
    // However, if sample rate is larger than 0.5, we use 1-factor for sampling so that in some cases, the sampling
    // performance would be better. In that case, we don't need to flip at last.
    bool need_flip = true;
    float factor = factor_;
    if (factor > 0.5) {
        need_flip = false;
        factor = 1.0 - factor;
    }
    auto sampled = Sample(active_count_, factor);
    for (auto n : sampled) {
        data[n] = true;
    }
    is_finished_ = true;

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> dur = end - start;
    LOG_INFO("debug_for_sample: sample duration {} seconds, sampled size {}",
             dur.count(),
             sampled.size());

    if (need_flip) {
        data.flip();
    }
    return std::make_shared<RowVector>(std::vector<VectorPtr>{col_input});
}

bool
PhyRandomSampleNode::IsFinished() {
    return is_finished_;
}

}  // namespace exec
}  // namespace milvus