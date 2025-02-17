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

#include <random>

#include "exec/operator/Operator.h"

namespace milvus {
namespace exec {

class PhyRandomSampleNode : public Operator {
 public:
    PhyRandomSampleNode(int32_t operator_id,
                        DriverContext* ctx,
                        const std::shared_ptr<const plan::RandomSampleNode>&
                            random_sample_node);

    bool
    IsFilter() override {
        return false;
    }

    bool
    NeedInput() const override {
        return !input_;
    }

    void
    AddInput(RowVectorPtr& input) override;

    RowVectorPtr
    GetOutput() override;

    bool
    IsFinished() override;

    void
    Close() override {
    }

    BlockingReason
    IsBlocked(ContinueFuture* /* unused */) override {
        return BlockingReason::kNotBlocked;
    }

    virtual std::string
    ToString() const override {
        return "PhyRandomSample";
    }

 private:
    // Samples M elements from 0 to N - 1 where every element has equal probability to be selected.
    static FixedVector<uint32_t>
    HashsetSample(const uint32_t N, const uint32_t M, std::mt19937& gen);

    // Samples M elements from 0 to N - 1 where every element has equal probability to be selected.
    static FixedVector<uint32_t>
    StandardSample(const uint32_t N, const uint32_t M, std::mt19937& gen);

    // Samples M elements from 0 to N - 1 where every element has equal probability to be selected.
    // It selects which kinds of sample method we use for sampling which is based on some experiments.
    // Basically, the performance of HashsetSample is highly related to N * factor while StandardSample is highly
    // related to N.
    static FixedVector<uint32_t>
    Sample(const uint32_t N, const float factor);

    float factor_{0};
    int64_t active_count_{0};
    bool is_finished_{false};
    bool is_source_node_{false};
};

}  // namespace exec
}  // namespace milvus