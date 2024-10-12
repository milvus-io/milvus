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
#include "exec/operator/Operator.h"
#include "exec/operator/query-agg/GroupingSet.h"
#include "common/Types.h"

namespace milvus {
namespace exec {
class PhyAggregationNode : public Operator {
 public:
    PhyAggregationNode(
        int32_t operator_id,
        DriverContext* ctx,
        const std::shared_ptr<const plan::AggregationNode>& node);

    bool
    NeedInput() const override {
        return true;
    }

    void
    AddInput(RowVectorPtr& input) override;

    RowVectorPtr
    GetOutput() override;

    bool
    IsFinished() override {
        return finished_;
    }

    bool
    IsFilter() const override {
        return false;
    }

    BlockingReason
    IsBlocked(ContinueFuture* future) {
        return BlockingReason::kNotBlocked;
    }

    void
    Close() override {
        input_ = nullptr;
        results_.clear();
    }

    void
    initialize() override;

    std::string
    ToString() const override {
        return "PhyAggregationNode";
    }

 private:
    void
    prepareOutput(vector_size_t size);

    RowVectorPtr output_;
    std::unique_ptr<GroupingSet> grouping_set_;
    std::shared_ptr<const plan::AggregationNode> aggregationNode_;
    const bool isGlobal_;

    // Count the number of input rows. It is reset on partial aggregation output
    // flush.
    int64_t numInputRows_ = 0;
    // Count the number of output rows. It is reset on partial aggregation output
    // flush.
    int64_t numOutputRows_ = 0;
    bool finished_ = false;

    const int64_t group_limit_;
};
}  // namespace exec
}  // namespace milvus
