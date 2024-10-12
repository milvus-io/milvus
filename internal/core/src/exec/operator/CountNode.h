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
#include <string>

#include "exec/Driver.h"
#include "exec/expression/Expr.h"
#include "exec/operator/Operator.h"
#include "exec/QueryContext.h"

namespace milvus {
namespace exec {

class PhyCountNode : public Operator {
 public:
    PhyCountNode(int32_t operator_id,
                 DriverContext* ctx,
                 const std::shared_ptr<const plan::CountNode>& node);

    bool
    IsFilter() const override {
        return false;
    }

    bool
    NeedInput() const override {
        return !is_finished_;
    }

    void
    AddInput(RowVectorPtr& input);

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
        return "PhyCountNode";
    }

 private:
    const segcore::SegmentInternalInterface* segment_;
    milvus::Timestamp query_timestamp_;
    int64_t active_count_;
    QueryContext* query_context_;
    bool is_finished_{false};
};

}  // namespace exec
}  // namespace milvus