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

#include <stdint.h>
#include <memory>
#include <string>

#include "common/Promise.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/Driver.h"
#include "exec/QueryContext.h"
#include "exec/expression/Expr.h"
#include "exec/operator/Operator.h"
#include "plan/PlanNode.h"

namespace milvus {
namespace exec {

class PhyElementFilterNode : public Operator {
 public:
    PhyElementFilterNode(int32_t operator_id,
                         DriverContext* ctx,
                         const std::shared_ptr<const plan::ElementFilterNode>&
                             element_filter_node);

    bool
    IsFilter() const override {
        return true;
    }

    bool
    NeedInput() const override {
        return !is_finished_;
    }

    void
    AddInput(RowVectorPtr& input) override;

    RowVectorPtr
    GetOutput() override;

    bool
    IsFinished() override {
        return is_finished_;
    }

    void
    Close() override {
        Operator::Close();
        if (element_exprs_) {
            element_exprs_->Clear();
        }
    }

    BlockingReason
    IsBlocked(ContinueFuture* /* unused */) override {
        return BlockingReason::kNotBlocked;
    }

    std::string
    ToString() const override {
        return "PhyElementFilterNode";
    }

 private:
    std::unique_ptr<ExprSet> element_exprs_;
    QueryContext* query_context_;
    std::string struct_name_;
    bool is_finished_{false};
};

}  // namespace exec
}  // namespace milvus
