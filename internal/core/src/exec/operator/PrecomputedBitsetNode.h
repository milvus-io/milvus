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
#include "exec/QueryContext.h"
#include "exec/operator/Operator.h"

namespace milvus {
namespace exec {

// Source operator that emits, exactly once, the bitset produced by a prior
// execution of the shared prefix. The bitset is owned by the QueryContext
// (set via set_precomputed_bitset) and handed downstream by shared_ptr, so all
// branches of a shared-filter group observe the same buffer without copying.
//
// Downstream operators MUST treat the emitted RowVector as read-only. Today
// PhyVectorSearchNode only reads it (it builds a BitsetView over the raw
// data); the element-level path derives a new bitmap rather than mutating
// this one.
class PhyPrecomputedBitsetNode : public Operator {
 public:
    PhyPrecomputedBitsetNode(
        int32_t operator_id,
        DriverContext* ctx,
        const std::shared_ptr<const plan::PrecomputedBitsetNode>& node);

    bool
    IsFilter() const override {
        return false;
    }

    bool
    NeedInput() const override {
        return false;
    }

    void
    AddInput(RowVectorPtr& input) override {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "PhyPrecomputedBitsetNode is a source operator and accepts no "
                  "input");
    }

    RowVectorPtr
    GetOutput() override;

    bool
    IsFinished() override {
        return is_finished_;
    }

    void
    Close() override {
    }

    BlockingReason
    IsBlocked(ContinueFuture* /* unused */) override {
        return BlockingReason::kNotBlocked;
    }

    std::string
    ToString() const override {
        return "PhyPrecomputedBitsetNode";
    }

 private:
    QueryContext* query_context_;
    bool is_finished_{false};
};

}  // namespace exec
}  // namespace milvus
