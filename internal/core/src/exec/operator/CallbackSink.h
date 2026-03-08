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

namespace milvus {
namespace exec {
class CallbackSink : public Operator {
 public:
    CallbackSink(
        int32_t operator_id,
        DriverContext* ctx,
        std::function<BlockingReason(RowVectorPtr, ContinueFuture*)> callback)
        : Operator(ctx, RowType::None, operator_id, "N/A", "CallbackSink"),
          callback_(callback) {
    }

    void
    AddInput(RowVectorPtr& input) override {
        blocking_reason_ = callback_(input, &future_);
    }

    RowVectorPtr
    GetOutput() override {
        return nullptr;
    }

    void
    NoMoreInput() override {
        Operator::NoMoreInput();
        Close();
    }

    bool
    NeedInput() const override {
        return callback_ != nullptr;
    }

    bool
    IsFilter() const override {
        return false;
    }

    bool
    IsFinished() override {
        return no_more_input_;
    }

    BlockingReason
    IsBlocked(ContinueFuture* future) override {
        if (blocking_reason_ != BlockingReason::kNotBlocked) {
            *future = std::move(future_);
            blocking_reason_ = BlockingReason::kNotBlocked;
            return BlockingReason::kWaitForConsumer;
        }
        return BlockingReason::kNotBlocked;
    }

    virtual std::string
    ToString() const override {
        return "CallbackSink";
    }

 private:
    void
    Close() override {
        if (callback_) {
            callback_(nullptr, nullptr);
            callback_ = nullptr;
        }
    }

    ContinueFuture future_;
    BlockingReason blocking_reason_{BlockingReason::kNotBlocked};
    std::function<BlockingReason(RowVectorPtr, ContinueFuture*)> callback_;
};

}  // namespace exec
}  // namespace milvus