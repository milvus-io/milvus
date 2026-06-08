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

#include <folly/futures/Future.h>
#include <stdint.h>
#include <memory>
#include <optional>
#include <string>

#include "common/Promise.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "common/protobuf_utils.h"
#include "exec/Driver.h"
#include "exec/QueryContext.h"
#include "exec/operator/Operator.h"
#include "plan/PlanNode.h"
#include "query/PlanImpl.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyVectorSearchNode : public Operator {
 public:
    PhyVectorSearchNode(
        int32_t operator_id,
        DriverContext* ctx,
        const std::shared_ptr<const plan::VectorSearchNode>& search_node);

    bool
    IsFilter() const override {
        return false;
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
        return "PhyVectorSearchNode";
    }

    void
    PrefetchAsync(const std::shared_ptr<folly::CPUThreadPoolExecutor>
                      prefetch_pool) override {
        auto self =
            std::static_pointer_cast<PhyVectorSearchNode>(shared_from_this());
        prefetch_future_.emplace(folly::via(prefetch_pool.get(), [self]() {
            auto* op_ctx = self->query_context_->get_op_context();
            if (op_ctx != nullptr &&
                op_ctx->cancellation_token.isCancellationRequested()) {
                return;
            }
            self->segment_->prefetch_vector(op_ctx,
                                            self->search_info_.field_id_);
        }));
    }

    void
    WaitPrefetch() override {
        if (prefetch_future_.has_value()) {
            std::move(*prefetch_future_).wait();
            prefetch_future_.reset();
        }
    }

 private:
    const milvus::segcore::SegmentInternalInterface* segment_;
    QueryContext* query_context_;
    milvus::Timestamp query_timestamp_;
    int64_t active_count_;
    bool is_finished_{false};

    const milvus::query::PlaceholderGroup* placeholder_group_;
    milvus::SearchInfo search_info_;

    std::optional<folly::Future<folly::Unit>> prefetch_future_;
};
}  // namespace exec
}  // namespace milvus
