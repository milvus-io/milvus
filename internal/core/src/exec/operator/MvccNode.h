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

#include <chrono>
#include <folly/futures/Future.h>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "common/RequestTrace.h"
#include "exec/Driver.h"
#include "exec/expression/Expr.h"
#include "exec/operator/Operator.h"
#include "exec/QueryContext.h"
#include "log/Log.h"

namespace milvus {
namespace exec {

class PhyMvccNode : public Operator {
 public:
    PhyMvccNode(int32_t operator_id,
                DriverContext* ctx,
                const std::shared_ptr<const plan::MvccNode>& mvcc_node);

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
        return "PhyMvccNode";
    }

    void
    PrefetchAsync(const std::shared_ptr<folly::CPUThreadPoolExecutor>
                      prefetch_pool) override {
        auto self = std::static_pointer_cast<PhyMvccNode>(shared_from_this());
        auto* query_context =
            self->operator_context_->get_exec_context()->get_query_context();
        auto search_info = query_context->get_search_info();
        auto trace_id =
            milvus::tracer::GetTraceIDAsHexStr(&search_info.trace_ctx_);
        if (trace_id.empty()) {
            trace_id = milvus::tracer::GetRequestTraceID(
                query_context->get_op_context());
        }
        const auto submit_time = std::chrono::steady_clock::now();
        prefetch_future_.emplace(
            folly::via(prefetch_pool.get(), [self, trace_id, submit_time]() {
                milvus::tracer::ScopedRequestTraceID trace_scope(trace_id);
                auto* query_context =
                    self->operator_context_->get_exec_context()
                        ->get_query_context();
                auto* op_context = query_context->get_op_context();
                milvus::tracer::ScopedOpContextTraceID op_trace(op_context,
                                                                trace_id);
                const auto start_time = std::chrono::steady_clock::now();
                const auto queue_duration_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        start_time - submit_time)
                        .count();
                LOG_INFO(
                    "[sss][www] milvus prefetch task start, traceID: {}, pool: "
                    "{}, task: {}, segment: {}, field: {}, queueDurationUs: {}",
                    trace_id,
                    "MILVUS_PREFETCH",
                    "mvcc",
                    self->segment_->get_segment_id(),
                    TimestampFieldID.get(),
                    queue_duration_us);
                self->segment_->prefetch_chunks(op_context, TimestampFieldID);
            }));
    }

    void
    WaitPrefetch() override {
        if (prefetch_future_.has_value()) {
            auto future = std::move(*prefetch_future_);
            prefetch_future_.reset();
            std::move(future).get();
        }
    }

 private:
    const segcore::SegmentInternalInterface* segment_;
    milvus::Timestamp query_timestamp_;
    int64_t active_count_;
    bool is_finished_{false};
    bool is_source_node_{false};
    milvus::Timestamp collection_ttl_timestamp_;
    std::optional<folly::Future<folly::Unit>> prefetch_future_;
};

}  // namespace exec
}  // namespace milvus
