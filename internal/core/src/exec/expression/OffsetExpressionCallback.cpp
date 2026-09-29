// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#include "exec/expression/OffsetExpressionCallback.h"

#include <limits>
#include <sstream>
#include <utility>

#include "common/EasyAssert.h"
#include "log/Log.h"

namespace milvus::exec {
using Status = knowhere::CandidateEvalStatus;

struct OffsetExpressionCallback::Worker {
    std::unique_ptr<OffsetExpressionWorkspace> workspace;
    int64_t row_count;
    bool debug_trace_offsets;
};

OffsetExpressionCallback::OffsetExpressionCallback(
    expr::TypedExprPtr expression, ExecContext* exec_context, int64_t row_count)
    : prepared_(std::move(expression), exec_context, true),
      row_count_(row_count) {
    AssertInfo(
        row_count >= 0 && row_count <= std::numeric_limits<int32_t>::max(),
        "callback row count outside supported offset domain");
    const auto params =
        exec_context->get_query_context()->get_search_info().search_params_;
    // A standalone evaluator/test may have no ANN search parameters at all.
    debug_trace_offsets_ =
        params.contains("debug_ann_fusing_trace_offsets") &&
        params.value("debug_ann_fusing_trace_offsets", false);
}

knowhere::CandidateEvaluatorViewV1
OffsetExpressionCallback::view() const {
    return {1,
            sizeof(knowhere::CandidateEvaluatorViewV1),
            this,
            CreateWorker,
            DestroyWorker,
            EvalBatch};
}

Status
OffsetExpressionCallback::CreateWorker(const void* context,
                                       void** output) noexcept {
    if (output == nullptr) {
        return Status::InvalidArgument;
    }
    *output = nullptr;
    if (context == nullptr) {
        return Status::InvalidArgument;
    }
    try {
        const auto& factory =
            *static_cast<const OffsetExpressionCallback*>(context);
        auto workspace = factory.prepared_.CreateWorkspace();
        if (!workspace->SupportsOffsetInput()) {
            return Status::Failed;
        }
        *output = new Worker{std::move(workspace),
                             factory.row_count_,
                             factory.debug_trace_offsets_};
        LOG_DEBUG("ann_fusing task workspace created");
        return Status::Success;
    } catch (...) {
        return Status::Failed;
    }
}

void
OffsetExpressionCallback::DestroyWorker(void* worker) noexcept {
    delete static_cast<Worker*>(worker);
}

Status
OffsetExpressionCallback::EvalBatch(void* worker,
                                    const int32_t* row_ids,
                                    uint32_t count,
                                    uint64_t active_mask,
                                    uint64_t* accepted_mask) noexcept {
    if (accepted_mask == nullptr) {
        return Status::InvalidArgument;
    }
    *accepted_mask = 0;
    if (worker == nullptr || count > 64 || (count != 0 && row_ids == nullptr)) {
        return Status::InvalidArgument;
    }
    const uint64_t lanes =
        count == 64 ? ~uint64_t{0} : (uint64_t{1} << count) - 1;
    if ((active_mask & ~lanes) != 0) {
        return Status::InvalidArgument;
    }
    auto& state = *static_cast<Worker*>(worker);
    for (uint32_t lane = 0; lane < count; ++lane) {
        if ((active_mask & (uint64_t{1} << lane)) != 0 &&
            (row_ids[lane] < 0 || row_ids[lane] >= state.row_count)) {
            return Status::InvalidArgument;
        }
    }
    try {
        *accepted_mask =
            state.workspace->EvalAcceptedBatch(row_ids, count, active_mask);
        LOG_DEBUG("ann_fusing callback batch count={} active={} accepted={}",
                  count, active_mask, *accepted_mask);
        if (state.debug_trace_offsets) {
            // Diagnostic graph-trace replay keeps original order/batch shape.
            // Never read inactive lanes: callers need not initialize their IDs.
            std::ostringstream offsets;
            for (uint32_t lane = 0; lane < count; ++lane) {
                if (lane != 0)
                    offsets << ',';
                offsets << ((active_mask & (uint64_t{1} << lane))
                                ? row_ids[lane]
                                : -1);
            }
            LOG_DEBUG(
                "ann_fusing offset_trace worker={} count={} active={} "
                "accepted={} ids=[{}]",
                worker,
                count,
                active_mask,
                *accepted_mask,
                offsets.str());
        }
        return Status::Success;
    } catch (...) {
        return Status::Failed;
    }
}

}  // namespace milvus::exec
