// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#include "exec/expression/OffsetExpressionCallback.h"

#include <limits>
#include <sstream>
#include <utility>

#include "common/EasyAssert.h"
#include "exec/FilterDiagnostics.h"
#include "log/Log.h"

namespace milvus::exec {
using Status = knowhere::CandidateEvalStatus;

struct OffsetExpressionCallback::Worker {
    std::unique_ptr<OffsetExpressionWorkspace> workspace;
    int64_t row_count;
    bool debug_trace_offsets;
    std::unique_ptr<FilterDiagnostics> profile;
    int64_t segment_id;
    uint64_t query_timestamp;
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
    debug_profile_ = params.contains("debug_ann_fusing_profile") &&
                     params.value("debug_ann_fusing_profile", false);
    if (debug_profile_) {
        auto* query = exec_context->get_query_context();
        segment_id_ = query->get_segment()->get_segment_id();
        query_timestamp_ = query->get_query_timestamp();
    }
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
    const auto& factory =
        *static_cast<const OffsetExpressionCallback*>(context);
    const auto report_failure = [&factory]() noexcept {
        if (factory.debug_profile_) {
            try {
                LOG_INFO(
                    "ann_fusing profile phase=workspace segment={} "
                    "timestamp={} status=failed",
                    factory.segment_id_,
                    factory.query_timestamp_);
            } catch (...) {
            }
        }
    };
    try {
        auto profile = factory.debug_profile_
                           ? std::make_unique<FilterDiagnostics>()
                           : nullptr;
        FilterDiagnosticTimer prepare(profile ? &profile->prepare_ns : nullptr);
        auto workspace = factory.prepared_.CreateWorkspace();
        if (!workspace->SupportsOffsetInput()) {
            report_failure();
            return Status::Failed;
        }
        prepare.Stop();
        *output = new Worker{std::move(workspace),
                             factory.row_count_,
                             factory.debug_trace_offsets_,
                             std::move(profile),
                             factory.segment_id_,
                             factory.query_timestamp_};
        LOG_DEBUG("ann_fusing task workspace created");
        return Status::Success;
    } catch (...) {
        report_failure();
        return Status::Failed;
    }
}

void
OffsetExpressionCallback::DestroyWorker(void* worker) noexcept {
    std::unique_ptr<Worker> state(static_cast<Worker*>(worker));
    if (state && state->profile) {
        // Emit once per worker, outside measured callbacks. Diagnostic output
        // is deliberately INFO so it needs no process-wide debug log change.
        try {
            const auto& p = *state->profile;
            std::ostringstream sizes;
            for (size_t i = 0; i < p.batch_sizes.size(); ++i) {
                if (p.batch_sizes[i])
                    sizes << i << ':' << p.batch_sizes[i] << ',';
            }
            LOG_INFO(
                "ann_fusing profile phase=callback segment={} timestamp={} "
                "worker={} calls={} input_rows={} active_rows={} "
                "accepted_rows={} "
                "errors={} prepare_us={} callback_us={} raw_path_rows={} "
                "index_path_rows={} raw_path_us={} index_path_us={} "
                "raw_read_rows={} index_read_rows={} raw_read_us={} "
                "index_read_us={} batch_sizes=[{}]",
                state->segment_id,
                state->query_timestamp,
                worker,
                p.calls,
                p.input_rows,
                p.active_rows,
                p.accepted_rows,
                p.errors,
                p.prepare_ns / 1000.0,
                p.execute_ns / 1000.0,
                p.raw_path_rows,
                p.index_path_rows,
                p.raw_path_ns / 1000.0,
                p.index_path_ns / 1000.0,
                p.raw_read_rows,
                p.index_read_rows,
                p.raw_read_ns / 1000.0,
                p.index_read_ns / 1000.0,
                sizes.str());
        } catch (...) {
            // Telemetry must not turn worker destruction into a query failure.
        }
    }
}

Status
OffsetExpressionCallback::EvalBatch(void* worker,
                                    const int32_t* row_ids,
                                    uint32_t count,
                                    uint64_t active_mask,
                                    uint64_t* accepted_mask) noexcept {
    auto* profile =
        worker ? static_cast<Worker*>(worker)->profile.get() : nullptr;
    if (accepted_mask == nullptr) {
        if (profile)
            ++profile->errors;
        return Status::InvalidArgument;
    }
    *accepted_mask = 0;
    if (worker == nullptr || count > 64 || (count != 0 && row_ids == nullptr)) {
        if (profile)
            ++profile->errors;
        return Status::InvalidArgument;
    }
    const uint64_t lanes =
        count == 64 ? ~uint64_t{0} : (uint64_t{1} << count) - 1;
    if ((active_mask & ~lanes) != 0) {
        if (profile)
            ++profile->errors;
        return Status::InvalidArgument;
    }
    auto& state = *static_cast<Worker*>(worker);
    FilterDiagnosticScope profile_scope(profile);
    FilterDiagnosticTimer elapsed(profile ? &profile->execute_ns : nullptr);
    if (profile) {
        ++profile->calls;
        ++profile->batch_sizes[count];
        profile->input_rows += count;
        profile->active_rows += __builtin_popcountll(active_mask);
    }
    for (uint32_t lane = 0; lane < count; ++lane) {
        if ((active_mask & (uint64_t{1} << lane)) != 0 &&
            (row_ids[lane] < 0 || row_ids[lane] >= state.row_count)) {
            if (profile)
                ++profile->errors;
            return Status::InvalidArgument;
        }
    }
    try {
        *accepted_mask =
            state.workspace->EvalAcceptedBatch(row_ids, count, active_mask);
        if (profile)
            profile->accepted_rows += __builtin_popcountll(*accepted_mask);
        elapsed
            .Stop();  // Exclude diagnostic formatting/output from callback time.
        LOG_DEBUG("ann_fusing callback batch count={} active={} accepted={}",
                  count,
                  active_mask,
                  *accepted_mask);
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
        if (profile)
            ++profile->errors;
        return Status::Failed;
    }
}

}  // namespace milvus::exec
