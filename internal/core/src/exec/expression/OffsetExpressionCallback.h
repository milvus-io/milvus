// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include "exec/expression/OffsetExpressionEvaluator.h"
#include "knowhere/candidate_evaluator.h"

namespace milvus::exec {

// Query-owned callback factory. Only adapts IDs/masks/ownership: all expression
// semantics remain in the shared offset evaluator used by iterative filtering.
// This object and the borrowed ExecContext/segment must outlive every worker.
class OffsetExpressionCallback final {
 public:
    OffsetExpressionCallback(expr::TypedExprPtr expression,
                             ExecContext* exec_context,
                             int64_t row_count);

    knowhere::CandidateEvaluatorViewV1
    view() const;

 private:
    struct Worker;

    static knowhere::CandidateEvalStatus
    CreateWorker(const void* context, void** output) noexcept;
    static void
    DestroyWorker(void* worker) noexcept;
    static knowhere::CandidateEvalStatus
    EvalBatch(void* worker,
              const int32_t* row_ids,
              uint32_t count,
              uint64_t active_mask,
              uint64_t* accepted_mask) noexcept;

    PreparedOffsetExpressionEvaluator prepared_;
    int64_t row_count_;
    // Opt-in diagnostic only; never enable while measuring search latency.
    bool debug_trace_offsets_{false};
    bool debug_profile_{false};
    int64_t segment_id_{0};
    uint64_t query_timestamp_{0};
};

}  // namespace milvus::exec
