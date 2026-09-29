// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"

namespace milvus::exec {

// Evaluate 10/20 distinct original offsets from one real scalar chunk through
// the same Expr offset evaluator used by candidate filtering. Only the ratio
// escapes; no dense bitmap or sampled-row cache is created.
std::optional<double>
SampleOffsetFilterRatio(const expr::TypedExprPtr& expression,
                        ExecContext* exec_context);

// SQL predicate truth for at most 64 input offsets.  A lane is accepted only
// when it is present in both true_mask and known_mask; UNKNOWN/NULL therefore
// remains distinguishable inside Milvus and can be folded to false only at the
// final search-filter boundary.
struct OffsetExpressionTruth {
    uint64_t true_mask{0};
    uint64_t known_mask{0};

    uint64_t
    accepted_mask() const {
        return true_mask & known_mask;
    }
};

// Mutable physical-expression state belongs to one evaluation worker.  This
// is deliberately a Milvus type: the later Knowhere boundary receives only an
// opaque C callback and never sees ExprSet, EvalCtx or expression operators.
class OffsetExpressionWorkspace final {
 public:
    OffsetExpressionWorkspace(expr::TypedExprPtr expression,
                              ExecContext* exec_context,
                              bool null_rejecting);

    bool
    SupportsOffsetInput() const;

    // Native Milvus offset evaluation entry. It preserves the
    // existing OffsetVector -> ColumnVector path without repacking or a
    // 64-lane limit. Under null rejection only data & validity is guaranteed;
    // FALSE and UNKNOWN need not remain distinguishable.
    std::shared_ptr<ColumnVector>
    EvalOffsets(OffsetVector& row_ids);

    // Evaluate the active lanes in row_ids.  Inactive lanes are not read, so a
    // caller may leave their IDs unspecified.  count is limited to 64 to map
    // directly to the eventual callback ABI. This exact SQL truth entry is
    // only available when the workspace was prepared without null rejection.
    OffsetExpressionTruth
    EvalTruthBatch(const int32_t* row_ids,
                   uint32_t count,
                   uint64_t active_mask);

    // Root-level filter result: bit i is set iff active input i is TRUE.
    // FALSE and UNKNOWN are both rejected. Supports null-rejecting execution.
    uint64_t
    EvalAcceptedBatch(const int32_t* row_ids,
                      uint32_t count,
                      uint64_t active_mask);

    ExprSet&
    expr_set() {
        return expr_set_;
    }

 private:
    OffsetExpressionTruth
    EvalBatchImpl(const int32_t* row_ids, uint32_t count, uint64_t active_mask);

    ExecContext* exec_context_;
    bool null_rejecting_;
    ExprSet expr_set_;
    OffsetVector compact_offsets_;
    std::vector<uint32_t> compact_to_lane_;
    std::vector<VectorPtr> results_;
};

// Immutable query-scoped description.  Each graph/iterator worker asks it for
// a private workspace, avoiding concurrent access to ExprSet cursor/cache
// state while keeping all predicate semantics in the original Milvus Expr
// implementations. This description BORROWS ExecContext and its query/segment;
// the caller must keep them alive until every workspace is destroyed. Creating
// private ExprSets does not make shared query/segment state thread-safe by itself.
class PreparedOffsetExpressionEvaluator final {
 public:
    PreparedOffsetExpressionEvaluator(expr::TypedExprPtr expression,
                                      ExecContext* exec_context,
                                      bool null_rejecting = false);

    std::unique_ptr<OffsetExpressionWorkspace>
    CreateWorkspace() const;

 private:
    // One root; AND/OR/NOT composition belongs inside this expression tree.
    expr::TypedExprPtr expression_;
    ExecContext* exec_context_;
    bool null_rejecting_;
};

}  // namespace milvus::exec
