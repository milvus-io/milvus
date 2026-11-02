// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#pragma once

#include <memory>
#include <vector>

#include "exec/expression/Expr.h"

namespace milvus::exec {

struct OffsetExpressionOptions {
    bool null_rejecting{false};
};

// One existing ExprSet per task, reused for successive offset batches. Reading
// and predicate evaluation stay in the original Expr implementations. Inputs
// preserve order/duplicates and are not limited to a transport mask width.
class OffsetExpressionWorkspace final {
 public:
    OffsetExpressionWorkspace(expr::TypedExprPtr expression,
                              ExecContext* exec_context,
                              OffsetExpressionOptions options = {});

    // ExprSet and its read state are mutable task-local objects. Copying the
    // shared_ptr members would alias that state rather than create a worker.
    OffsetExpressionWorkspace(const OffsetExpressionWorkspace&) = delete;
    OffsetExpressionWorkspace&
    operator=(const OffsetExpressionWorkspace&) = delete;
    OffsetExpressionWorkspace(OffsetExpressionWorkspace&&) = delete;
    OffsetExpressionWorkspace&
    operator=(OffsetExpressionWorkspace&&) = delete;

    bool
    SupportsOffsetInput() const;

    std::shared_ptr<ColumnVector>
    EvalOffsets(OffsetVector& offsets);

 private:
    ExecContext* exec_context_;
    ExprSet expr_set_;
    bool supports_offset_input_{false};
    std::vector<VectorPtr> results_;
};

// Immutable factory description: owns only the logical expression and options,
// not a query/segment or mutable execution context. It may be shared by tasks.
// Each caller supplies its private ExecContext when creating a workspace;
// that workspace BORROWS the context/query/segment until its destruction.
// Supplying one mutable context to several tasks is not made thread-safe here.
class PreparedOffsetExpressionEvaluator final {
 public:
    PreparedOffsetExpressionEvaluator(expr::TypedExprPtr expression,
                                      OffsetExpressionOptions options = {});

    std::unique_ptr<OffsetExpressionWorkspace>
    CreateWorkspace(ExecContext* exec_context) const;

 private:
    expr::TypedExprPtr expression_;
    OffsetExpressionOptions options_;
};
}  // namespace milvus::exec
