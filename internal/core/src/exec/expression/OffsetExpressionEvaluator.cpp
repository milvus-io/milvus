// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include "exec/expression/OffsetExpressionEvaluator.h"

#include <algorithm>
#include <utility>

namespace milvus::exec {
namespace {
std::vector<expr::TypedExprPtr>
CheckedRoot(expr::TypedExprPtr expression, ExecContext* context) {
    AssertInfo(expression != nullptr && context != nullptr,
               "offset evaluator requires a root and execution context");
    return {std::move(expression)};
}
}  // namespace

OffsetExpressionWorkspace::OffsetExpressionWorkspace(
    expr::TypedExprPtr expression,
    ExecContext* exec_context,
    OffsetExpressionOptions options)
    : exec_context_(exec_context),
      take_context_(std::make_shared<TakeContext>(options.take)),
      expr_set_(CheckedRoot(std::move(expression), exec_context),
                exec_context,
                options.null_rejecting) {
    for (const auto& root : expr_set_.exprs()) {
        root->SetOffsetTakeContext(take_context_);
    }
}

bool
OffsetExpressionWorkspace::SupportsOffsetInput() const {
    return std::all_of(expr_set_.exprs().begin(),
                       expr_set_.exprs().end(),
                       [](const auto& expression) {
                           return expression &&
                                  expression->SupportOffsetInput();
                       });
}

std::shared_ptr<ColumnVector>
OffsetExpressionWorkspace::EvalOffsets(OffsetVector& offsets) {
    AssertInfo(SupportsOffsetInput(),
               "expression does not support positional evaluation");
    if (offsets.empty()) {
        // Many existing kernels intentionally require a nonempty evaluation
        // window. An empty transport batch must not enter those kernels.
        return std::make_shared<ColumnVector>(TargetBitmap{}, TargetBitmap{});
    }
    EvalCtx context(exec_context_, &offsets);
    results_.clear();
    expr_set_.Eval(0, 1, true, context, results_);
    AssertInfo(results_.size() == 1 && results_[0] != nullptr,
               "offset expression must produce exactly one result");
    auto result = std::dynamic_pointer_cast<ColumnVector>(results_[0]);
    AssertInfo(result != nullptr && result->IsBitmap() &&
                   result->size() == offsets.size(),
               "offset expression result must match input lanes");
    return result;
}

PreparedOffsetExpressionEvaluator::PreparedOffsetExpressionEvaluator(
    expr::TypedExprPtr expression, OffsetExpressionOptions options)
    : expression_(std::move(expression)), options_(options) {
    AssertInfo(expression_ != nullptr,
               "offset evaluator requires a logical root");
}

std::unique_ptr<OffsetExpressionWorkspace>
PreparedOffsetExpressionEvaluator::CreateWorkspace(
    ExecContext* exec_context) const {
    return std::make_unique<OffsetExpressionWorkspace>(
        expression_, exec_context, options_);
}
}  // namespace milvus::exec
