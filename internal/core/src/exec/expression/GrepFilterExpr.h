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
#include <optional>
#include <string>
#include <vector>

#include <folly/CancellationToken.h>

#include "common/FieldMeta.h"
#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "grep/Grep.h"
#include "grep/SegmentGrepReader.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

// grep(field, "rg-style arguments") as a filter predicate.
//
// One implementation serves both evaluation modes of the expression framework:
//   - batch evaluation (filter node): consecutive row ranges driven by this
//     expression's own cursor; only rows set in bitmap_input are refined;
//   - offset evaluation (limit-bounded Query evaluation, iterative filter,
//     scorer filters): the rows named by EvalCtx's offset input.
//
// Refinement reads the field text and runs the ripgrep-based matcher with
// "stop after the first matching line", since only the row verdict is needed.
//
// NGRAM use. The index can only produce the candidate bitmap of the whole
// segment in one call, at a cost independent of how many rows the caller still
// needs. The bitmap is therefore materialized lazily: this expression counts
// the rows and bytes it refined without it and queries the index once when a
// threshold would be exceeded. A caller satisfied by the first few rows never
// pays for the index. The byte threshold needs value lengths before matching,
// which borrowed VARCHAR views provide; for TEXT and for VARCHAR without views
// the bitmap is materialized before the first refinement.
class PhyGrepFilterExpr : public Expr {
 public:
    // Rows and bytes that may be refined before the NGRAM bitmap is built.
    static constexpr int64_t kLazyIndexRows = 256;
    static constexpr size_t kLazyIndexBytes = 256 * 1024;

    PhyGrepFilterExpr(
        std::vector<ExprPtr>&& input,
        const std::shared_ptr<const milvus::expr::GrepExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        std::shared_ptr<const segcore::SegmentReadSnapshot> snapshot);

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    // Batch mode only: a conjunction that short-circuits a batch still has to
    // keep this expression's cursor aligned with its siblings.
    void
    MoveCursor() override {
        current_pos_ += NextBatchSize();
    }

    bool
    SupportOffsetInput() override {
        return true;
    }

    std::string
    ToString() const override {
        return expr_->ToString();
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    // With a null-rejecting consumer a NULL row may be reported as FALSE, so
    // rows the NGRAM bitmap excludes need no validity lookup.
    void
    MarkNullRejecting() override {
        null_rejecting_ = true;
    }

    // Observability for tests: how the lazy NGRAM rule behaved.
    int64_t
    IndexQueryCount() const {
        return index_queries_;
    }

    int64_t
    RefinedRowCount() const {
        return refined_rows_;
    }

 private:
    int64_t
    NextBatchSize() const {
        auto remain = active_count_ - current_pos_;
        return remain < batch_size_ ? remain : batch_size_;
    }

    // Compiles the matcher and resolves readers on first use, so an expression
    // whose rows are all pruned by its siblings costs nothing.
    void
    Prepare();

    void
    MaterializeCandidates();

    // Refines `offsets` (segment row offsets) and writes the verdict of
    // offsets[i] to bit positions[i] of res/valid.
    void
    EvalRows(std::vector<int64_t>& offsets,
             std::vector<int64_t>& positions,
             TargetBitmap& res,
             TargetBitmap& valid);

    std::shared_ptr<const milvus::expr::GrepExpr> expr_;
    const segcore::SegmentInternalInterface* segment_;
    int64_t active_count_;
    int64_t batch_size_;
    std::shared_ptr<const segcore::SegmentReadSnapshot> snapshot_;
    int64_t current_pos_{0};
    bool null_rejecting_{false};

    // Filled by Prepare().
    bool prepared_{false};
    std::optional<FieldMeta> meta_;
    std::optional<grep::Session> session_;
    std::unique_ptr<folly::CancellationCallback> cancel_callback_;
    int64_t rows_{0};
    int64_t read_batch_size_{128};
    bool use_views_{false};
    const ChunkedColumnInterface* read_column_{nullptr};

    // NGRAM state: a prepared reader not yet queried, or its candidate bitmap.
    std::optional<grep::GrepIndex> index_;
    std::optional<TargetBitmap> candidates_;
    // True when the byte threshold can be checked before matching.
    bool lazy_index_{false};
    int64_t raw_rows_{0};
    size_t raw_bytes_{0};

    int64_t index_queries_{0};
    int64_t refined_rows_{0};

    // Reused across Eval calls.
    std::vector<int64_t> offsets_;
    std::vector<int64_t> positions_;
};

}  // namespace exec
}  // namespace milvus
