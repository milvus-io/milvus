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

#include <fmt/core.h>

#include "common/EasyAssert.h"
#include "common/OpContext.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "segcore/SegmentInterface.h"

namespace milvus {
namespace exec {

class PhyMatchFilterExpr : public Expr {
 public:
    PhyMatchFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::MatchExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size)
        : Expr(DataType::BOOL, std::move(input), name, op_ctx),
          expr_(expr),
          segment_(segment),
          active_count_(active_count),
          batch_size_(batch_size) {
        size_per_chunk_ = segment_->size_per_chunk();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    MoveCursor() override {
        if (!has_offset_input_) {
            int64_t real_batch_size =
                current_pos_ + batch_size_ >= active_count_
                    ? active_count_ - current_pos_
                    : batch_size_;
            current_pos_ += real_batch_size;
            for (auto& input : inputs_) {
                input->MoveCursor();
            }
        }
    }

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    bool
    IsSource() const override {
        return true;
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return std::nullopt;
    }

    bool
    CanExecuteAllAtOnce() const override {
        return false;
    }

 private:
    // Shared element -> row aggregation. Works for any source of ArrayOffsets:
    //   - plain array / struct array: segment_->GetArrayOffsets(field_id)
    //   - JSON with path index (future): index-owned ArrayOffsets
    // The predicate (inputs_[0]) must produce an element-level bitset aligned
    // with the supplied ArrayOffsets.
    void
    EvalWithOffsets(EvalCtx& context,
                    VectorPtr& result,
                    std::shared_ptr<const milvus::IArrayOffsets> array_offsets,
                    FieldId field_id);

    // JSON path entry point. Tries the element-level nested-index fast path
    // (EvalJsonIndexed); on any incompatibility it falls back to the brute-force
    // per-row path (EvalJsonBrute), which stays the source of truth.
    void
    EvalJson(EvalCtx& context, VectorPtr& result);

    // JSON element-level nested-index fast path. When a real element-granular
    // ScalarIndex<T> exists over the queried JSON array path (a
    // JsonNestedIndexWrapper, cast_type ARRAY_*), the element predicate is served
    // by the index (In/Range) instead of re-parsing every JSON row, then the
    // element-level hits are folded to rows through the JSON IArrayOffsets exactly
    // like EvalWithOffsets does for real arrays.
    //
    // Returns true iff it fully produced `result` for this batch. Returns false
    // (leaving `result` untouched) whenever the query is not eligible -- no
    // sealed segment, no pinnable nested ARRAY-cast index, numeric array-index
    // path segments tantivy cannot serve, no JSON IArrayOffsets, an element
    // predicate the index cannot serve (arith / like / conjunct / binary-range /
    // variable-IN), an int64 literal that is not injective in the double index,
    // or an index whose element enumeration does not align element-for-element
    // with the offsets table (any null / type-mismatched array element makes the
    // two diverge). The caller then runs EvalJsonBrute for bit-for-bit parity.
    bool
    EvalJsonIndexed(EvalCtx& context, VectorPtr& result);

    // Typed body of EvalJsonIndexed: pins the nested ScalarIndex<T>, runs the
    // element predicate through it (In / NotIn / Range) to get an element-level
    // bitmap sized to the total indexed element count, verifies that count equals
    // the JSON IArrayOffsets total (the alignment guard), folds element -> row
    // with MATCH semantics, and applies NULL/non-array masking. Returns false to
    // request brute fallback.
    template <typename T>
    bool
    EvalJsonIndexedImpl(EvalCtx& context,
                        VectorPtr& result,
                        const std::string& index_pointer,
                        std::shared_ptr<const milvus::IArrayOffsets> array_offsets,
                        const milvus::expr::ColumnInfo& column,
                        milvus::proto::plan::OpType op_type,
                        const std::vector<milvus::proto::plan::GenericValue>&
                            values);

    // JSON brute-force: iterate JSON rows one at a time, evaluate the inner
    // predicate through the normal Eval framework, then aggregate that row's
    // element-level bitmap with MATCH semantics.
    void
    EvalJsonBrute(EvalCtx& context, VectorPtr& result);

    // Fold an element-level match/valid bitmap through `array_offsets` with the
    // MATCH quantifier for the current batch, writing the row-level result into
    // `col_vec`. Shared by EvalWithOffsets-style aggregation and the JSON indexed
    // path. `match_view` / `valid_view` are element-level and cover the whole
    // segment (element ids [0, total_elements)); the batch slice is taken here.
    void
    FoldElementBitsetToRows(ColumnVector* col_vec,
                            const OffsetVector* input,
                            int64_t batch_rows,
                            const milvus::IArrayOffsets* array_offsets,
                            const TargetBitmapView& match_view,
                            const TargetBitmapView& valid_view,
                            bool all_valid);

    // Three-valued MATCH: mask rows whose ARRAY field value is NULL. For each row
    // in the batch, clears both the valid bit and the match bit when the field is
    // NULL. Works for both sealed and growing segments (reads field validity via
    // segment_->ApplyFieldValidDataByOffsets). No-op for a non-nullable field.
    // Shared by the scalar-array and struct-array paths.
    void
    MaskNullRows(ColumnVector* col_vec,
                 FieldId field_id,
                 const OffsetVector* input,
                 int64_t batch_rows);

    // Three-valued MATCH for JSON: mask rows where the JSON path does NOT resolve
    // to a real JSON array (field NULL, JSON null at path, missing key, or a
    // non-array value at the path). Clears both the valid bit and the match bit
    // for such rows so they are excluded (UNKNOWN), while a genuine empty array
    // `[]` keeps its vacuous semantics.
    void
    MaskJsonNonArrayRows(ColumnVector* col_vec,
                         const OffsetVector* input,
                         int64_t batch_rows);

    std::shared_ptr<const milvus::expr::MatchExpr> expr_;
    const segcore::SegmentInternalInterface* segment_;
    int64_t active_count_;
    int64_t current_pos_{0};
    int64_t batch_size_;
    int64_t size_per_chunk_;
};

}  // namespace exec
}  // namespace milvus
