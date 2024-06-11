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
#include <string>

#include "common/Types.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/VectorFunction.h"
#include "exec/expression/Utils.h"
#include "exec/QueryContext.h"
#include "expr/ITypeExpr.h"
#include "query/PlanProto.h"

namespace milvus {
namespace exec {

class Expr {
 public:
    Expr(DataType type,
         const std::vector<std::shared_ptr<Expr>>&& inputs,
         const std::string& name)
        : type_(type),
          inputs_(std::move(inputs)),
          name_(name),
          vector_func_(nullptr) {
    }

    Expr(DataType type,
         const std::vector<std::shared_ptr<Expr>>&& inputs,
         std::shared_ptr<VectorFunction> vec_func,
         const std::string& name)
        : type_(type),
          inputs_(std::move(inputs)),
          name_(name),
          vector_func_(vec_func) {
    }
    virtual ~Expr() = default;

    const DataType&
    type() const {
        return type_;
    }

    std::string
    get_name() {
        return name_;
    }

    virtual void
    Eval(EvalCtx& context, VectorPtr& result) {
    }

    // Only move cursor to next batch
    // but not do real eval for optimization
    virtual void
    MoveCursor() {
    }

 protected:
    DataType type_;
    const std::vector<std::shared_ptr<Expr>> inputs_;
    std::string name_;
    std::shared_ptr<VectorFunction> vector_func_;
};

using ExprPtr = std::shared_ptr<milvus::exec::Expr>;

using SkipFunc = bool (*)(const milvus::SkipIndex&, FieldId, int);

class SegmentExpr : public Expr {
 public:
    SegmentExpr(const std::vector<ExprPtr>&& input,
                const std::string& name,
                const segcore::SegmentInternalInterface* segment,
                const FieldId& field_id,
                int64_t active_count,
                int64_t batch_size)
        : Expr(DataType::BOOL, std::move(input), name),
          segment_(segment),
          field_id_(field_id),
          active_count_(active_count),
          batch_size_(batch_size) {
        size_per_chunk_ = segment_->size_per_chunk();
        AssertInfo(
            batch_size_ > 0,
            fmt::format("expr batch size should greater than zero, but now: {}",
                        batch_size_));
        InitSegmentExpr();
    }

    void
    InitSegmentExpr() {
        auto& schema = segment_->get_schema();
        auto& field_meta = schema[field_id_];

        if (schema.get_primary_field_id().has_value() &&
            schema.get_primary_field_id().value() == field_id_ &&
            IsPrimaryKeyDataType(field_meta.get_data_type())) {
            is_pk_field_ = true;
            pk_type_ = field_meta.get_data_type();
        }

        is_index_mode_ = segment_->HasIndex(field_id_);
        if (is_index_mode_) {
            num_index_chunk_ = segment_->num_chunk_index(field_id_);
        } else {
            num_data_chunk_ = upper_div(active_count_, size_per_chunk_);
        }
    }

    void
    MoveCursorForData() {
        if (segment_->type() == SegmentType::Sealed) {
            auto size =
                std::min(active_count_ - current_data_chunk_pos_, batch_size_);
            current_data_chunk_pos_ += size;
        } else {
            int64_t processed_size = 0;
            for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
                auto data_pos =
                    (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;
                auto size = (i == (num_data_chunk_ - 1) &&
                             active_count_ % size_per_chunk_ != 0)
                                ? active_count_ % size_per_chunk_ - data_pos
                                : size_per_chunk_ - data_pos;

                size = std::min(size, batch_size_ - processed_size);

                processed_size += size;
                if (processed_size >= batch_size_) {
                    current_data_chunk_ = i;
                    current_data_chunk_pos_ = data_pos + size;
                    break;
                }
            }
        }
    }

    void
    MoveCursorForIndex() {
        AssertInfo(segment_->type() == SegmentType::Sealed,
                   "index mode only for sealed segment");
        auto size =
            std::min(active_count_ - current_index_chunk_pos_, batch_size_);

        current_index_chunk_pos_ += size;
    }

    void
    MoveCursor() override {
        if (is_index_mode_) {
            MoveCursorForIndex();
        } else {
            MoveCursorForData();
        }
    }

    int64_t
    GetNextBatchSize() {
        auto current_chunk =
            is_index_mode_ ? current_index_chunk_ : current_data_chunk_;
        auto current_chunk_pos =
            is_index_mode_ ? current_index_chunk_pos_ : current_data_chunk_pos_;
        auto current_rows = current_chunk * size_per_chunk_ + current_chunk_pos;
        return current_rows + batch_size_ >= active_count_
                   ? active_count_ - current_rows
                   : batch_size_;
    }

    template <typename T, typename FUNC, typename... ValTypes>
    int64_t
    ProcessDataChunks(
        FUNC func,
        std::function<bool(const milvus::SkipIndex&, FieldId, int)> skip_func,
        TargetBitmapView res,
        ValTypes... values) {
        int64_t processed_size = 0;

        for (size_t i = current_data_chunk_; i < num_data_chunk_; i++) {
            auto data_pos =
                (i == current_data_chunk_) ? current_data_chunk_pos_ : 0;
            auto size =
                (i == (num_data_chunk_ - 1))
                    ? (segment_->type() == SegmentType::Growing
                           ? (active_count_ % size_per_chunk_ == 0
                                  ? size_per_chunk_ - data_pos
                                  : active_count_ % size_per_chunk_ - data_pos)
                           : active_count_ - data_pos)
                    : size_per_chunk_ - data_pos;

            size = std::min(size, batch_size_ - processed_size);

            auto& skip_index = segment_->GetSkipIndex();
            if (!skip_func || !skip_func(skip_index, field_id_, i)) {
                auto chunk = segment_->chunk_data<T>(field_id_, i);
                const T* data = chunk.data() + data_pos;
                func(data, size, res + processed_size, values...);
            }

            processed_size += size;
            if (processed_size >= batch_size_) {
                current_data_chunk_ = i;
                current_data_chunk_pos_ = data_pos + size;
                break;
            }
        }

        return processed_size;
    }

    int
    ProcessIndexOneChunk(TargetBitmap& result,
                         size_t chunk_id,
                         const TargetBitmap& chunk_res,
                         int processed_rows) {
        auto data_pos =
            chunk_id == current_index_chunk_ ? current_index_chunk_pos_ : 0;
        auto size = std::min(
            std::min(size_per_chunk_ - data_pos, batch_size_ - processed_rows),
            int64_t(chunk_res.size()));

        //        result.insert(result.end(),
        //                      chunk_res.begin() + data_pos,
        //                      chunk_res.begin() + data_pos + size);
        result.append(chunk_res, data_pos, size);
        return size;
    }

    template <typename T, typename FUNC, typename... ValTypes>
    TargetBitmap
    ProcessIndexChunks(FUNC func, ValTypes... values) {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;
        TargetBitmap result;
        int processed_rows = 0;

        for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
            // This cache result help getting result for every batch loop.
            // It avoids indexing execute for evevy batch because indexing
            // executing costs quite much time.
            if (cached_index_chunk_id_ != i) {
                const Index& index =
                    segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);
                auto* index_ptr = const_cast<Index*>(&index);
                cached_index_chunk_res_ = std::move(func(index_ptr, values...));
                cached_index_chunk_id_ = i;
            }

            auto size = ProcessIndexOneChunk(
                result, i, cached_index_chunk_res_, processed_rows);

            if (processed_rows + size >= batch_size_) {
                current_index_chunk_ = i;
                current_index_chunk_pos_ = i == current_index_chunk_
                                               ? current_index_chunk_pos_ + size
                                               : size;
                break;
            }
            processed_rows += size;
        }

        return result;
    }

    template <typename T, typename FUNC, typename... ValTypes>
    void
    ProcessIndexChunksV2(FUNC func, ValTypes... values) {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        using Index = index::ScalarIndex<IndexInnerType>;

        for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
            const Index& index =
                segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);
            auto* index_ptr = const_cast<Index*>(&index);
            func(index_ptr, values...);
        }
    }

    template <typename T>
    bool
    CanUseIndex(OpType op) const {
        typedef std::
            conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
                IndexInnerType;
        if constexpr (!std::is_same_v<IndexInnerType, std::string>) {
            return true;
        }

        using Index = index::ScalarIndex<IndexInnerType>;
        if (op == OpType::Match) {
            for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
                const Index& index =
                    segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);
                // 1, index support regex query, then index handles the query;
                // 2, index has raw data, then call index.Reverse_Lookup to handle the query;
                if (!index.SupportRegexQuery() && !index.HasRawData()) {
                    return false;
                }
                // all chunks have same index.
                return true;
            }
        }

        return true;
    }

 protected:
    const segcore::SegmentInternalInterface* segment_;
    const FieldId field_id_;
    bool is_pk_field_{false};
    DataType pk_type_;
    int64_t batch_size_;

    // State indicate position that expr computing at
    // because expr maybe called for every batch.
    bool is_index_mode_{false};
    bool is_data_mode_{false};

    int64_t active_count_{0};
    int64_t num_data_chunk_{0};
    int64_t num_index_chunk_{0};
    int64_t current_data_chunk_{0};
    int64_t current_data_chunk_pos_{0};
    int64_t current_index_chunk_{0};
    int64_t current_index_chunk_pos_{0};
    int64_t size_per_chunk_{0};

    // Cache for index scan to avoid search index every batch
    int64_t cached_index_chunk_id_{-1};
    TargetBitmap cached_index_chunk_res_{};
};

void
OptimizeCompiledExprs(ExecContext* context, const std::vector<ExprPtr>& exprs);

std::vector<ExprPtr>
CompileExpressions(const std::vector<expr::TypedExprPtr>& logical_exprs,
                   ExecContext* context,
                   const std::unordered_set<std::string>& flatten_cadidates =
                       std::unordered_set<std::string>(),
                   bool enable_constant_folding = false);

std::vector<ExprPtr>
CompileInputs(const expr::TypedExprPtr& expr,
              QueryContext* config,
              const std::unordered_set<std::string>& flatten_cadidates);

ExprPtr
CompileExpression(const expr::TypedExprPtr& expr,
                  QueryContext* context,
                  const std::unordered_set<std::string>& flatten_cadidates,
                  bool enable_constant_folding);

class ExprSet {
 public:
    explicit ExprSet(const std::vector<expr::TypedExprPtr>& logical_exprs,
                     ExecContext* exec_ctx) {
        exprs_ = CompileExpressions(logical_exprs, exec_ctx);
    }

    virtual ~ExprSet() = default;

    void
    Eval(EvalCtx& ctx, std::vector<VectorPtr>& results) {
        Eval(0, exprs_.size(), true, ctx, results);
    }

    virtual void
    Eval(int32_t begin,
         int32_t end,
         bool initialize,
         EvalCtx& ctx,
         std::vector<VectorPtr>& result);

    void
    Clear() {
        exprs_.clear();
    }

    ExecContext*
    get_exec_context() const {
        return exec_ctx_;
    }

    size_t
    size() const {
        return exprs_.size();
    }

    const std::vector<std::shared_ptr<Expr>>&
    exprs() const {
        return exprs_;
    }

    const std::shared_ptr<Expr>&
    expr(int32_t index) const {
        return exprs_[index];
    }

 private:
    std::vector<std::shared_ptr<Expr>> exprs_;
    ExecContext* exec_ctx_;
};

}  //namespace exec
}  // namespace milvus