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

#include <any>
#include <cmath>
#include <functional>
#include <string_view>
#include <utility>
#include <fmt/core.h>

#include "common/Array.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "index/SkipIndex.h"
#include "segcore/SegmentInterface.h"
#include "index/json_stats/bson_inverted.h"
#include "cachinglayer/CacheSlot.h"

namespace milvus {
namespace exec {

template <typename T>
struct TermIndexFunc {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    TargetBitmap
    operator()(Index* index, size_t n, const IndexInnerType* val) {
        return index->In(n, val);
    }
};

using TermFilterChunkFn =
    std::function<void(const void* data, int size, TargetBitmapView res)>;

// IN / NOT IN over a scalar column, or over ARRAY elements when element-level.
template <typename T>
struct TermScalarKernel {
    milvus::OpContext* op_ctx;
    const MultiElement* vals;
    // Batch SIMD filter over vals; nullptr or empty when vals is not a
    // SimdBatchElement<T>. It only sets bits, so it relies on match == 0.
    const TermFilterChunkFn* simd_filter_fn;
    // vals as SetElement<std::string>, or nullptr.
    const SetElement<std::string>* str_set_elem;
    // std::vector<T> of IN values for SkipIndex, or an empty std::any.
    const std::any* skip_elements;

    bool
    CanSkip(const SkipIndex& skip_index,
            FieldId field_id,
            int64_t chunk_id) const {
        const auto* elements = std::any_cast<std::vector<T>>(skip_elements);
        if (elements == nullptr) {
            return false;
        }
        return skip_index.CanSkipInQuery<T>(
            op_ctx, field_id, chunk_id, *elements);
    }

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<T>& b, TriStateOut out) const {
        if constexpr (filter_type == FilterType::sequential) {
            if (simd_filter_fn != nullptr && *simd_filter_fn) {
                (*simd_filter_fn)(b.data, static_cast<int>(b.size), out.match);
                return;
            }
        }
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (b.validity && !b.validity[i]) {
                continue;
            }
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            bool hit;
            if constexpr (std::is_same_v<T, std::string> ||
                          std::is_same_v<T, std::string_view>) {
                if (str_set_elem != nullptr) {
                    hit = str_set_elem->values_.find(std::string_view(
                              b.data[i])) != str_set_elem->values_.end();
                } else {
                    hit = vals->In(
                        MultiElement::ValueType(std::string_view(b.data[i])));
                }
            } else {
                hit = vals->In(
                    MultiElement::ValueType(std::in_place_type<T>, b.data[i]));
            }
            if (hit) {
                out.SetTrue(i);
            }
        }
    }
};

static_assert(KernelCanSkip<TermScalarKernel<int64_t>>);

// `value in array_field`
template <typename ValueType>
struct TermArrayVariableInFieldKernel {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    ValueType target_val;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::ArrayView>& b, TriStateOut out) const {
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (b.validity && !b.validity[i]) {
                continue;
            }
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            const auto& row = b.data[i];
            for (int j = 0; j < row.length(); ++j) {
                if (row.template get_data<GetType>(j) == target_val) {
                    out.SetTrue(i);
                    break;
                }
            }
        }
    }
};

// `array_field[index] in [...]`
template <typename ValueType>
struct TermArrayFieldInVariableKernel {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    int index;
    const MultiElement* term_set;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::ArrayView>& b, TriStateOut out) const {
        AssertInfo(index >= 0,
                   "array element term predicate requires nested path");
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (b.validity && !b.validity[i]) {
                continue;
            }
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            const auto& row = b.data[i];
            if (index >= row.length()) {
                out.SetUnknown(i);
                continue;
            }
            auto value = row.template get_data<GetType>(index);
            if (term_set->In(ValueType(value))) {
                out.SetTrue(i);
            }
        }
    }
};

// `value in json["path"]`
template <typename ValueType>
struct TermJsonVariableInFieldKernel {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    std::string_view pointer;
    ValueType target_val;

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (b.validity && !b.validity[i]) {
                continue;
            }
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            auto doc = b.data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error()) {
                out.SetUnknown(i);
                continue;
            }
            for (auto it = array.begin(); it != array.end(); ++it) {
                auto val = (*it).template get<GetType>();
                if (val.error()) {
                    continue;
                }
                if (val.value() == target_val) {
                    out.SetTrue(i);
                    break;
                }
            }
        }
    }
};

// `json["path"] in [...]`
template <typename ValueType>
struct TermJsonFieldInVariableKernel {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    std::string_view pointer;
    const MultiElement* terms;

    // (valid, matched); valid is false when the path is missing, null, or
    // holds a value of another JSON type.
    std::pair<bool, bool>
    Probe(const milvus::Json& row) const {
        if constexpr (std::is_same_v<GetType, std::int64_t>) {
            auto x_num = row.at_numeric(pointer);
            if (x_num.error()) {
                return {false, false};
            }
            auto n = x_num.value();
            if (n.is_int64()) {
                return {true, terms->In(ValueType(n.get_int64()))};
            }
            auto dval = n.is_uint64() ? static_cast<double>(n.get_uint64())
                                      : n.get_double();
            return {true,
                    std::floor(dval) == dval && terms->In(ValueType(dval))};
        } else {
            auto x = row.template at<GetType>(pointer);
            if (x.error()) {
                return {false, false};
            }
            return {true, terms->In(ValueType(x.value()))};
        }
    }

    template <FilterType filter_type>
    void
    Eval(const CandidateBatch<milvus::Json>& b, TriStateOut out) const {
        const bool has_candidates = !b.candidates.empty();
        for (size_t i = 0; i < b.size; ++i) {
            if (b.validity && !b.validity[i]) {
                continue;
            }
            if (has_candidates && !b.candidates[i]) {
                continue;
            }
            const auto [valid, matched] = Probe(b.data[i]);
            if (!valid) {
                out.SetUnknown(i);
                continue;
            }
            if (matched) {
                out.SetTrue(i);
            }
        }
    }
};

class PhyTermFilterExpr : public SegmentExpr {
 public:
    PhyTermFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::TermFilterExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        milvus::Timestamp timestamp,
        int64_t batch_size,
        int32_t consistency_level,
        const query::PlanOptions& plan_options = {})
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      expr->vals_.size() == 0
                          ? DataType::NONE
                          : FromValCase(expr->vals_[0].val_case()),
                      active_count,
                      batch_size,
                      consistency_level,
                      false,
                      false,
                      plan_options),
          expr_(expr) {
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    void
    DetermineExecPath() override;

    bool
    IsSource() const override {
        return true;
    }

    std::string
    ToString() const override {
        return fmt::format("{}", expr_->ToString());
    }

    std::optional<milvus::expr::ColumnInfo>
    GetColumnInfo() const override {
        return expr_->column_;
    }

    bool
    IsElementLevelExpression() const override {
        return expr_->column_.element_level_;
    }

    void
    PrefetchRawData() override;

    template <typename T>
    void
    PrefetchRawData();

 private:
    void
    InitPkCacheOffset();

    template <typename T>
    bool
    CanSkipSegment();

    VectorPtr
    ExecPkTermImpl();

    template <typename T>
    VectorPtr
    ExecVisitorImpl(EvalCtx& context);

    template <typename T>
    VectorPtr
    ExecVisitorImplForIndex();

    template <typename T>
    VectorPtr
    ExecVisitorImplForData(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecVisitorImplTemplateJson(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecTermJsonVariableInField(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecTermJsonFieldInVariable(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecVisitorImplTemplateArray(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecTermArrayVariableInField(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecTermArrayFieldInVariable(EvalCtx& context);

    template <typename ValueType>
    VectorPtr
    ExecJsonInVariableByStats();

    // `x in []`: FALSE and known for every row of the batch, NULL rows
    // included.
    VectorPtr
    EmptyTermBatch(EvalCtx& context);

 private:
    std::shared_ptr<const milvus::expr::TermFilterExpr> expr_;
    bool cached_bits_inited_{false};
    TargetBitmap cached_bits_;
    bool arg_inited_{false};
    std::shared_ptr<MultiElement> arg_set_;
    SingleElement arg_val_;
    PinWrapper<index::BsonInvertedIndex*> bson_index_{nullptr};

    // Type-safe cached FilterChunk dispatch (avoids per-call dynamic_cast).
    // Set once during arg_inited_; empty when arg_set_ is not SimdBatch.
    // Captures a typed SimdBatchElement<T>* inside the lambda at init time.
    using FilterChunkFn = TermFilterChunkFn;
    FilterChunkFn cached_filter_chunk_;
    // Cached SetElement<string> pointer for per-row string lookup without
    // variant construction. Set once during init; nullptr when arg_set_ is not
    // SetElement<string> (e.g. FlatVectorElement for small IN).
    SetElement<std::string>* cached_str_set_elem_{nullptr};
    // Cached element values for skip_index (avoids per-chunk vector copy).
    std::any cached_skip_elements_;
};
}  //namespace exec
}  // namespace milvus
