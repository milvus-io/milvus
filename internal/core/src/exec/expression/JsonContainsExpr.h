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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <utility>
#include <vector>

#include <fmt/core.h>

#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "exec/expression/JsonNumberComparison.h"
#include "segcore/SegmentInterface.h"
#include "common/bson_view.h"
#include "exec/expression/Utils.h"
#include "index/json_stats/bson_inverted.h"
#include "cachinglayer/CacheSlot.h"

namespace milvus {
namespace exec {

// Reusable per-row found state for ContainsAll. Candidate positions are kept
// instead of candidate values because equivalent literals such as 2 and 2.0
// must both be marked by a single matching JSON value.
class ContainsAllCandidateTracker {
 public:
    explicit ContainsAllCandidateTracker(size_t candidate_count)
        : candidate_count_(candidate_count),
          uses_inline_mask_(candidate_count <= 64),
          all_candidates_mask_(candidate_count == 64 ? ~uint64_t(0)
                               : candidate_count < 64
                                   ? (uint64_t(1) << candidate_count) - 1
                                   : 0),
          matched_candidate_words_(
              uses_inline_mask_ ? 0 : (candidate_count + 63) / 64) {
    }

    void
    Reset() {
        if (uses_inline_mask_) {
            matched_candidates_mask_ = 0;
        } else {
            std::fill(matched_candidate_words_.begin(),
                      matched_candidate_words_.end(),
                      0);
            remaining_candidate_count_ = candidate_count_;
        }
    }

    bool
    MarkCandidateMatched(size_t candidate_position) {
        if (uses_inline_mask_) {
            matched_candidates_mask_ |= uint64_t(1) << candidate_position;
            return matched_candidates_mask_ == all_candidates_mask_;
        }

        auto& word = matched_candidate_words_[candidate_position / 64];
        const auto bit = uint64_t(1) << (candidate_position % 64);
        if (!(word & bit)) {
            word |= bit;
            --remaining_candidate_count_;
        }
        return remaining_candidate_count_ == 0;
    }

    bool
    AllCandidatesMatched() const {
        return uses_inline_mask_
                   ? matched_candidates_mask_ == all_candidates_mask_
                   : remaining_candidate_count_ == 0;
    }

 private:
    size_t candidate_count_{0};
    bool uses_inline_mask_{true};
    uint64_t all_candidates_mask_{0};
    uint64_t matched_candidates_mask_{0};
    std::vector<uint64_t> matched_candidate_words_;
    size_t remaining_candidate_count_{0};
};

template <typename OnMatch>
void
VisitMatchingBsonCandidatePositions(
    const milvus::bson::value_view& value,
    const std::vector<proto::plan::GenericValue>& candidates,
    const JsonNumberCandidatePositionIndex& number_position_index,
    OnMatch&& on_match) {
    switch (value.type()) {
        case milvus::bson::type::k_int32:
        case milvus::bson::type::k_int64:
        case milvus::bson::type::k_double:
            number_position_index.VisitMatchingPositionsForBsonNumber(
                value, std::forward<OnMatch>(on_match));
            return;
        case milvus::bson::type::k_bool: {
            auto parsed = milvus::BsonView::GetValueFromBsonView<bool>(value);
            if (!parsed.has_value()) {
                return;
            }
            for (size_t i = 0; i < candidates.size(); ++i) {
                if (candidates[i].has_bool_val() &&
                    *parsed == candidates[i].bool_val() && on_match(i)) {
                    return;
                }
            }
            return;
        }
        case milvus::bson::type::k_string: {
            auto parsed =
                milvus::BsonView::GetValueFromBsonView<std::string>(value);
            if (!parsed.has_value()) {
                return;
            }
            for (size_t i = 0; i < candidates.size(); ++i) {
                if (candidates[i].has_string_val() &&
                    *parsed == candidates[i].string_val() && on_match(i)) {
                    return;
                }
            }
            return;
        }
        case milvus::bson::type::k_array: {
            auto parsed = milvus::BsonView::GetValueFromBsonView<
                milvus::bson::array_view>(value);
            if (!parsed.has_value()) {
                return;
            }
            for (size_t i = 0; i < candidates.size(); ++i) {
                if (candidates[i].has_array_val() &&
                    CompareTwoJsonArray(*parsed, candidates[i].array_val()) &&
                    on_match(i)) {
                    return;
                }
            }
            return;
        }
        default:
            return;
    }
}

inline bool
BsonValueMatchesAnyCandidate(
    const milvus::bson::value_view& value,
    const std::vector<proto::plan::GenericValue>& candidates,
    const JsonNumberMembershipMatcher& number_membership_matcher) {
    switch (value.type()) {
        case milvus::bson::type::k_int32:
        case milvus::bson::type::k_int64:
        case milvus::bson::type::k_double:
            return number_membership_matcher.MatchesAnyBsonNumber(value);
        case milvus::bson::type::k_bool: {
            auto parsed = milvus::BsonView::GetValueFromBsonView<bool>(value);
            if (!parsed.has_value()) {
                return false;
            }
            return std::any_of(candidates.begin(),
                               candidates.end(),
                               [&](const auto& candidate) {
                                   return candidate.has_bool_val() &&
                                          *parsed == candidate.bool_val();
                               });
        }
        case milvus::bson::type::k_string: {
            auto parsed =
                milvus::BsonView::GetValueFromBsonView<std::string>(value);
            if (!parsed.has_value()) {
                return false;
            }
            return std::any_of(candidates.begin(),
                               candidates.end(),
                               [&](const auto& candidate) {
                                   return candidate.has_string_val() &&
                                          *parsed == candidate.string_val();
                               });
        }
        case milvus::bson::type::k_array: {
            auto parsed = milvus::BsonView::GetValueFromBsonView<
                milvus::bson::array_view>(value);
            if (!parsed.has_value()) {
                return false;
            }
            return std::any_of(candidates.begin(),
                               candidates.end(),
                               [&](const auto& candidate) {
                                   return candidate.has_array_val() &&
                                          CompareTwoJsonArray(
                                              *parsed, candidate.array_val());
                               });
        }
        default:
            return false;
    }
}

class ShreddingArrayBsonContainsArrayExecutor {
 public:
    explicit ShreddingArrayBsonContainsArrayExecutor(
        const std::vector<proto::plan::Array>& elems)
        : elements_(elems) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& sub_value : array_view.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    milvus::bson::array_view>(sub_value.get_value());
                if (!sub_array.has_value())
                    continue;
                for (const auto& element : elements_) {
                    if (CompareTwoJsonArray(sub_array.value(), element)) {
                        matched = true;
                        break;
                    }
                }
                if (matched)
                    break;
            }
            res[i] = matched;
        }
    }

 private:
    const std::vector<proto::plan::Array> elements_;
};

class ShreddingArrayBsonContainsAllArrayExecutor {
 public:
    explicit ShreddingArrayBsonContainsAllArrayExecutor(
        const std::vector<proto::plan::Array>& elems)
        : elements_(elems) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            std::set<int> exist_elements_index;
            for (const auto& sub_value : array_view.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    milvus::bson::array_view>(sub_value.get_value());
                if (!sub_array.has_value())
                    continue;

                for (int idx = 0; idx < static_cast<int>(elements_.size());
                     ++idx) {
                    if (CompareTwoJsonArray(sub_array.value(),
                                            elements_[idx])) {
                        exist_elements_index.insert(idx);
                    }
                }
                if (exist_elements_index.size() == elements_.size()) {
                    break;
                }
            }
            res[i] = exist_elements_index.size() == elements_.size();
        }
    }

 private:
    const std::vector<proto::plan::Array> elements_;
};

template <typename GetType>
class ShreddingArrayBsonContainsAnyExecutor {
 public:
    explicit ShreddingArrayBsonContainsAnyExecutor(
        std::shared_ptr<MultiElement> arg_set)
        : arg_set_(std::move(arg_set)) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& element : array_view.value()) {
                if constexpr (std::is_same_v<GetType, int64_t> ||
                              std::is_same_v<GetType, double>) {
                    auto value =
                        GetBsonNumberExact<GetType>(element.get_value());
                    if (value.has_value() && arg_set_->In(*value)) {
                        matched = true;
                        break;
                    }
                } else {
                    auto value =
                        milvus::BsonView::GetValueFromBsonView<GetType>(
                            element.get_value());
                    if (value.has_value() && arg_set_->In(value.value())) {
                        matched = true;
                        break;
                    }
                }
            }
            res[i] = matched;
        }
    }

 private:
    std::shared_ptr<MultiElement> arg_set_;
};

template <typename GetType>
class ShreddingArrayBsonContainsAllExecutor {
 public:
    explicit ShreddingArrayBsonContainsAllExecutor(
        const std::set<GetType>& elements)
        : elements_(elements) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            std::set<GetType> tmp_elements(elements_);
            for (const auto& element : array_view.value()) {
                auto value = [&]() -> std::optional<GetType> {
                    if constexpr (std::is_same_v<GetType, int64_t> ||
                                  std::is_same_v<GetType, double>) {
                        return GetBsonNumberExact<GetType>(element.get_value());
                    } else {
                        return milvus::BsonView::GetValueFromBsonView<GetType>(
                            element.get_value());
                    }
                }();
                if (!value.has_value()) {
                    continue;
                }
                tmp_elements.erase(value.value());
                if (tmp_elements.empty()) {
                    break;
                }
            }
            res[i] = tmp_elements.empty();
        }
    }

 private:
    std::set<GetType> elements_;
};

class ShreddingArrayBsonContainsAllWithDiffTypeExecutor {
 public:
    ShreddingArrayBsonContainsAllWithDiffTypeExecutor(
        std::vector<proto::plan::GenericValue> elements,
        std::shared_ptr<const JsonNumberCandidatePositionIndex>
            number_position_index)
        : elements_(std::move(elements)),
          number_position_index_(std::move(number_position_index)) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        ContainsAllCandidateTracker match_tracker(elements_.size());
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array = bson.ParseAsArrayAtOffset(0);
            if (!array.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            match_tracker.Reset();
            for (const auto& sub_value : array.value()) {
                VisitMatchingBsonCandidatePositions(
                    sub_value.get_value(),
                    elements_,
                    *number_position_index_,
                    [&](size_t candidate_position) {
                        return match_tracker.MarkCandidateMatched(
                            candidate_position);
                    });
                if (match_tracker.AllCandidatesMatched()) {
                    break;
                }
            }
            res[i] = match_tracker.AllCandidatesMatched();
        }
    }

 private:
    std::vector<proto::plan::GenericValue> elements_;
    std::shared_ptr<const JsonNumberCandidatePositionIndex>
        number_position_index_;
};

class ShreddingArrayBsonContainsAnyWithDiffTypeExecutor {
 public:
    explicit ShreddingArrayBsonContainsAnyWithDiffTypeExecutor(
        std::vector<proto::plan::GenericValue> elements,
        std::shared_ptr<const JsonNumberMembershipMatcher>
            number_membership_matcher)
        : elements_(std::move(elements)),
          number_membership_matcher_(std::move(number_membership_matcher)) {
    }

    void
    operator()(const std::string_view* src,
               ValidityView valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array = bson.ParseAsArrayAtOffset(0);
            if (!array.has_value()) {
                res[i] = valid_res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& sub_value : array.value()) {
                matched =
                    BsonValueMatchesAnyCandidate(sub_value.get_value(),
                                                 elements_,
                                                 *number_membership_matcher_);
                if (matched) {
                    break;
                }
            }
            res[i] = matched;
        }
    }

 private:
    std::vector<proto::plan::GenericValue> elements_;
    std::shared_ptr<const JsonNumberMembershipMatcher>
        number_membership_matcher_;
};

class PhyJsonContainsFilterExpr : public SegmentExpr {
 public:
    PhyJsonContainsFilterExpr(
        const std::vector<std::shared_ptr<Expr>>& input,
        const std::shared_ptr<const milvus::expr::JsonContainsExpr>& expr,
        const std::string& name,
        milvus::OpContext* op_ctx,
        const segcore::SegmentInternalInterface* segment,
        int64_t active_count,
        int64_t batch_size,
        int32_t consistency_level,
        const query::PlanOptions& plan_options = {})
        : SegmentExpr(std::move(input),
                      name,
                      op_ctx,
                      segment,
                      expr->column_.field_id_,
                      expr->column_.nested_path_,
                      expr->vals_.empty()
                          ? DataType::NONE
                          : FromValCase(expr->vals_[0].val_case()),
                      active_count,
                      batch_size,
                      consistency_level,
                      false,
                      true,
                      plan_options),
          expr_(expr) {
        // DetermineExecPath();
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

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
        return expr_->column_;
    }

    bool
    IsElementLevelExpression() const override {
        return expr_->column_.element_level_;
    }

    // The one operand shape no JSON accelerator can answer: mixed element
    // types, or an array literal whose element boundaries a typed projection
    // does not preserve. Both run on raw JSON, as on master.
    bool
    JsonOperandShapeIsIndexable() const {
        return expr_->same_type_ &&
               std::none_of(expr_->vals_.begin(),
                            expr_->vals_.end(),
                            [](const auto& value) {
                                return value.val_case() ==
                                       proto::plan::GenericValue::kArrayVal;
                            });
    }

    bool
    PrefersTypedJsonPathIndex() const override {
        return JsonOperandShapeIsIndexable() &&
               HasTypedJsonPathIndexForOperandTypeAtInit();
    }

    void
    DetermineExecPath() override {
        if (CanUseJsonStatsAtInit()) {
            exec_path_ = ExprExecPath::JsonStats;
            return;
        }
        if (expr_->column_.data_type_ == DataType::JSON &&
            !JsonOperandShapeIsIndexable()) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }
        if (expr_->column_.data_type_ == DataType::ARRAY &&
            (expr_->column_.element_level_ || expr_->vals_.empty())) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }
        // A DOUBLE JSON Path index answers large integer elements with double
        // semantics instead of declining the index. INT* Path indexes answer
        // within their configured width. JsonFlatIndex keeps an exact integer
        // field and is unaffected.
        SegmentExpr::DetermineExecPath();
    }

 private:
    VectorPtr
    EvalJsonContainsForDataSegment(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContains(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsByStats();

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContains(EvalCtx& context);

    template <typename ArrayType, typename ExprValueType, bool ElementLevel>
    VectorPtr
    ExecArrayContainsImpl(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsAll(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsAllByStats();

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContainsAll(EvalCtx& context);

    template <typename ArrayType, typename ExprValueType, bool ElementLevel>
    VectorPtr
    ExecArrayContainsAllImpl(EvalCtx& context);

    VectorPtr
    ExecJsonContainsArray(EvalCtx& context);

    VectorPtr
    ExecJsonContainsArrayByStats();

    VectorPtr
    ExecJsonContainsAllArray(EvalCtx& context);

    VectorPtr
    ExecJsonContainsAllArrayByStats();

    VectorPtr
    ExecJsonContainsAllWithDiffType(EvalCtx& context);

    VectorPtr
    ExecJsonContainsAllWithDiffTypeByStats();

    VectorPtr
    ExecJsonContainsWithDiffType(EvalCtx& context);

    VectorPtr
    ExecJsonContainsWithDiffTypeByStats();

    VectorPtr
    EvalArrayContainsForIndexSegment(DataType data_type);

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContainsForIndexSegmentImpl();

 private:
    std::shared_ptr<const milvus::expr::JsonContainsExpr> expr_;
    bool arg_inited_{false};
    std::shared_ptr<MultiElement> arg_set_;
    std::shared_ptr<JsonNumberMembershipMatcher>
        json_number_membership_matcher_;
    std::shared_ptr<JsonNumberCandidatePositionIndex>
        json_number_position_index_;
    std::shared_ptr<void>
        arg_cached_set_;  // For caching std::set<T> or std::vector<T>
    PinWrapper<index::BsonInvertedIndex*> bson_index_{nullptr};
};
}  //namespace exec
}  // namespace milvus
