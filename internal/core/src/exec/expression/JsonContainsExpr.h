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
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/Expr.h"
#include "exec/expression/Element.h"
#include "segcore/SegmentInterface.h"
#include "common/bson_view.h"
#include "exec/expression/Utils.h"

namespace milvus {
namespace exec {

class ShreddingArrayBsonContainsArrayExecutor {
 public:
    explicit ShreddingArrayBsonContainsArrayExecutor(
        const std::vector<proto::plan::Array>& elems)
        : elements_(elems) {
    }

    void
    operator()(const std::string_view* src,
               const bool* valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid != nullptr && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& sub_value : array_view.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    bsoncxx::array::view>(sub_value.get_value());
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
               const bool* valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid != nullptr && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = false;
                continue;
            }
            std::set<int> exist_elements_index;
            for (const auto& sub_value : array_view.value()) {
                auto sub_array = milvus::BsonView::GetValueFromBsonView<
                    bsoncxx::array::view>(sub_value.get_value());
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
    ShreddingArrayBsonContainsAnyExecutor(
        std::shared_ptr<MultiElement> arg_set,
        std::shared_ptr<MultiElement> arg_set_double)
        : arg_set_(std::move(arg_set)),
          arg_set_double_(std::move(arg_set_double)) {
    }

    void
    operator()(const std::string_view* src,
               const bool* valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid != nullptr && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& element : array_view.value()) {
                if constexpr (std::is_same_v<GetType, int64_t> ||
                              std::is_same_v<GetType, double>) {
                    auto value = milvus::BsonView::GetValueFromBsonView<double>(
                        element.get_value());
                    if (value.has_value() &&
                        arg_set_double_->In(value.value())) {
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
    std::shared_ptr<MultiElement> arg_set_double_;
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
               const bool* valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid != nullptr && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array_view = bson.ParseAsArrayAtOffset(0);
            if (!array_view.has_value()) {
                res[i] = false;
                continue;
            }
            std::set<GetType> tmp_elements(elements_);
            for (const auto& element : array_view.value()) {
                auto value = milvus::BsonView::GetValueFromBsonView<GetType>(
                    element.get_value());
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
        std::set<int> elements_index)
        : elements_(std::move(elements)),
          elements_index_(std::move(elements_index)) {
    }

    void
    operator()(const std::string_view* src,
               const bool* valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid != nullptr && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array = bson.ParseAsArrayAtOffset(0);
            if (!array.has_value()) {
                res[i] = false;
                continue;
            }
            std::set<int> tmp_elements_index(elements_index_);
            for (const auto& sub_value : array.value()) {
                int idx = -1;
                for (auto& element : elements_) {
                    idx++;
                    switch (element.val_case()) {
                        case proto::plan::GenericValue::kBoolVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<bool>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.bool_val()) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<int64_t>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.int64_val()) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<double>(
                                    sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.float_val()) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                std::string>(sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (val.value() == element.string_val()) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                bsoncxx::array::view>(sub_value.get_value());
                            if (!val.has_value()) {
                                continue;
                            }
                            if (CompareTwoJsonArray(val.value(),
                                                    element.array_val())) {
                                tmp_elements_index.erase(idx);
                            }
                            break;
                        }
                        default:
                            ThrowInfo(DataTypeInvalid,
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
                    }
                    if (tmp_elements_index.size() == 0) {
                        break;
                    }
                }
                if (tmp_elements_index.size() == 0) {
                    break;
                }
            }
            res[i] = tmp_elements_index.size() == 0;
        }
    }

 private:
    std::vector<proto::plan::GenericValue> elements_;
    std::set<int> elements_index_;
};

class ShreddingArrayBsonContainsAnyWithDiffTypeExecutor {
 public:
    explicit ShreddingArrayBsonContainsAnyWithDiffTypeExecutor(
        std::vector<proto::plan::GenericValue> elements)
        : elements_(std::move(elements)) {
    }

    void
    operator()(const std::string_view* src,
               const bool* valid,
               size_t size,
               TargetBitmapView res,
               TargetBitmapView valid_res) {
        for (size_t i = 0; i < size; ++i) {
            if (valid != nullptr && !valid[i]) {
                res[i] = valid_res[i] = false;
                continue;
            }
            milvus::BsonView bson(
                reinterpret_cast<const uint8_t*>(src[i].data()), src[i].size());
            auto array = bson.ParseAsArrayAtOffset(0);
            if (!array.has_value()) {
                res[i] = false;
                continue;
            }
            bool matched = false;
            for (const auto& sub_value : array.value()) {
                for (auto const& element : elements_) {
                    switch (element.val_case()) {
                        case proto::plan::GenericValue::kBoolVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<bool>(
                                    sub_value.get_value());
                            if (val.has_value() &&
                                val.value() == element.bool_val()) {
                                matched = true;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kInt64Val: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<int64_t>(
                                    sub_value.get_value());
                            if (val.has_value() &&
                                val.value() == element.int64_val()) {
                                matched = true;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kFloatVal: {
                            auto val =
                                milvus::BsonView::GetValueFromBsonView<double>(
                                    sub_value.get_value());
                            if (val.has_value() &&
                                val.value() == element.float_val()) {
                                matched = true;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kStringVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                std::string>(sub_value.get_value());
                            if (val.has_value() &&
                                val.value() == element.string_val()) {
                                matched = true;
                            }
                            break;
                        }
                        case proto::plan::GenericValue::kArrayVal: {
                            auto val = milvus::BsonView::GetValueFromBsonView<
                                bsoncxx::array::view>(sub_value.get_value());
                            if (val.has_value() &&
                                CompareTwoJsonArray(val.value(),
                                                    element.array_val())) {
                                matched = true;
                            }
                            break;
                        }
                        default:
                            ThrowInfo(DataTypeInvalid,
                                      fmt::format("unsupported data type {}",
                                                  element.val_case()));
                    }
                    if (matched)
                        break;
                }
                if (matched)
                    break;
            }
            res[i] = matched;
        }
    }

 private:
    std::vector<proto::plan::GenericValue> elements_;
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
        int32_t consistency_level)
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
                      true),
          expr_(expr) {
    }

    void
    Eval(EvalCtx& context, VectorPtr& result) override;

    std::string
    ToString() const {
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

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsAll(EvalCtx& context);

    template <typename ExprValueType>
    VectorPtr
    ExecJsonContainsAllByStats();

    template <typename ExprValueType>
    VectorPtr
    ExecArrayContainsAll(EvalCtx& context);

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
    std::shared_ptr<MultiElement> arg_set_double_;
    PinWrapper<index::JsonKeyStats*> pinned_json_stats_{nullptr};
};
}  //namespace exec
}  // namespace milvus
