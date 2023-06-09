// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
// Generated File
// DO NOT EDIT
#include <optional>
#include <boost/variant.hpp>
#include <type_traits>
#include <utility>
#include <deque>
#include "segcore/SegmentGrowingImpl.h"
#include "query/ExprImpl.h"
#include "ExprVisitor.h"

namespace milvus::query {

void
AppendOneChunk(BitsetType& result, const FixedVector<bool>& chunk_res);

class ExecExprVisitor : public ExprVisitor {
 public:
    void
    visit(LogicalUnaryExpr& expr) override;

    void
    visit(LogicalBinaryExpr& expr) override;

    void
    visit(TermExpr& expr) override;

    void
    visit(UnaryRangeExpr& expr) override;

    void
    visit(BinaryArithOpEvalRangeExpr& expr) override;

    void
    visit(BinaryRangeExpr& expr) override;

    void
    visit(CompareExpr& expr) override;

    void
    visit(ExistsExpr& expr) override;

 public:
    ExecExprVisitor(const segcore::SegmentInternalInterface& segment,
                    int64_t row_count,
                    Timestamp timestamp)
        : segment_(segment), row_count_(row_count), timestamp_(timestamp) {
    }

    BitsetType
    call_child(Expr& expr) {
        Assert(!bitset_opt_.has_value());
        expr.accept(*this);
        Assert(bitset_opt_.has_value());
        auto res = std::move(bitset_opt_);
        bitset_opt_ = std::nullopt;
        return std::move(res.value());
    }

 public:
    template <typename T, typename IndexFunc, typename ElementFunc>
    auto
    ExecRangeVisitorImpl(FieldId field_id,
                         IndexFunc func,
                         ElementFunc element_func) -> BitsetType;

    template <typename T, typename IndexFunc, typename ElementFunc>
    auto
    ExecDataRangeVisitorImpl(FieldId field_id,
                             IndexFunc index_func,
                             ElementFunc element_func) -> BitsetType;

    template <typename T>
    auto
    ExecUnaryRangeVisitorDispatcher(UnaryRangeExpr& expr_raw) -> BitsetType;

    template <typename ExprValueType>
    auto
    ExecUnaryRangeVisitorDispatcherJson(UnaryRangeExpr& expr_raw) -> BitsetType;

    template <typename ExprValueType>
    auto
    ExecBinaryArithOpEvalRangeVisitorDispatcherJson(
        BinaryArithOpEvalRangeExpr& expr_raw) -> BitsetType;

    template <typename T>
    auto
    ExecBinaryArithOpEvalRangeVisitorDispatcher(
        BinaryArithOpEvalRangeExpr& expr_raw) -> BitsetType;

    template <typename ExprValueType>
    auto
    ExecBinaryRangeVisitorDispatcherJson(BinaryRangeExpr& expr_raw)
        -> BitsetType;

    template <typename T>
    auto
    ExecBinaryRangeVisitorDispatcher(BinaryRangeExpr& expr_raw) -> BitsetType;

    template <typename T>
    auto
    ExecTermVisitorImpl(TermExpr& expr_raw) -> BitsetType;

    template <typename T>
    auto
    ExecTermVisitorImplTemplate(TermExpr& expr_raw) -> BitsetType;

    template <typename ExprValueType>
    auto
    ExecTermJsonVariableInField(TermExpr& expr_raw) -> BitsetType;

    template <typename ExprValueType>
    auto
    ExecTermJsonFieldInVariable(TermExpr& expr_raw) -> BitsetType;

    template <typename ExprValueType>
    auto
    ExecTermVisitorImplTemplateJson(TermExpr& expr_raw) -> BitsetType;

    template <typename CmpFunc>
    auto
    ExecCompareExprDispatcher(CompareExpr& expr, CmpFunc cmp_func)
        -> BitsetType;

    template <typename CmpFunc>
    BitsetType
    ExecCompareExprDispatcherForNonIndexedSegment(CompareExpr& expr,
                                                  CmpFunc cmp_func);

    // This function only used to compare sealed segment
    // which has only one chunk.
    template <typename T, typename U, typename CmpFunc>
    TargetBitmap
    ExecCompareRightType(const T* left_raw_data,
                         const FieldId& right_field_id,
                         const int64_t current_chunk_id,
                         CmpFunc cmp_func);

    template <typename T, typename CmpFunc>
    BitsetType
    ExecCompareLeftType(const FieldId& left_field_id,
                        const FieldId& right_field_id,
                        const DataType& right_field_type,
                        CmpFunc cmp_func);

 private:
    const segcore::SegmentInternalInterface& segment_;
    Timestamp timestamp_;
    int64_t row_count_;

    BitsetTypeOpt bitset_opt_;
};
}  // namespace milvus::query
