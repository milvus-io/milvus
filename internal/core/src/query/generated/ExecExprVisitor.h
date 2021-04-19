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
#include <boost/dynamic_bitset.hpp>
#include <utility>
#include <deque>
#include "segcore/SegmentGrowingImpl.h"
#include "query/ExprImpl.h"
#include "ExprVisitor.h"

namespace milvus::query {
class ExecExprVisitor : public ExprVisitor {
 public:
    void
    visit(BoolUnaryExpr& expr) override;

    void
    visit(BoolBinaryExpr& expr) override;

    void
    visit(TermExpr& expr) override;

    void
    visit(RangeExpr& expr) override;

 public:
    using RetType = std::deque<boost::dynamic_bitset<>>;
    explicit ExecExprVisitor(const segcore::SegmentGrowingImpl& segment) : segment_(segment) {
    }
    RetType
    call_child(Expr& expr) {
        Assert(!ret_.has_value());
        expr.accept(*this);
        Assert(ret_.has_value());
        auto ret = std::move(ret_);
        ret_ = std::nullopt;
        return std::move(ret.value());
    }

 public:
    template <typename T, typename IndexFunc, typename ElementFunc>
    auto
    ExecRangeVisitorImpl(RangeExprImpl<T>& expr, IndexFunc func, ElementFunc element_func) -> RetType;

    template <typename T>
    auto
    ExecRangeVisitorDispatcher(RangeExpr& expr_raw) -> RetType;

    template <typename T>
    auto
    ExecTermVisitorImpl(TermExpr& expr_raw) -> RetType;

 private:
    const segcore::SegmentGrowingImpl& segment_;
    std::optional<RetType> ret_;
};
}  // namespace milvus::query
