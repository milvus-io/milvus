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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "common/Schema.h"
#include "pb/plan.pb.h"

namespace milvus::query {

class ExprVisitor;

// Base of all Exprs
struct Expr {
 public:
    virtual ~Expr() = default;
    virtual void
    accept(ExprVisitor&) = 0;
};

using ExprPtr = std::unique_ptr<Expr>;

struct BinaryExprBase : Expr {
    const ExprPtr left_;
    const ExprPtr right_;

    BinaryExprBase() = delete;

    BinaryExprBase(ExprPtr& left, ExprPtr& right) : left_(std::move(left)), right_(std::move(right)) {
    }
};

struct UnaryExprBase : Expr {
    const ExprPtr child_;

    UnaryExprBase() = delete;

    explicit UnaryExprBase(ExprPtr& child) : child_(std::move(child)) {
    }
};

struct LogicalUnaryExpr : UnaryExprBase {
    enum class OpType { Invalid = 0, LogicalNot = 1 };
    const OpType op_type_;

    LogicalUnaryExpr(const OpType op_type, ExprPtr& child) : UnaryExprBase(child), op_type_(op_type) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

struct LogicalBinaryExpr : BinaryExprBase {
    // Note: bitA - bitB == bitA & ~bitB, alias to LogicalMinus
    enum class OpType { Invalid = 0, LogicalAnd = 1, LogicalOr = 2, LogicalXor = 3, LogicalMinus = 4 };
    const OpType op_type_;

    LogicalBinaryExpr(const OpType op_type, ExprPtr& left, ExprPtr& right)
        : BinaryExprBase(left, right), op_type_(op_type) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

struct TermExpr : Expr {
    const FieldId field_id_;
    const DataType data_type_;

 protected:
    // prevent accidential instantiation
    TermExpr() = delete;

    TermExpr(const FieldId field_id, const DataType data_type) : field_id_(field_id), data_type_(data_type) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

static const std::map<std::string, ArithOpType> arith_op_mapping_ = {
    // arith_op_name -> arith_op
    {"add", ArithOpType::Add}, {"sub", ArithOpType::Sub}, {"mul", ArithOpType::Mul},
    {"div", ArithOpType::Div}, {"mod", ArithOpType::Mod},
};

static const std::map<ArithOpType, std::string> mapping_arith_op_ = {
    // arith_op_name -> arith_op
    {ArithOpType::Add, "add"}, {ArithOpType::Sub, "sub"}, {ArithOpType::Mul, "mul"},
    {ArithOpType::Div, "div"}, {ArithOpType::Mod, "mod"},
};

struct BinaryArithOpEvalRangeExpr : Expr {
    const FieldId field_id_;
    const DataType data_type_;
    const OpType op_type_;
    const ArithOpType arith_op_;

 protected:
    // prevent accidential instantiation
    BinaryArithOpEvalRangeExpr() = delete;

    BinaryArithOpEvalRangeExpr(const FieldId field_id,
                               const DataType data_type,
                               const OpType op_type,
                               const ArithOpType arith_op)
        : field_id_(field_id), data_type_(data_type), op_type_(op_type), arith_op_(arith_op) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

static const std::map<std::string, OpType> mapping_ = {
    // op_name -> op
    {"lt", OpType::LessThan},    {"le", OpType::LessEqual},    {"lte", OpType::LessEqual},
    {"gt", OpType::GreaterThan}, {"ge", OpType::GreaterEqual}, {"gte", OpType::GreaterEqual},
    {"eq", OpType::Equal},       {"ne", OpType::NotEqual},
};

struct UnaryRangeExpr : Expr {
    const FieldId field_id_;
    const DataType data_type_;
    const OpType op_type_;

 protected:
    // prevent accidential instantiation
    UnaryRangeExpr() = delete;

    UnaryRangeExpr(const FieldId field_id, const DataType data_type, const OpType op_type)
        : field_id_(field_id), data_type_(data_type), op_type_(op_type) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

struct BinaryRangeExpr : Expr {
    const FieldId field_id_;
    const DataType data_type_;
    const bool lower_inclusive_;
    const bool upper_inclusive_;

 protected:
    // prevent accidential instantiation
    BinaryRangeExpr() = delete;

    BinaryRangeExpr(const FieldId field_id,
                    const DataType data_type,
                    const bool lower_inclusive,
                    const bool upper_inclusive)
        : field_id_(field_id),
          data_type_(data_type),
          lower_inclusive_(lower_inclusive),
          upper_inclusive_(upper_inclusive) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

struct CompareExpr : Expr {
    FieldId left_field_id_;
    FieldId right_field_id_;
    DataType left_data_type_;
    DataType right_data_type_;
    OpType op_type_;

 public:
    void
    accept(ExprVisitor&) override;
};

}  // namespace milvus::query
