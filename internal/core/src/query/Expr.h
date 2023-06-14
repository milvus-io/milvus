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
#include "common/Types.h"
#include "pb/plan.pb.h"

namespace milvus::query {

using optype = proto::plan::OpType;

class ExprVisitor;

struct ColumnInfo {
    FieldId field_id;
    DataType data_type;
    std::vector<std::string> nested_path;

    ColumnInfo(const proto::plan::ColumnInfo& column_info)
        : field_id(column_info.field_id()),
          data_type(static_cast<DataType>(column_info.data_type())),
          nested_path(column_info.nested_path().begin(),
                      column_info.nested_path().end()) {
    }

    ColumnInfo(FieldId field_id,
               DataType data_type,
               std::vector<std::string> nested_path = {})
        : field_id(field_id),
          data_type(data_type),
          nested_path(std::move(nested_path)) {
    }
};

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

    BinaryExprBase(ExprPtr& left, ExprPtr& right)
        : left_(std::move(left)), right_(std::move(right)) {
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

    LogicalUnaryExpr(const OpType op_type, ExprPtr& child)
        : UnaryExprBase(child), op_type_(op_type) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

struct LogicalBinaryExpr : BinaryExprBase {
    // Note: bitA - bitB == bitA & ~bitB, alias to LogicalMinus
    enum class OpType {
        Invalid = 0,
        LogicalAnd = 1,
        LogicalOr = 2,
        LogicalXor = 3,
        LogicalMinus = 4
    };
    const OpType op_type_;

    LogicalBinaryExpr(const OpType op_type, ExprPtr& left, ExprPtr& right)
        : BinaryExprBase(left, right), op_type_(op_type) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

struct TermExpr : Expr {
    const ColumnInfo column_;
    const proto::plan::GenericValue::ValCase val_case_;
    const bool is_in_field_;

 protected:
    // prevent accidental instantiation
    TermExpr() = delete;

    TermExpr(ColumnInfo column,
             const proto::plan::GenericValue::ValCase val_case,
             const bool is_in_field)
        : column_(std::move(column)),
          val_case_(val_case),
          is_in_field_(is_in_field) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

static const std::map<std::string, ArithOpType> arith_op_mapping_ = {
    // arith_op_name -> arith_op
    {"add", ArithOpType::Add},
    {"sub", ArithOpType::Sub},
    {"mul", ArithOpType::Mul},
    {"div", ArithOpType::Div},
    {"mod", ArithOpType::Mod},
};

static const std::map<ArithOpType, std::string> mapping_arith_op_ = {
    // arith_op_name -> arith_op
    {ArithOpType::Add, "add"},
    {ArithOpType::Sub, "sub"},
    {ArithOpType::Mul, "mul"},
    {ArithOpType::Div, "div"},
    {ArithOpType::Mod, "mod"},
};

struct BinaryArithOpEvalRangeExpr : Expr {
    const ColumnInfo column_;
    const proto::plan::GenericValue::ValCase val_case_;
    const OpType op_type_;
    const ArithOpType arith_op_;

 protected:
    // prevent accidental instantiation
    BinaryArithOpEvalRangeExpr() = delete;

    BinaryArithOpEvalRangeExpr(
        ColumnInfo column,
        const proto::plan::GenericValue::ValCase val_case,
        const OpType op_type,
        const ArithOpType arith_op)
        : column_(std::move(column)),
          val_case_(val_case),
          op_type_(op_type),
          arith_op_(arith_op) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

static const std::map<std::string, OpType> mapping_ = {
    // op_name -> op
    {"lt", OpType::LessThan},
    {"le", OpType::LessEqual},
    {"lte", OpType::LessEqual},
    {"gt", OpType::GreaterThan},
    {"ge", OpType::GreaterEqual},
    {"gte", OpType::GreaterEqual},
    {"eq", OpType::Equal},
    {"ne", OpType::NotEqual},
};

struct UnaryRangeExpr : Expr {
    ColumnInfo column_;
    const OpType op_type_;
    const proto::plan::GenericValue::ValCase val_case_;

 protected:
    // prevent accidental instantiation
    UnaryRangeExpr() = delete;

    UnaryRangeExpr(ColumnInfo column,
                   const OpType op_type,
                   const proto::plan::GenericValue::ValCase val_case)
        : column_(std::move(column)), op_type_(op_type), val_case_(val_case) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

struct BinaryRangeExpr : Expr {
    const ColumnInfo column_;
    const proto::plan::GenericValue::ValCase val_case_;
    const bool lower_inclusive_;
    const bool upper_inclusive_;

 protected:
    // prevent accidental instantiation
    BinaryRangeExpr() = delete;

    BinaryRangeExpr(ColumnInfo column,
                    const proto::plan::GenericValue::ValCase val_case,
                    const bool lower_inclusive,
                    const bool upper_inclusive)
        : column_(std::move(column)),
          val_case_(val_case),
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

struct ExistsExpr : Expr {
    const ColumnInfo column_;

 protected:
    // prevent accidental instantiation
    ExistsExpr() = delete;

    ExistsExpr(ColumnInfo column) : column_(std::move(column)) {
    }

 public:
    void
    accept(ExprVisitor&) override;
};

}  // namespace milvus::query
