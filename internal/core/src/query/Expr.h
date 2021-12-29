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
#include <vector>

#include "common/Schema.h"

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
    ExprPtr left_;
    ExprPtr right_;
};

struct UnaryExprBase : Expr {
    ExprPtr child_;
};

struct LogicalUnaryExpr : UnaryExprBase {
    enum class OpType { Invalid = 0, LogicalNot = 1 };
    OpType op_type_;

 public:
    void
    accept(ExprVisitor&) override;
};

struct LogicalBinaryExpr : BinaryExprBase {
    // Note: bitA - bitB == bitA & ~bitB, alias to LogicalMinus
    enum class OpType { Invalid = 0, LogicalAnd = 1, LogicalOr = 2, LogicalXor = 3, LogicalMinus = 4 };
    OpType op_type_;

 public:
    void
    accept(ExprVisitor&) override;
};

struct TermExpr : Expr {
    FieldOffset field_offset_;
    DataType data_type_ = DataType::NONE;

 protected:
    // prevent accidential instantiation
    TermExpr() = default;

 public:
    void
    accept(ExprVisitor&) override;
};

enum class OpType {
    Invalid = 0,
    GreaterThan = 1,
    GreaterEqual = 2,
    LessThan = 3,
    LessEqual = 4,
    Equal = 5,
    NotEqual = 6,
};

static const std::map<std::string, OpType> mapping_ = {
    // op_name -> op
    {"lt", OpType::LessThan},    {"le", OpType::LessEqual},    {"lte", OpType::LessEqual},
    {"gt", OpType::GreaterThan}, {"ge", OpType::GreaterEqual}, {"gte", OpType::GreaterEqual},
    {"eq", OpType::Equal},       {"ne", OpType::NotEqual},
};

struct UnaryRangeExpr : Expr {
    FieldOffset field_offset_;
    DataType data_type_ = DataType::NONE;
    OpType op_type_;

 protected:
    // prevent accidential instantiation
    UnaryRangeExpr() = default;

 public:
    void
    accept(ExprVisitor&) override;
};

struct BinaryRangeExpr : Expr {
    FieldOffset field_offset_;
    DataType data_type_ = DataType::NONE;
    bool lower_inclusive_;
    bool upper_inclusive_;

 protected:
    // prevent accidential instantiation
    BinaryRangeExpr() = default;

 public:
    void
    accept(ExprVisitor&) override;
};

struct CompareExpr : Expr {
    FieldOffset left_field_offset_;
    FieldOffset right_field_offset_;
    DataType left_data_type_;
    DataType right_data_type_;
    OpType op_type_;

 public:
    void
    accept(ExprVisitor&) override;
};

}  // namespace milvus::query
