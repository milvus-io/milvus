#pragma once
#include <memory>
#include <vector>
#include <any>
#include <string>
#include <optional>
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

struct BinaryExpr : Expr {
    ExprPtr left_;
    ExprPtr right_;

 public:
    void
    accept(ExprVisitor&) = 0;
};

struct UnaryExpr : Expr {
    ExprPtr child_;

 public:
    void
    accept(ExprVisitor&) = 0;
};

// TODO: not enabled in sprint 1
struct BoolUnaryExpr : UnaryExpr {
    enum class OpType { LogicalNot };
    OpType op_type_;

 public:
    void
    accept(ExprVisitor&) override;
};

// TODO: not enabled in sprint 1
struct BoolBinaryExpr : BinaryExpr {
    enum class OpType { LogicalAnd, LogicalOr, LogicalXor };
    OpType op_type_;

 public:
    void
    accept(ExprVisitor&) override;
};

using FieldId = std::string;

struct TermExpr : Expr {
    FieldId field_id_;
    DataType data_type_;
    // std::vector<std::any> terms_;

 protected:
    // prevent accidential instantiation
    TermExpr() = default;

 public:
    void
    accept(ExprVisitor&) override;
};

struct RangeExpr : Expr {
    FieldId field_id_;
    DataType data_type_;
    enum class OpType { LessThan, LessEqual, GreaterThan, GreaterEqual, Equal, NotEqual };
    static const std::map<std::string, OpType> mapping_;  // op_name -> op

    // std::vector<std::tuple<OpType, std::any>> conditions_;
 protected:
    // prevent accidential instantiation
    RangeExpr() = default;

 public:
    void
    accept(ExprVisitor&) override;
};
}  // namespace milvus::query
