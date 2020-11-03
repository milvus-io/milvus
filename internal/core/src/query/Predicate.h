#pragma once
#include <memory>
#include <vector>
#include <any>
#include <string>
#include <optional>
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

// // TODO: not enabled in sprint 1
// struct ArthmeticBinaryOpExpr : BinaryExpr {
//     enum class OpType { Add, Sub, Multiply, Divide };
//     OpType op_type_;
//  public:
//     void
//     accept(ExprVisitor&) override;
// };

using FieldId = int64_t;

struct TermExpr : Expr {
    FieldId field_id_;
    std::vector<std::any> terms_;  //
 public:
    void
    accept(ExprVisitor&) override;
};

struct RangeExpr : Expr {
    FieldId field_id_;
    enum class OpType { LessThan, LessEqual, GreaterThan, GreaterEqual, Equal, NotEqual };
    std::vector<std::tuple<OpType, std::any>> conditions_;

 public:
    void
    accept(ExprVisitor&) override;
};

}  // namespace milvus::query
