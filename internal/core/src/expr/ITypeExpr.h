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
#include <memory>
#include <string>
#include <vector>

#include "exec/expression/function/FunctionFactory.h"
#include "common/Exception.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/Utils.h"
#include "pb/plan.pb.h"

namespace milvus {
namespace expr {

// Collect information from expressions
struct ExprInfo {
    struct GenericValueEqual {
        using GenericValue = proto::plan::GenericValue;
        bool
        operator()(const GenericValue& lhs, const GenericValue& rhs) const {
            if (lhs.val_case() != rhs.val_case())
                return false;
            switch (lhs.val_case()) {
                case GenericValue::kBoolVal:
                    return lhs.bool_val() == rhs.bool_val();
                case GenericValue::kInt64Val:
                    return lhs.int64_val() == rhs.int64_val();
                case GenericValue::kFloatVal:
                    return lhs.float_val() == rhs.float_val();
                case GenericValue::kStringVal:
                    return lhs.string_val() == rhs.string_val();
                case GenericValue::VAL_NOT_SET:
                    return true;
                default:
                    PanicInfo(NotImplemented,
                              "Not supported GenericValue type");
            }
        }
    };

    struct GenericValueHasher {
        using GenericValue = proto::plan::GenericValue;
        std::size_t
        operator()(const GenericValue& value) const {
            std::size_t h = 0;
            switch (value.val_case()) {
                case GenericValue::kBoolVal:
                    h = std::hash<bool>()(value.bool_val());
                    break;
                case GenericValue::kInt64Val:
                    h = std::hash<int64_t>()(value.int64_val());
                    break;
                case GenericValue::kFloatVal:
                    h = std::hash<float>()(value.float_val());
                    break;
                case GenericValue::kStringVal:
                    h = std::hash<std::string>()(value.string_val());
                    break;
                case GenericValue::VAL_NOT_SET:
                    break;
                default:
                    PanicInfo(NotImplemented,
                              "Not supported GenericValue type");
            }
            return h;
        }
    };

    /* For Materialized View (vectors and scalars), that is when performing filtered search. */
    // The map describes which scalar field is involved during search,
    // and the set of category values
    // for example, if we have scalar field `color` with field id `111` and it has three categories: red, green, blue
    // expression `color == "red"`, yields `111 -> (red)`
    // expression `color == "red" && color == "green"`, yields `111 -> (red, green)`
    std::unordered_map<int64_t,
                       std::unordered_set<proto::plan::GenericValue,
                                          GenericValueHasher,
                                          GenericValueEqual>>
        field_id_to_values;
    // whether the search exression has AND (&&) logical operator only
    bool is_pure_and = true;
    // whether the search expression has NOT (!) logical unary operator
    bool has_not = false;
};

inline bool
IsMaterializedViewSupported(const DataType& data_type) {
    return data_type == DataType::BOOL || data_type == DataType::INT8 ||
           data_type == DataType::INT16 || data_type == DataType::INT32 ||
           data_type == DataType::INT64 || data_type == DataType::FLOAT ||
           data_type == DataType::DOUBLE || data_type == DataType::VARCHAR ||
           data_type == DataType::STRING;
}

struct ColumnInfo {
    FieldId field_id_;
    DataType data_type_;
    DataType element_type_;
    std::vector<std::string> nested_path_;
    bool nullable_;

    ColumnInfo(const proto::plan::ColumnInfo& column_info)
        : field_id_(column_info.field_id()),
          data_type_(static_cast<DataType>(column_info.data_type())),
          element_type_(static_cast<DataType>(column_info.element_type())),
          nested_path_(column_info.nested_path().begin(),
                       column_info.nested_path().end()),
          nullable_(column_info.nullable()) {
    }

    ColumnInfo(FieldId field_id,
               DataType data_type,
               std::vector<std::string> nested_path = {},
               bool nullable = false)
        : field_id_(field_id),
          data_type_(data_type),
          element_type_(DataType::NONE),
          nested_path_(std::move(nested_path)),
          nullable_(nullable) {
    }

    bool
    operator==(const ColumnInfo& other) {
        if (field_id_ != other.field_id_) {
            return false;
        }

        if (data_type_ != other.data_type_) {
            return false;
        }

        if (element_type_ != other.element_type_) {
            return false;
        }

        for (int i = 0; i < nested_path_.size(); ++i) {
            if (nested_path_[i] != other.nested_path_[i]) {
                return false;
            }
        }

        return true;
    }

    std::string
    ToString() const {
        return fmt::format(
            "[FieldId:{}, data_type:{}, element_type:{}, nested_path:{}]",
            std::to_string(field_id_.get()),
            data_type_,
            element_type_,
            milvus::Join<std::string>(nested_path_, ","));
    }
};

/** 
 * @brief Base class for all exprs
 * a strongly-typed expression, such as literal, function call, etc...
 */
class ITypeExpr {
 public:
    explicit ITypeExpr(DataType type) : type_(type), inputs_{} {
    }

    ITypeExpr(DataType type,
              std::vector<std::shared_ptr<const ITypeExpr>> inputs)
        : type_(type), inputs_{std::move(inputs)} {
    }

    virtual ~ITypeExpr() = default;

    const std::vector<std::shared_ptr<const ITypeExpr>>&
    inputs() const {
        return inputs_;
    }

    DataType
    type() const {
        return type_;
    }

    virtual std::string
    ToString() const = 0;

    const std::vector<std::shared_ptr<const ITypeExpr>>&
    inputs() {
        return inputs_;
    }

    virtual void
    GatherInfo(ExprInfo& info) const {};

 protected:
    DataType type_;
    std::vector<std::shared_ptr<const ITypeExpr>> inputs_;
};

using TypedExprPtr = std::shared_ptr<const ITypeExpr>;

// NOTE: unused
class InputTypeExpr : public ITypeExpr {
 public:
    InputTypeExpr(DataType type) : ITypeExpr(type) {
    }

    std::string
    ToString() const override {
        return "ROW";
    }
};

using InputTypeExprPtr = std::shared_ptr<const InputTypeExpr>;

// NOTE: unused
class FieldAccessTypeExpr : public ITypeExpr {
 public:
    FieldAccessTypeExpr(DataType type, const std::string& name, FieldId fieldId)
        : ITypeExpr{type},
          name_(name),
          field_id_(fieldId),
          is_input_column_(true) {
    }

    FieldAccessTypeExpr(DataType type, FieldId fieldId)
        : ITypeExpr{type},
          name_(""),
          field_id_(fieldId),
          is_input_column_(true) {
    }

    FieldAccessTypeExpr(DataType type,
                        const TypedExprPtr& input,
                        const std::string& name,
                        FieldId fieldId)
        : ITypeExpr{type, {std::move(input)}}, name_(name), field_id_(fieldId) {
        is_input_column_ =
            dynamic_cast<const InputTypeExpr*>(inputs_[0].get()) != nullptr;
    }

    bool
    is_input_column() const {
        return is_input_column_;
    }

    const std::string&
    name() const {
        return name_;
    }

    std::string
    ToString() const override {
        if (inputs_.empty()) {
            return fmt::format("{}", name_);
        }

        return fmt::format(
            "{}[{}{}]", inputs_[0]->ToString(), name_, field_id_.get());
    }

 private:
    std::string name_;
    const FieldId field_id_;
    bool is_input_column_;
};

using FieldAccessTypeExprPtr = std::shared_ptr<const FieldAccessTypeExpr>;

/** 
 * @brief Base class for all milvus filter exprs, output type must be BOOL
 * a strongly-typed expression, such as literal, function call, etc...
 */
class ITypeFilterExpr : public ITypeExpr {
 public:
    ITypeFilterExpr() : ITypeExpr(DataType::BOOL) {
    }

    ITypeFilterExpr(std::vector<std::shared_ptr<const ITypeExpr>> inputs)
        : ITypeExpr(DataType::BOOL, std::move(inputs)) {
    }

    virtual ~ITypeFilterExpr() = default;
};

class ColumnExpr : public ITypeExpr {
 public:
    explicit ColumnExpr(const ColumnInfo& column)
        : ITypeExpr(column.data_type_), column_(column) {
    }

    const ColumnInfo&
    GetColumn() const {
        return column_;
    }

    std::string
    ToString() const override {
        std::stringstream ss;
        ss << "ColumnExpr: {columnInfo:" << column_.ToString() << "}";
        return ss.str();
    }

 private:
    const ColumnInfo column_;
};

class ValueExpr : public ITypeExpr {
 public:
    explicit ValueExpr(const proto::plan::GenericValue& val)
        : ITypeExpr(DataType::NONE), val_(val) {
        switch (val.val_case()) {
            case proto::plan::GenericValue::ValCase::kBoolVal:
                type_ = DataType::BOOL;
                break;
            case proto::plan::GenericValue::ValCase::kInt64Val:
                type_ = DataType::INT64;
                break;
            case proto::plan::GenericValue::ValCase::kFloatVal:
                type_ = DataType::FLOAT;
                break;
            case proto::plan::GenericValue::ValCase::kStringVal:
                type_ = DataType::VARCHAR;
                break;
            case proto::plan::GenericValue::ValCase::kArrayVal:
                type_ = DataType::ARRAY;
                break;
            case proto::plan::GenericValue::ValCase::VAL_NOT_SET:
                type_ = DataType::NONE;
                break;
        }
    }

    std::string
    ToString() const override {
        std::stringstream ss;
        ss << "ValueExpr: {"
           << " val:" << val_.DebugString() << "}";
        return ss.str();
    }

    const proto::plan::GenericValue
    GetGenericValue() const {
        return val_;
    }

 private:
    const proto::plan::GenericValue val_;
};

class UnaryRangeFilterExpr : public ITypeFilterExpr {
 public:
    explicit UnaryRangeFilterExpr(const ColumnInfo& column,
                                  proto::plan::OpType op_type,
                                  const proto::plan::GenericValue& val)
        : ITypeFilterExpr(), column_(column), op_type_(op_type), val_(val) {
    }

    std::string
    ToString() const override {
        std::stringstream ss;
        ss << "UnaryRangeFilterExpr: {columnInfo:" << column_.ToString()
           << " op_type:" << milvus::proto::plan::OpType_Name(op_type_)
           << " val:" << val_.DebugString() << "}";
        return ss.str();
    }

    void
    GatherInfo(ExprInfo& info) const override {
        if (IsMaterializedViewSupported(column_.data_type_)) {
            info.field_id_to_values[column_.field_id_.get()].insert(val_);

            // for expression `Field == Value`, we do nothing else
            if (op_type_ == proto::plan::OpType::Equal) {
                return;
            }

            // for expression `Field != Value`, we consider it equivalent
            // as `not (Field == Value)`, so we set `has_not` to true
            if (op_type_ == proto::plan::OpType::NotEqual) {
                info.has_not = true;
                return;
            }

            // for other unary range filter <, >, <=, >=
            // we add a dummy value to indicate multiple values
            // this double insertion is intentional and the default GenericValue
            // will be considered as equal in the unordered_set
            info.field_id_to_values[column_.field_id_.get()].emplace();
        }
    }

 public:
    const ColumnInfo column_;
    const proto::plan::OpType op_type_;
    const proto::plan::GenericValue val_;
};

class AlwaysTrueExpr : public ITypeFilterExpr {
 public:
    explicit AlwaysTrueExpr() {
    }

    std::string
    ToString() const override {
        return "AlwaysTrue expr";
    }
};

class ExistsExpr : public ITypeFilterExpr {
 public:
    explicit ExistsExpr(const ColumnInfo& column)
        : ITypeFilterExpr(), column_(column) {
    }

    std::string
    ToString() const override {
        return "{Exists Expression - Column: " + column_.ToString() + "}";
    }

    const ColumnInfo column_;
};

class LogicalUnaryExpr : public ITypeFilterExpr {
 public:
    enum class OpType { Invalid = 0, LogicalNot = 1 };

    explicit LogicalUnaryExpr(const OpType op_type, const TypedExprPtr& child)
        : op_type_(op_type) {
        inputs_.emplace_back(child);
    }

    std::string
    ToString() const override {
        std::string opTypeString;

        switch (op_type_) {
            case OpType::LogicalNot:
                opTypeString = "Logical NOT";
                break;
            default:
                opTypeString = "Invalid Operator";
                break;
        }

        return fmt::format("LogicalUnaryExpr:[{} - Child: {}]",
                           opTypeString,
                           inputs_[0]->ToString());
    }

    void
    GatherInfo(ExprInfo& info) const override {
        if (op_type_ == OpType::LogicalNot) {
            info.has_not = true;
        }
        assert(inputs_.size() == 1);
        inputs_[0]->GatherInfo(info);
    }

    const OpType op_type_;
};

class TermFilterExpr : public ITypeFilterExpr {
 public:
    explicit TermFilterExpr(const ColumnInfo& column,
                            const std::vector<proto::plan::GenericValue>& vals,
                            bool is_in_field = false)
        : ITypeFilterExpr(),
          column_(column),
          vals_(vals),
          is_in_field_(is_in_field) {
    }

    std::string
    ToString() const override {
        std::string values;

        for (const auto& val : vals_) {
            values += val.DebugString() + ", ";
        }

        std::stringstream ss;
        ss << "TermFilterExpr:[Column: " << column_.ToString() << ", Values: ["
           << values << "]"
           << ", Is In Field: " << (is_in_field_ ? "true" : "false") << "]";

        return ss.str();
    }

    void
    GatherInfo(ExprInfo& info) const override {
        if (IsMaterializedViewSupported(column_.data_type_)) {
            info.field_id_to_values[column_.field_id_.get()].insert(
                vals_.begin(), vals_.end());
        }
    }

 public:
    const ColumnInfo column_;
    const std::vector<proto::plan::GenericValue> vals_;
    const bool is_in_field_;
};

class LogicalBinaryExpr : public ITypeFilterExpr {
 public:
    enum class OpType { Invalid = 0, And = 1, Or = 2 };

    explicit LogicalBinaryExpr(OpType op_type,
                               const TypedExprPtr& left,
                               const TypedExprPtr& right)
        : ITypeFilterExpr(), op_type_(op_type) {
        inputs_.emplace_back(left);
        inputs_.emplace_back(right);
    }

    std::string
    GetOpTypeString() const {
        switch (op_type_) {
            case OpType::Invalid:
                return "Invalid";
            case OpType::And:
                return "And";
            case OpType::Or:
                return "Or";
            default:
                return "Unknown";  // Handle the default case if necessary
        }
    }

    std::string
    ToString() const override {
        return fmt::format("LogicalBinaryExpr:[{} - Left: {}, Right: {}]",
                           GetOpTypeString(),
                           inputs_[0]->ToString(),
                           inputs_[1]->ToString());
    }

    std::string
    name() const {
        return GetOpTypeString();
    }

    void
    GatherInfo(ExprInfo& info) const override {
        if (op_type_ == OpType::Or) {
            info.is_pure_and = false;
        }
        assert(inputs_.size() == 2);
        inputs_[0]->GatherInfo(info);
        inputs_[1]->GatherInfo(info);
    }

 public:
    const OpType op_type_;
};

class BinaryRangeFilterExpr : public ITypeFilterExpr {
 public:
    BinaryRangeFilterExpr(const ColumnInfo& column,
                          const proto::plan::GenericValue& lower_value,
                          const proto::plan::GenericValue& upper_value,
                          bool lower_inclusive,
                          bool upper_inclusive)
        : ITypeFilterExpr(),
          column_(column),
          lower_val_(lower_value),
          upper_val_(upper_value),
          lower_inclusive_(lower_inclusive),
          upper_inclusive_(upper_inclusive) {
    }

    std::string
    ToString() const override {
        std::stringstream ss;
        ss << "BinaryRangeFilterExpr:[Column: " << column_.ToString()
           << ", Lower Value: " << lower_val_.DebugString()
           << ", Upper Value: " << upper_val_.DebugString()
           << ", Lower Inclusive: " << (lower_inclusive_ ? "true" : "false")
           << ", Upper Inclusive: " << (upper_inclusive_ ? "true" : "false")
           << "]";

        return ss.str();
    }

    void
    GatherInfo(ExprInfo& info) const override {
        if (IsMaterializedViewSupported(column_.data_type_)) {
            info.field_id_to_values[column_.field_id_.get()].insert(lower_val_);
            info.field_id_to_values[column_.field_id_.get()].insert(upper_val_);
        }
    }

    const ColumnInfo column_;
    const proto::plan::GenericValue lower_val_;
    const proto::plan::GenericValue upper_val_;
    const bool lower_inclusive_;
    const bool upper_inclusive_;
};

class BinaryArithOpEvalRangeExpr : public ITypeFilterExpr {
 public:
    BinaryArithOpEvalRangeExpr(const ColumnInfo& column,
                               const proto::plan::OpType op_type,
                               const proto::plan::ArithOpType arith_op_type,
                               const proto::plan::GenericValue value,
                               const proto::plan::GenericValue right_operand)
        : column_(column),
          op_type_(op_type),
          arith_op_type_(arith_op_type),
          right_operand_(right_operand),
          value_(value) {
    }

    std::string
    ToString() const override {
        std::stringstream ss;
        ss << "BinaryArithOpEvalRangeExpr:[Column: " << column_.ToString()
           << ", Operator Type: " << milvus::proto::plan::OpType_Name(op_type_)
           << ", Arith Operator Type: "
           << milvus::proto::plan::ArithOpType_Name(arith_op_type_)
           << ", Value: " << value_.DebugString()
           << ", Right Operand: " << right_operand_.DebugString() << "]";

        return ss.str();
    }

 public:
    const ColumnInfo column_;
    const proto::plan::OpType op_type_;
    const proto::plan::ArithOpType arith_op_type_;
    const proto::plan::GenericValue right_operand_;
    const proto::plan::GenericValue value_;
};

class NullExpr : public ITypeFilterExpr {
 public:
    explicit NullExpr(const ColumnInfo& column, NullExprType op)
        : ITypeFilterExpr(), column_(column), op_(op) {
    }

    std::string
    ToString() const override {
        return fmt::format("NullExpr:[Column: {}, Operator: {} ",
                           column_.ToString(),
                           NullExpr_NullOp_Name(op_));
    }

 public:
    const ColumnInfo column_;
    NullExprType op_;
};

class CallExpr : public ITypeFilterExpr {
 public:
    CallExpr(const std::string fun_name,
             const std::vector<TypedExprPtr>& parameters,
             const exec::expression::FilterFunctionPtr function_ptr)
        : fun_name_(std::move(fun_name)), function_ptr_(function_ptr) {
        inputs_.insert(inputs_.end(), parameters.begin(), parameters.end());
    }

    virtual ~CallExpr() = default;

    const std::string&
    fun_name() const {
        return fun_name_;
    }

    const exec::expression::FilterFunctionPtr
    function_ptr() const {
        return function_ptr_;
    }

    std::string
    ToString() const override {
        std::string parameters;
        for (auto& e : inputs_) {
            parameters += e->ToString();
            parameters += ", ";
        }
        return fmt::format("CallExpr:[Function Name: {}, Parameters: {}]",
                           fun_name_,
                           parameters);
    }

 private:
    const std::string fun_name_;
    const exec::expression::FilterFunctionPtr function_ptr_;
};

using CallExprPtr = std::shared_ptr<const CallExpr>;

class CompareExpr : public ITypeFilterExpr {
 public:
    CompareExpr(const FieldId& left_field,
                const FieldId& right_field,
                DataType left_data_type,
                DataType right_data_type,
                proto::plan::OpType op_type)
        : left_field_id_(left_field),
          right_field_id_(right_field),
          left_data_type_(left_data_type),
          right_data_type_(right_data_type),
          op_type_(op_type) {
    }

    std::string
    ToString() const override {
        std::string opTypeString;

        return fmt::format(
            "CompareExpr:[Left Field ID: {}, Right Field ID: {}, Left Data "
            "Type: {}, "
            "Operator: {}, Right "
            "Data Type: {}]",
            left_field_id_.get(),
            right_field_id_.get(),
            milvus::proto::plan::OpType_Name(op_type_),
            left_data_type_,
            right_data_type_);
    }

 public:
    const FieldId left_field_id_;
    const FieldId right_field_id_;
    const DataType left_data_type_;
    const DataType right_data_type_;
    const proto::plan::OpType op_type_;
};

class JsonContainsExpr : public ITypeFilterExpr {
 public:
    JsonContainsExpr(ColumnInfo column,
                     ContainsType op,
                     const bool same_type,
                     const std::vector<proto::plan::GenericValue>& vals)
        : column_(column),
          op_(op),
          same_type_(same_type),
          vals_(std::move(vals)) {
    }

    std::string
    ToString() const override {
        std::string values;
        for (const auto& val : vals_) {
            values += val.DebugString() + ", ";
        }
        return fmt::format(
            "JsonContainsExpr:[Column: {}, Operator: {}, Same Type: {}, "
            "Values: [{}]]",
            column_.ToString(),
            JSONContainsExpr_JSONOp_Name(op_),
            (same_type_ ? "true" : "false"),
            values);
    }

 public:
    const ColumnInfo column_;
    ContainsType op_;
    bool same_type_;
    const std::vector<proto::plan::GenericValue> vals_;
};
}  // namespace expr
}  // namespace milvus

template <>
struct fmt::formatter<milvus::proto::plan::ArithOpType>
    : formatter<string_view> {
    auto
    format(milvus::proto::plan::ArithOpType c, format_context& ctx) const {
        using namespace milvus::proto::plan;
        string_view name = "unknown";
        switch (c) {
            case ArithOpType::Unknown:
                name = "Unknown";
                break;
            case ArithOpType::Add:
                name = "Add";
                break;
            case ArithOpType::Sub:
                name = "Sub";
                break;
            case ArithOpType::Mul:
                name = "Mul";
                break;
            case ArithOpType::Div:
                name = "Div";
                break;
            case ArithOpType::Mod:
                name = "Mod";
                break;
            case ArithOpType::ArrayLength:
                name = "ArrayLength";
                break;
            case ArithOpType::ArithOpType_INT_MIN_SENTINEL_DO_NOT_USE_:
                name = "ArithOpType_INT_MIN_SENTINEL_DO_NOT_USE_";
                break;
            case ArithOpType::ArithOpType_INT_MAX_SENTINEL_DO_NOT_USE_:
                name = "ArithOpType_INT_MAX_SENTINEL_DO_NOT_USE_";
                break;
        }
        return formatter<string_view>::format(name, ctx);
    }
};
