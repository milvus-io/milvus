#include "segcore/SegmentSmallIndex.h"
#include <optional>
#include "query/ExprImpl.h"
#include "boost/dynamic_bitset.hpp"
#include "query/generated/ExecExprVisitor.h"

namespace milvus::query {
#if 1
// THIS CONTAINS EXTRA BODY FOR VISITOR
// WILL BE USED BY GENERATOR
namespace impl {
class ExecExprVisitor : ExprVisitor {
 public:
    using RetType = std::deque<boost::dynamic_bitset<>>;
    explicit ExecExprVisitor(segcore::SegmentSmallIndex& segment) : segment_(segment) {
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
    template <typename Tp, typename Func>
    auto
    ExecRangeVisitorImpl(RangeExprImpl<Tp>& expr_scp, Func func) -> RetType;

    template <typename T>
    auto
    ExecRangeVisitorDispatcher(RangeExpr& expr_raw) -> RetType;

 private:
    segcore::SegmentSmallIndex& segment_;
    std::optional<RetType> ret_;
};
}  // namespace impl
#endif

void
ExecExprVisitor::visit(BoolUnaryExpr& expr) {
    PanicInfo("unimplemented");
}

void
ExecExprVisitor::visit(BoolBinaryExpr& expr) {
    PanicInfo("unimplemented");
}

void
ExecExprVisitor::visit(TermExpr& expr) {
    PanicInfo("unimplemented");
}

template <typename T, typename Func>
auto
ExecExprVisitor::ExecRangeVisitorImpl(RangeExprImpl<T>& expr, Func func) -> RetType {
    auto& records = segment_.get_insert_record();
    auto data_type = expr.data_type_;
    auto& schema = segment_.get_schema();
    auto field_offset_opt = schema.get_offset(expr.field_id_);
    Assert(field_offset_opt);
    auto field_offset = field_offset_opt.value();
    auto& field_meta = schema[field_offset];
    auto vec_ptr = records.get_scalar_entity<T>(field_offset);
    auto& vec = *vec_ptr;
    RetType results(vec.chunk_size());
    for (auto chunk_id = 0; chunk_id < vec.chunk_size(); ++chunk_id) {
        auto& result = results[chunk_id];
        result.resize(segcore::DefaultElementPerChunk);
        auto chunk = vec.get_chunk(chunk_id);
        const T* data = chunk.data();
        for (int index = 0; index < segcore::DefaultElementPerChunk; ++index) {
            result[index] = func(data[index]);
        }
    }
    return results;
}

#pragma clang diagnostic push
#pragma ide diagnostic ignored "Simplify"
template <typename T>
auto
ExecExprVisitor::ExecRangeVisitorDispatcher(RangeExpr& expr_raw) -> RetType {
    auto& expr = static_cast<RangeExprImpl<T>&>(expr_raw);
    auto conditions = expr.conditions_;
    std::sort(conditions.begin(), conditions.end());
    using OpType = RangeExpr::OpType;
    if (conditions.size() == 1) {
        auto cond = conditions[0];
        // auto [op, val] = cond; // strange bug on capture
        auto op = std::get<0>(cond);
        auto val = std::get<1>(cond);
        switch (op) {
            case OpType::Equal: {
                return ExecRangeVisitorImpl(expr, [val](T x) { return !(x == val); });
            }

            case OpType::NotEqual: {
                return ExecRangeVisitorImpl(expr, [val](T x) { return !(x != val); });
            }

            case OpType::GreaterEqual: {
                return ExecRangeVisitorImpl(expr, [val](T x) { return !(x >= val); });
            }

            case OpType::GreaterThan: {
                return ExecRangeVisitorImpl(expr, [val](T x) { return !(x > val); });
            }

            case OpType::LessEqual: {
                return ExecRangeVisitorImpl(expr, [val](T x) { return !(x <= val); });
            }

            case OpType::LessThan: {
                return ExecRangeVisitorImpl(expr, [val](T x) { return !(x < val); });
            }
            default: {
                PanicInfo("unsupported range node");
            }
        }
    } else if (conditions.size() == 2) {
        OpType op1, op2;
        T val1, val2;
        std::tie(op1, val1) = conditions[0];
        std::tie(op2, val2) = conditions[1];
        Assert(val1 <= val2);
        auto ops = std::make_tuple(op1, op2);
        if (false) {
        } else if (ops == std::make_tuple(OpType::GreaterThan, OpType::LessThan)) {
            return ExecRangeVisitorImpl(expr, [val1, val2](T x) { return !(val1 < x && x < val2); });
        } else if (ops == std::make_tuple(OpType::GreaterThan, OpType::LessEqual)) {
            return ExecRangeVisitorImpl(expr, [val1, val2](T x) { return !(val1 < x && x <= val2); });
        } else if (ops == std::make_tuple(OpType::GreaterEqual, OpType::LessThan)) {
            return ExecRangeVisitorImpl(expr, [val1, val2](T x) { return !(val1 <= x && x < val2); });
        } else if (ops == std::make_tuple(OpType::GreaterEqual, OpType::LessEqual)) {
            return ExecRangeVisitorImpl(expr, [val1, val2](T x) { return !(val1 <= x && x <= val2); });
        } else {
            PanicInfo("unsupported range node");
        }
    } else {
        PanicInfo("unsupported range node");
    }
}
#pragma clang diagnostic pop

void
ExecExprVisitor::visit(RangeExpr& expr) {
    auto& field_meta = segment_.get_schema()[expr.field_id_];
    Assert(expr.data_type_ == field_meta.get_data_type());
    RetType ret;
    switch (expr.data_type_) {
        // case DataType::BOOL: {
        //    ret = ExecRangeVisitorDispatcher<bool>(expr);
        //    break;
        //}
        case DataType::BOOL:
        case DataType::INT8: {
            ret = ExecRangeVisitorDispatcher<int8_t>(expr);
            break;
        }
        case DataType::INT16: {
            ret = ExecRangeVisitorDispatcher<int16_t>(expr);
            break;
        }
        case DataType::INT32: {
            ret = ExecRangeVisitorDispatcher<int32_t>(expr);
            break;
        }
        case DataType::INT64: {
            ret = ExecRangeVisitorDispatcher<int64_t>(expr);
            break;
        }
        case DataType::FLOAT: {
            ret = ExecRangeVisitorDispatcher<float>(expr);
            break;
        }
        case DataType::DOUBLE: {
            ret = ExecRangeVisitorDispatcher<double>(expr);
            break;
        }
        default:
            PanicInfo("unsupported");
    }
    ret_ = std::move(ret);
}

}  // namespace milvus::query
