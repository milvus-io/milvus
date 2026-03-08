#include "TimestamptzArithCompareExpr.h"

#include <climits>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <memory>
#include <variant>

#include "BinaryArithOpEvalRangeExpr.h"
#include "bitset/bitset.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"

namespace milvus {
namespace exec {

std::string
PhyTimestamptzArithCompareExpr::ToString() const {
    return expr_->ToString();
}

void
PhyTimestamptzArithCompareExpr::Eval(EvalCtx& context, VectorPtr& result) {
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    result = ExecCompareVisitorImpl<int64_t>(input);
}

template <typename T>
VectorPtr
PhyTimestamptzArithCompareExpr::ExecCompareVisitorImpl(OffsetVector* input) {
    // We can not use index by transforming ts_col + interval > iso_string to ts_col > iso_string - interval
    // Because year / month interval is not fixed days, it depends on the specific date.
    // So, currently, we only support the data scanning path.
    // In the future, one could add a switch here to check for index availability.
    return ExecCompareVisitorImplForAll<T>(input);
}

template <typename T>
VectorPtr
PhyTimestamptzArithCompareExpr::ExecCompareVisitorImplForAll(
    OffsetVector* input) {
    if (!arg_inited_) {
        interval_ = expr_->interval_;
        compare_value_.SetValue<T>(expr_->compare_value_);
        arg_inited_ = true;
    }

    auto arith_op = expr_->arith_op_;
    auto compare_op = expr_->compare_op_;
    auto compare_value = this->compare_value_.GetValue<T>();
    auto interval = interval_;

    if (arith_op == proto::plan::ArithOpType::Unknown) {
        if (!helperPhyExpr_) {  // reconstruct helper expr would cause an error
            proto::plan::GenericValue zeroRightOperand;
            zeroRightOperand.set_int64_val(0);
            auto helperExpr =
                std::make_shared<milvus::expr::BinaryArithOpEvalRangeExpr>(
                    expr_->column_,
                    expr_->compare_op_,
                    proto::plan::ArithOpType::Add,
                    expr_->compare_value_,
                    zeroRightOperand);
            helperPhyExpr_ = std::make_shared<PhyBinaryArithOpEvalRangeExpr>(
                inputs_,
                helperExpr,
                "PhyTimestamptzArithCompareExprHelper",
                op_ctx_,
                segment_,
                active_count_,
                batch_size_,
                consistency_level_);
        }
        return helperPhyExpr_->ExecRangeVisitorImpl<T>(input);
    }
    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));

    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    auto exec_sub_batch =
        [ arith_op,
          compare_op ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            T compare_value,
            proto::plan::Interval interval) {
        const int64_t compare_us = compare_value;
        for (int i = 0; i < size; ++i) {
            auto offset = (offsets) ? offsets[i] : i;
            const int64_t current_ts_us = data[i];
            const int op_sign =
                (arith_op == proto::plan::ArithOpType::Add) ? 1 : -1;
            // Floor division to correctly handle pre-epoch (negative) timestamps:
            // C++ truncates toward zero, but we need floor for time decomposition.
            // e.g., -1500000 us should be epoch_sec=-2, sub_sec_us=500000
            //        not epoch_sec=-1, sub_sec_us=-500000
            // Uses div-then-adjust pattern to avoid signed overflow near INT64_MIN.
            std::time_t epoch_sec =
                current_ts_us / 1000000 - (current_ts_us % 1000000 < 0 ? 1 : 0);
            int64_t sub_sec_us = current_ts_us - epoch_sec * 1000000;
            struct std::tm tm_buf;
            if (::gmtime_r(&epoch_sec, &tm_buf) == nullptr) {
                ThrowInfo(OpTypeInvalid,
                          "gmtime_r failed for timestamp {} us",
                          current_ts_us);
            }
            // Apply interval fields using int64_t intermediate arithmetic
            // to avoid int overflow UB, then validate range before assigning
            // back to struct tm (which uses int fields).
            auto safe_add = [](int base, int64_t delta) -> int {
                int64_t result = static_cast<int64_t>(base) + delta;
                AssertInfo(result >= INT_MIN && result <= INT_MAX,
                           "timestamp interval arithmetic overflow: "
                           "{} + {} = {}",
                           base,
                           delta,
                           result);
                return static_cast<int>(result);
            };
            // Cast to int64_t before multiplying to avoid int * int overflow
            // (e.g. interval.years() == INT32_MIN, op_sign == -1).
            tm_buf.tm_year =
                safe_add(tm_buf.tm_year,
                         static_cast<int64_t>(interval.years()) * op_sign);
            tm_buf.tm_mon =
                safe_add(tm_buf.tm_mon,
                         static_cast<int64_t>(interval.months()) * op_sign);
            tm_buf.tm_mday =
                safe_add(tm_buf.tm_mday,
                         static_cast<int64_t>(interval.days()) * op_sign);
            tm_buf.tm_hour =
                safe_add(tm_buf.tm_hour,
                         static_cast<int64_t>(interval.hours()) * op_sign);
            tm_buf.tm_min =
                safe_add(tm_buf.tm_min,
                         static_cast<int64_t>(interval.minutes()) * op_sign);
            tm_buf.tm_sec =
                safe_add(tm_buf.tm_sec,
                         static_cast<int64_t>(interval.seconds()) * op_sign);
            // timegm normalizes the tm fields and converts back to epoch.
            // It succeeds for all normalized inputs from gmtime_r + safe_add.
            // No -1 check: -1 is a valid epoch second (1969-12-31T23:59:59Z)
            // and is reachable via legal interval arithmetic (e.g., epoch 0 - 1s).
            std::time_t new_epoch_sec = ::timegm(&tm_buf);
            // Guard against overflow in epoch_sec * 1000000.
            // INT64_MAX / 1000000 ≈ ±9.2e12 sec ≈ ±292,271 years from epoch.
            constexpr int64_t kMaxSec = (INT64_MAX - 999999) / 1000000;
            constexpr int64_t kMinSec = (INT64_MIN + 999999) / 1000000;
            AssertInfo(
                new_epoch_sec >= kMinSec && new_epoch_sec <= kMaxSec,
                "timestamp after interval arithmetic out of representable "
                "range: {} seconds from epoch",
                static_cast<int64_t>(new_epoch_sec));
            // Restore sub-second microseconds from the original timestamp
            int64_t final_us =
                static_cast<int64_t>(new_epoch_sec) * 1000000 + sub_sec_us;
            bool match = false;
            switch (compare_op) {
                case proto::plan::OpType::Equal:
                    match = (final_us == compare_us);
                    break;
                case proto::plan::OpType::NotEqual:
                    match = (final_us != compare_us);
                    break;
                case proto::plan::OpType::GreaterThan:
                    match = (final_us > compare_us);
                    break;
                case proto::plan::OpType::GreaterEqual:
                    match = (final_us >= compare_us);
                    break;
                case proto::plan::OpType::LessThan:
                    match = (final_us < compare_us);
                    break;
                case proto::plan::OpType::LessEqual:
                    match = (final_us <= compare_us);
                    break;
                default:  // Should not happen
                    ThrowInfo(OpTypeInvalid,
                              "Unsupported compare op for "
                              "timestamptz_arith_compare_expr");
            }
            res[i] = match;
        }
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<T>(exec_sub_batch,
                                                 std::nullptr_t{},
                                                 input,
                                                 res,
                                                 valid_res,
                                                 compare_value,
                                                 interval);
    } else {
        processed_size = ProcessDataChunks<T>(exec_sub_batch,
                                              std::nullptr_t{},
                                              res,
                                              valid_res,
                                              compare_value,
                                              interval);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);

    return res_vec;
}

}  // namespace exec
}  // namespace milvus