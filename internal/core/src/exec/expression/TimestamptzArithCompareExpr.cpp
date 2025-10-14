#include "TimestamptzArithCompareExpr.h"
#include <cstddef>
#include <memory>
#include "absl/time/civil_time.h"
#include "absl/time/time.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"
#include "BinaryArithOpEvalRangeExpr.h"

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
                    expr_->timestamp_column_,
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
        absl::TimeZone utc = absl::UTCTimeZone();
        absl::Time compare_t = absl::FromUnixMicros(compare_value);
        for (int i = 0; i < size; ++i) {
            auto offset = (offsets) ? offsets[i] : i;
            const int64_t current_ts_us = data[i];
            const int op_sign =
                (arith_op == proto::plan::ArithOpType::Add) ? 1 : -1;
            absl::Time t = absl::FromUnixMicros(current_ts_us);
            // CivilSecond can handle calendar time for us
            absl::CivilSecond cs = absl::ToCivilSecond(t, utc);
            absl::CivilSecond new_cs(
                cs.year() + (interval.years() * op_sign),
                cs.month() + (interval.months() * op_sign),
                cs.day() + (interval.days() * op_sign),
                cs.hour() + (interval.hours() * op_sign),
                cs.minute() + (interval.minutes() * op_sign),
                cs.second() + (interval.seconds() * op_sign));
            absl::Time final_time = absl::FromCivil(new_cs, utc);
            bool match = false;
            switch (compare_op) {
                case proto::plan::OpType::Equal:
                    match = (final_time == compare_t);
                    break;
                case proto::plan::OpType::NotEqual:
                    match = (final_time != compare_t);
                    break;
                case proto::plan::OpType::GreaterThan:
                    match = (final_time > compare_t);
                    break;
                case proto::plan::OpType::GreaterEqual:
                    match = (final_time >= compare_t);
                    break;
                case proto::plan::OpType::LessThan:
                    match = (final_time < compare_t);
                    break;
                case proto::plan::OpType::LessEqual:
                    match = (final_time <= compare_t);
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