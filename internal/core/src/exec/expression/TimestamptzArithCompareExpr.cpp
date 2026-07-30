#include "TimestamptzArithCompareExpr.h"

#include <climits>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <memory>
#include <variant>

#include "bitset/bitset.h"
#include "common/EasyAssert.h"
#include "common/Types.h"
#include "common/Vector.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"
#include "storage/PrefetchThreadPool.h"

namespace milvus {
namespace exec {

namespace {

int
SafeAddTimestampField(int base, int64_t delta) {
    const int64_t min_delta = static_cast<int64_t>(INT_MIN) - base;
    const int64_t max_delta = static_cast<int64_t>(INT_MAX) - base;
    AssertInfo(delta >= min_delta && delta <= max_delta,
               "timestamp interval arithmetic overflow: {} + {} = {}",
               base,
               delta,
               delta < 0 ? "less than INT_MIN" : "greater than INT_MAX");
    return static_cast<int>(static_cast<int64_t>(base) + delta);
}

int64_t
ApplyIntervalSign(int64_t value, int op_sign) {
    if (op_sign > 0) {
        return value;
    }
    AssertInfo(value != INT64_MIN,
               "timestamp interval arithmetic overflow: cannot negate {}",
               value);
    return -value;
}

int64_t
ApplyTimestampInterval(int64_t current_ts_us,
                       proto::plan::ArithOpType arith_op,
                       const proto::plan::Interval& interval) {
    int op_sign;
    switch (arith_op) {
        case proto::plan::ArithOpType::Add:
            op_sign = 1;
            break;
        case proto::plan::ArithOpType::Sub:
            op_sign = -1;
            break;
        case proto::plan::ArithOpType::Unknown:
            return current_ts_us;
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported arithmetic op for "
                      "timestamptz_arith_compare_expr: {}",
                      arith_op);
    }

    // Floor division to correctly handle pre-epoch (negative) timestamps.
    // Keep the remainder separate so INT64_MIN does not overflow while
    // reconstructing the sub-second component.
    int64_t epoch_sec = current_ts_us / 1000000;
    int64_t sub_sec_us = current_ts_us % 1000000;
    if (sub_sec_us < 0) {
        --epoch_sec;
        sub_sec_us += 1000000;
    }

    std::time_t epoch_time = epoch_sec;
    struct std::tm tm_buf;
    if (::gmtime_r(&epoch_time, &tm_buf) == nullptr) {
        ThrowInfo(UnexpectedError,
                  "gmtime_r failed for timestamp {} us",
                  current_ts_us);
    }

    // Apply the operation sign without overflowing when an INT64_MIN interval
    // is subtracted.
    tm_buf.tm_year = SafeAddTimestampField(
        tm_buf.tm_year, ApplyIntervalSign(interval.years(), op_sign));
    tm_buf.tm_mon = SafeAddTimestampField(
        tm_buf.tm_mon, ApplyIntervalSign(interval.months(), op_sign));
    tm_buf.tm_mday = SafeAddTimestampField(
        tm_buf.tm_mday, ApplyIntervalSign(interval.days(), op_sign));
    tm_buf.tm_hour = SafeAddTimestampField(
        tm_buf.tm_hour, ApplyIntervalSign(interval.hours(), op_sign));
    tm_buf.tm_min = SafeAddTimestampField(
        tm_buf.tm_min, ApplyIntervalSign(interval.minutes(), op_sign));
    tm_buf.tm_sec = SafeAddTimestampField(
        tm_buf.tm_sec, ApplyIntervalSign(interval.seconds(), op_sign));

    // timegm normalizes the fields. -1 is a valid epoch second, so it cannot
    // be used as an error sentinel here.
    const std::time_t new_epoch_time = ::timegm(&tm_buf);
    const auto new_epoch_sec = static_cast<int64_t>(new_epoch_time);
    constexpr int64_t kMaxSec = (INT64_MAX - 999999) / 1000000;
    constexpr int64_t kMinSec = (INT64_MIN + 999999) / 1000000;
    AssertInfo(new_epoch_sec >= kMinSec && new_epoch_sec <= kMaxSec,
               "timestamp after interval arithmetic out of representable "
               "range: {} seconds from epoch",
               new_epoch_sec);
    return new_epoch_sec * 1000000 + sub_sec_us;
}

bool
CompareTimestamp(int64_t lhs, int64_t rhs, proto::plan::OpType compare_op) {
    switch (compare_op) {
        case proto::plan::OpType::Equal:
            return lhs == rhs;
        case proto::plan::OpType::NotEqual:
            return lhs != rhs;
        case proto::plan::OpType::GreaterThan:
            return lhs > rhs;
        case proto::plan::OpType::GreaterEqual:
            return lhs >= rhs;
        case proto::plan::OpType::LessThan:
            return lhs < rhs;
        case proto::plan::OpType::LessEqual:
            return lhs <= rhs;
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported compare op for "
                      "timestamptz_arith_compare_expr: {}",
                      compare_op);
    }
}

bool
EvaluateTimestamp(int64_t current_ts_us,
                  proto::plan::ArithOpType arith_op,
                  const proto::plan::Interval& interval,
                  proto::plan::OpType compare_op,
                  int64_t compare_us) {
    const auto final_us =
        ApplyTimestampInterval(current_ts_us, arith_op, interval);
    return CompareTimestamp(final_us, compare_us, compare_op);
}

}  // namespace

std::string
PhyTimestamptzArithCompareExpr::ToString() const {
    return expr_->ToString();
}

void
PhyTimestamptzArithCompareExpr::DetermineExecPath() {
    // Prefer field data when it is present. TIMESTAMPTZ calendar arithmetic
    // cannot be transformed into a scalar-index range query, and keeping the
    // raw-data path also preserves its batch cursor when another raw predicate
    // is evaluated alongside it.
    if (num_data_chunk_ > 0 || active_count_ == 0) {
        exec_path_ = ExprExecPath::RawData;
        return;
    }

    // An index-only sealed segment can still evaluate the expression if its
    // scalar index retains the original values for Reverse_Lookup().
    SegmentExpr::DetermineExecPath();
    if (exec_path_ == ExprExecPath::ScalarIndex && num_index_chunk_ == 1) {
        using Index = index::ScalarIndex<int64_t>;
        auto index_ptr = dynamic_cast<const Index*>(pinned_index_[0].get());
        if (index_ptr != nullptr && index_ptr->HasRawData()) {
            return;
        }
    }

    ThrowInfo(UnexpectedError,
              "TIMESTAMPTZ arithmetic comparison requires field data or a "
              "scalar index with raw data for field {}",
              field_id_.get());
}

void
PhyTimestamptzArithCompareExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    result = ExecCompareVisitorImpl<int64_t>(input);
}

template <typename T>
VectorPtr
PhyTimestamptzArithCompareExpr::ExecCompareVisitorImpl(OffsetVector* input) {
    if (exec_path_ == ExprExecPath::ScalarIndex) {
        return ExecCompareVisitorImplForIndex<T>(input);
    }
    return ExecCompareVisitorImplForAll<T>(input);
}

template <typename T>
VectorPtr
PhyTimestamptzArithCompareExpr::ExecCompareVisitorImplForIndex(
    OffsetVector* input) {
    using Index = index::ScalarIndex<T>;
    if (!arg_inited_) {
        interval_ = expr_->interval_;
        compare_value_.SetValue<T>(expr_->compare_value_);
        arg_inited_ = true;
    }

    const auto arith_op = expr_->arith_op_;
    const auto compare_op = expr_->compare_op_;
    const auto compare_value = compare_value_.GetValue<T>();
    const auto interval = interval_;
    const int64_t real_batch_size =
        input != nullptr ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    const int64_t eval_size =
        input != nullptr ? real_batch_size : active_count_;

    auto exec_index =
        [ arith_op,
          compare_op ]<FilterType filter_type = FilterType::sequential>(
            Index * index_ptr,
            int64_t size,
            T compare_value,
            const proto::plan::Interval& interval,
            const int32_t* offsets = nullptr) {
        TargetBitmap result(size, false);
        for (int64_t i = 0; i < size; ++i) {
            size_t offset = i;
            if constexpr (filter_type == FilterType::random) {
                offset = offsets == nullptr ? i : offsets[i];
            }
            auto raw = index_ptr->Reverse_Lookup(offset);
            if (raw.has_value()) {
                result[i] = EvaluateTimestamp(
                    raw.value(), arith_op, interval, compare_op, compare_value);
            }
        }
        return result;
    };

    VectorPtr result;
    if (input != nullptr) {
        result = ProcessIndexChunksByOffsets<T>(
            exec_index, input, eval_size, compare_value, interval);
    } else {
        result = ProcessIndexChunks<T>(
            exec_index, eval_size, compare_value, interval);
    }
    AssertInfo(result != nullptr && result->size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               result == nullptr ? 0 : result->size(),
               real_batch_size);
    return result;
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
        if (data == nullptr) {
            return;
        }
        const int64_t compare_us = compare_value;
        for (int i = 0; i < size; ++i) {
            if (valid_data != nullptr && !valid_data[i]) {
                // NULL never matches, under either polarity (three-valued
                // logic); do not evaluate the storage placeholder value.
                res[i] = valid_res[i] = false;
                continue;
            }
            res[i] = EvaluateTimestamp(
                data[i], arith_op, interval, compare_op, compare_us);
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
