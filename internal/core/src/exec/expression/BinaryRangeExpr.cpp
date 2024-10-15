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

#include "BinaryRangeExpr.h"

#include "query/Utils.h"

namespace milvus {
namespace exec {

void
PhyBinaryRangeFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    switch (expr_->column_.data_type_) {
        case DataType::BOOL: {
            result = ExecRangeVisitorImpl<bool>();
            break;
        }
        case DataType::INT8: {
            result = ExecRangeVisitorImpl<int8_t>();
            break;
        }
        case DataType::INT16: {
            result = ExecRangeVisitorImpl<int16_t>();
            break;
        }
        case DataType::INT32: {
            result = ExecRangeVisitorImpl<int32_t>();
            break;
        }
        case DataType::INT64: {
            result = ExecRangeVisitorImpl<int64_t>();
            break;
        }
        case DataType::FLOAT: {
            result = ExecRangeVisitorImpl<float>();
            break;
        }
        case DataType::DOUBLE: {
            result = ExecRangeVisitorImpl<double>();
            break;
        }
        case DataType::VARCHAR: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                result = ExecRangeVisitorImpl<std::string>();
            } else {
                result = ExecRangeVisitorImpl<std::string_view>();
            }
            break;
        }
        case DataType::JSON: {
            auto value_type = expr_->lower_val_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    result = ExecRangeVisitorImplForJson<int64_t>();
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    result = ExecRangeVisitorImplForJson<double>();
                    break;
                }
                case proto::plan::GenericValue::ValCase::kStringVal: {
                    result = ExecRangeVisitorImplForJson<std::string>();
                    break;
                }
                default: {
                    PanicInfo(
                        DataTypeInvalid,
                        fmt::format("unsupported value type {} in expression",
                                    value_type));
                }
            }
            break;
        }
        case DataType::ARRAY: {
            auto value_type = expr_->lower_val_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<int64_t>();
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<double>();
                    break;
                }
                case proto::plan::GenericValue::ValCase::kStringVal: {
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplForArray<std::string>();
                    break;
                }
                default: {
                    PanicInfo(
                        DataTypeInvalid,
                        fmt::format("unsupported value type {} in expression",
                                    value_type));
                }
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImpl() {
    if (is_index_mode_) {
        return ExecRangeVisitorImplForIndex<T>();
    } else {
        return ExecRangeVisitorImplForData<T>();
    }
}

template <typename T, typename IndexInnerType, typename HighPrecisionType>
ColumnVectorPtr
PhyBinaryRangeFilterExpr::PreCheckOverflow(HighPrecisionType& val1,
                                           HighPrecisionType& val2,
                                           bool& lower_inclusive,
                                           bool& upper_inclusive) {
    lower_inclusive = expr_->lower_inclusive_;
    upper_inclusive = expr_->upper_inclusive_;
    val1 = GetValueFromProto<HighPrecisionType>(expr_->lower_val_);
    val2 = GetValueFromProto<HighPrecisionType>(expr_->upper_val_);
    auto get_next_overflow_batch = [this]() -> ColumnVectorPtr {
        int64_t batch_size = overflow_check_pos_ + batch_size_ >= active_count_
                                 ? active_count_ - overflow_check_pos_
                                 : batch_size_;
        overflow_check_pos_ += batch_size;
        if (cached_overflow_res_ != nullptr &&
            cached_overflow_res_->size() == batch_size) {
            return cached_overflow_res_;
        }
        auto res = std::make_shared<ColumnVector>(TargetBitmap(batch_size));
        return res;
    };

    if constexpr (std::is_integral_v<T> && !std::is_same_v<bool, T>) {
        if (milvus::query::gt_ub<T>(val1)) {
            return get_next_overflow_batch();
        } else if (milvus::query::lt_lb<T>(val1)) {
            val1 = std::numeric_limits<T>::min();
            lower_inclusive = true;
        }

        if (milvus::query::gt_ub<T>(val2)) {
            val2 = std::numeric_limits<T>::max();
            upper_inclusive = true;
        } else if (milvus::query::lt_lb<T>(val2)) {
            return get_next_overflow_batch();
        }
    }
    return nullptr;
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForIndex() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    typedef std::conditional_t<std::is_integral_v<IndexInnerType> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               IndexInnerType>
        HighPrecisionType;

    HighPrecisionType val1;
    HighPrecisionType val2;
    bool lower_inclusive = false;
    bool upper_inclusive = false;
    if (auto res =
            PreCheckOverflow<T>(val1, val2, lower_inclusive, upper_inclusive)) {
        return res;
    }

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto execute_sub_batch =
        [lower_inclusive, upper_inclusive](
            Index* index_ptr, HighPrecisionType val1, HighPrecisionType val2) {
            BinaryRangeIndexFunc<T> func;
            return std::move(
                func(index_ptr, val1, val2, lower_inclusive, upper_inclusive));
        };
    auto res = ProcessIndexChunks<T>(execute_sub_batch, val1, val2);
    AssertInfo(res.size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res.size(),
               real_batch_size);
    return std::make_shared<ColumnVector>(std::move(res));
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForData() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    typedef std::conditional_t<std::is_integral_v<IndexInnerType> &&
                                   !std::is_same_v<bool, T>,
                               int64_t,
                               IndexInnerType>
        HighPrecisionType;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    HighPrecisionType val1;
    HighPrecisionType val2;
    bool lower_inclusive = false;
    bool upper_inclusive = false;
    if (auto res =
            PreCheckOverflow<T>(val1, val2, lower_inclusive, upper_inclusive)) {
        return res;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    auto execute_sub_batch = [lower_inclusive, upper_inclusive](
                                 const T* data,
                                 const bool* valid_data,
                                 const int size,
                                 TargetBitmapView res,
                                 HighPrecisionType val1,
                                 HighPrecisionType val2) {
        if (lower_inclusive && upper_inclusive) {
            BinaryRangeElementFunc<T, true, true> func;
            func(val1, val2, data, valid_data, size, res);
        } else if (lower_inclusive && !upper_inclusive) {
            BinaryRangeElementFunc<T, true, false> func;
            func(val1, val2, data, valid_data, size, res);
        } else if (!lower_inclusive && upper_inclusive) {
            BinaryRangeElementFunc<T, false, true> func;
            func(val1, val2, data, valid_data, size, res);
        } else {
            BinaryRangeElementFunc<T, false, false> func;
            func(val1, val2, data, valid_data, size, res);
        }
    };
    auto skip_index_func =
        [val1, val2, lower_inclusive, upper_inclusive](
            const SkipIndex& skip_index, FieldId field_id, int64_t chunk_id) {
            if (lower_inclusive && upper_inclusive) {
                return skip_index.CanSkipBinaryRange<T>(
                    field_id, chunk_id, val1, val2, true, true);
            } else if (lower_inclusive && !upper_inclusive) {
                return skip_index.CanSkipBinaryRange<T>(
                    field_id, chunk_id, val1, val2, true, false);
            } else if (!lower_inclusive && upper_inclusive) {
                return skip_index.CanSkipBinaryRange<T>(
                    field_id, chunk_id, val1, val2, false, true);
            } else {
                return skip_index.CanSkipBinaryRange<T>(
                    field_id, chunk_id, val1, val2, false, false);
            }
        };
    int64_t processed_size = ProcessDataChunks<T>(
        execute_sub_batch, skip_index_func, res, val1, val2);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForJson() {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    bool lower_inclusive = expr_->lower_inclusive_;
    bool upper_inclusive = expr_->upper_inclusive_;
    ValueType val1 = GetValueFromProto<ValueType>(expr_->lower_val_);
    ValueType val2 = GetValueFromProto<ValueType>(expr_->upper_val_);
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    auto execute_sub_batch = [lower_inclusive, upper_inclusive, pointer](
                                 const milvus::Json* data,
                                 const bool* valid_data,
                                 const int size,
                                 TargetBitmapView res,
                                 ValueType val1,
                                 ValueType val2) {
        if (lower_inclusive && upper_inclusive) {
            BinaryRangeElementFuncForJson<ValueType, true, true> func;
            func(val1, val2, pointer, data, valid_data, size, res);
        } else if (lower_inclusive && !upper_inclusive) {
            BinaryRangeElementFuncForJson<ValueType, true, false> func;
            func(val1, val2, pointer, data, valid_data, size, res);
        } else if (!lower_inclusive && upper_inclusive) {
            BinaryRangeElementFuncForJson<ValueType, false, true> func;
            func(val1, val2, pointer, data, valid_data, size, res);
        } else {
            BinaryRangeElementFuncForJson<ValueType, false, false> func;
            func(val1, val2, pointer, data, valid_data, size, res);
        }
    };
    int64_t processed_size = ProcessDataChunks<milvus::Json>(
        execute_sub_batch, std::nullptr_t{}, res, val1, val2);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForArray() {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    bool lower_inclusive = expr_->lower_inclusive_;
    bool upper_inclusive = expr_->upper_inclusive_;
    ValueType val1 = GetValueFromProto<ValueType>(expr_->lower_val_);
    ValueType val2 = GetValueFromProto<ValueType>(expr_->upper_val_);
    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }

    auto execute_sub_batch = [lower_inclusive, upper_inclusive](
                                 const milvus::ArrayView* data,
                                 const bool* valid_data,
                                 const int size,
                                 TargetBitmapView res,
                                 ValueType val1,
                                 ValueType val2,
                                 int index) {
        if (lower_inclusive && upper_inclusive) {
            BinaryRangeElementFuncForArray<ValueType, true, true> func;
            func(val1, val2, index, data, valid_data, size, res);
        } else if (lower_inclusive && !upper_inclusive) {
            BinaryRangeElementFuncForArray<ValueType, true, false> func;
            func(val1, val2, index, data, valid_data, size, res);
        } else if (!lower_inclusive && upper_inclusive) {
            BinaryRangeElementFuncForArray<ValueType, false, true> func;
            func(val1, val2, index, data, valid_data, size, res);
        } else {
            BinaryRangeElementFuncForArray<ValueType, false, false> func;
            func(val1, val2, index, data, valid_data, size, res);
        }
    };
    int64_t processed_size = ProcessDataChunks<milvus::ArrayView>(
        execute_sub_batch, std::nullptr_t{}, res, val1, val2, index);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

}  //namespace exec
}  // namespace milvus
