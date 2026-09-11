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

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

#include "common/EasyAssert.h"
#include "common/Tracer.h"
#include "common/bson_view.h"
#include "common/type_c.h"
#include "common/ScopedTimer.h"
#include "exec/expression/JsonNumberComparison.h"
#include "exec/expression/Utils.h"
#include "fmt/core.h"
#include "folly/FBVector.h"
#include "glog/logging.h"
#include "index/SkipIndex.h"
#include "monitor/Monitor.h"
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/utils.h"
#include "log/Log.h"
#include "opentelemetry/trace/span.h"
#include "query/Utils.h"
#include "segcore/SegmentInterface.h"
#include "segcore/SegmentSealed.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"

namespace milvus {

namespace exec {

void
PhyBinaryRangeFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    WaitPrefetch();
    tracer::AutoSpan span(
        "PhyBinaryRangeFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.SetAttribute("data_type", static_cast<int>(expr_->column_.data_type_));

    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));

    auto data_type = expr_->column_.data_type_;
    if (expr_->column_.element_level_) {
        data_type = expr_->column_.element_type_;
    }
    switch (data_type) {
        case DataType::BOOL: {
            result = ExecRangeVisitorImpl<bool>(context);
            break;
        }
        case DataType::INT8: {
            result = ExecRangeVisitorImpl<int8_t>(context);
            break;
        }
        case DataType::INT16: {
            result = ExecRangeVisitorImpl<int16_t>(context);
            break;
        }
        case DataType::INT32: {
            result = ExecRangeVisitorImpl<int32_t>(context);
            break;
        }
        case DataType::INT64: {
            result = ExecRangeVisitorImpl<int64_t>(context);
            break;
        }
        case DataType::FLOAT: {
            result = ExecRangeVisitorImpl<float>(context);
            break;
        }
        case DataType::DOUBLE: {
            result = ExecRangeVisitorImpl<double>(context);
            break;
        }
        case DataType::VARCHAR: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                result = ExecRangeVisitorImpl<std::string>(context);
            } else {
                result = ExecRangeVisitorImpl<std::string_view>(context);
            }
            break;
        }
        case DataType::JSON: {
            span.SetAttribute("json_filter_expr_type", "binary_range");
            auto lower_type = expr_->lower_val_.val_case();
            auto upper_type = expr_->upper_val_.val_case();
            // For numeric types, if either bound is float, use double for both.
            // This handles mixed int64/float cases properly.
            bool use_double =
                (lower_type == proto::plan::GenericValue::ValCase::kFloatVal ||
                 upper_type == proto::plan::GenericValue::ValCase::kFloatVal);
            bool is_numeric =
                ((lower_type == proto::plan::GenericValue::ValCase::kInt64Val ||
                  lower_type ==
                      proto::plan::GenericValue::ValCase::kFloatVal) &&
                 (upper_type == proto::plan::GenericValue::ValCase::kInt64Val ||
                  upper_type == proto::plan::GenericValue::ValCase::kFloatVal));
            const auto requires_precise_int64_comparison =
                is_numeric && (JsonNumericBoundRequiresPreciseInt64Comparison(
                                   expr_->lower_val_) ||
                               JsonNumericBoundRequiresPreciseInt64Comparison(
                                   expr_->upper_val_));

            // Keep sparse JsonFlat offset batches candidate-local when raw
            // JSON is resident.  An index-only segment cannot use the generic
            // JSON reverse-lookup path, so it must query the typed JsonFlat
            // executor and gather the requested rows instead.
            const bool use_json_flat_raw_offsets =
                exec_path_ == ExprExecPath::ScalarIndex && has_offset_input_ &&
                PinnedJsonIndexIsFlat() && num_data_chunk_ > 0;
            if (exec_path_ == ExprExecPath::ScalarIndex &&
                !use_json_flat_raw_offsets) {
                if (is_numeric) {
                    if (!use_double && PinnedJsonIndexIsFlat()) {
                        result = ExecRangeVisitorImplForIndex<int64_t>(input);
                    } else {
                        proto::plan::GenericValue double_lower_val;
                        if (lower_type ==
                            proto::plan::GenericValue::ValCase::kInt64Val) {
                            double_lower_val.set_float_val(static_cast<double>(
                                expr_->lower_val_.int64_val()));
                        } else {
                            double_lower_val.set_float_val(
                                expr_->lower_val_.float_val());
                        }
                        proto::plan::GenericValue double_upper_val;
                        if (upper_type ==
                            proto::plan::GenericValue::ValCase::kInt64Val) {
                            double_upper_val.set_float_val(static_cast<double>(
                                expr_->upper_val_.int64_val()));
                        } else {
                            double_upper_val.set_float_val(
                                expr_->upper_val_.float_val());
                        }

                        lower_arg_.SetValue<double>(double_lower_val);
                        upper_arg_.SetValue<double>(double_upper_val);
                        arg_inited_ = true;

                        result = ExecRangeVisitorImplForIndex<double>(input);
                    }
                } else if (lower_type ==
                           proto::plan::GenericValue::ValCase::kStringVal) {
                    result = ExecRangeVisitorImplForIndex<std::string>(input);
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format("unsupported value type {} in expression",
                                    lower_type));
                }
            } else {
                if (requires_precise_int64_comparison &&
                    exec_path_ != ExprExecPath::JsonStats) {
                    result = ExecRangeVisitorImplForJsonPreciseNumeric(context);
                } else if (is_numeric && use_double) {
                    // Use double when either bound is float
                    result = ExecRangeVisitorImplForJson<double>(context);
                } else if (lower_type ==
                           proto::plan::GenericValue::ValCase::kInt64Val) {
                    result = ExecRangeVisitorImplForJson<int64_t>(context);
                } else if (lower_type ==
                           proto::plan::GenericValue::ValCase::kFloatVal) {
                    result = ExecRangeVisitorImplForJson<double>(context);
                } else if (lower_type ==
                           proto::plan::GenericValue::ValCase::kStringVal) {
                    result = ExecRangeVisitorImplForJson<std::string>(context);
                } else {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format("unsupported value type {} in expression",
                                    lower_type));
                }
            }
            break;
        }
        case DataType::ARRAY: {
            auto value_type = expr_->lower_val_.val_case();
            switch (value_type) {
                case proto::plan::GenericValue::ValCase::kInt64Val: {
                    result = ExecRangeVisitorImplForArray<int64_t>(context);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kFloatVal: {
                    result = ExecRangeVisitorImplForArray<double>(context);
                    break;
                }
                case proto::plan::GenericValue::ValCase::kStringVal: {
                    result = ExecRangeVisitorImplForArray<std::string>(context);
                    break;
                }
                default: {
                    ThrowInfo(
                        UnexpectedError,
                        fmt::format("unsupported value type {} in expression",
                                    value_type));
                }
            }
            break;
        }
        default:
            ThrowInfo(UnexpectedError,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForJsonPreciseNumeric(
    EvalCtx& context) {
    return EvalKernel<milvus::Json>(
        context,
        BinaryRangeJsonPreciseNumericKernel{
            .lower_bound = expr_->lower_val_,
            .upper_bound = expr_->upper_val_,
            .lower_inclusive = expr_->lower_inclusive_,
            .upper_inclusive = expr_->upper_inclusive_,
            .pointer = milvus::Json::pointer(expr_->column_.nested_path_),
        },
        false);
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImpl(EvalCtx& context) {
    if (!has_offset_input_ && exec_path_ == ExprExecPath::PkIndex) {
        if (pk_type_ == DataType::VARCHAR) {
            return ExecRangeVisitorImplForPk<std::string_view>(context);
        } else {
            return ExecRangeVisitorImplForPk<int64_t>(context);
        }
    }

    if (exec_path_ == ExprExecPath::ScalarIndex && !has_offset_input_) {
        return ExecRangeVisitorImplForIndex<T>();
    } else {
        return ExecRangeVisitorImplForData<T>(context);
    }
}

template <typename T>
BinaryRangeBounds<T>
PhyBinaryRangeFilterExpr::GetBinaryRangeBounds() {
    using HighPrecisionType = BinaryRangeHighPrecisionType<T>;
    if (!arg_inited_) {
        lower_arg_.SetValue<HighPrecisionType>(expr_->lower_val_);
        upper_arg_.SetValue<HighPrecisionType>(expr_->upper_val_);
        arg_inited_ = true;
    }
    return ClampBinaryRangeBounds<T>(lower_arg_.GetValue<HighPrecisionType>(),
                                     upper_arg_.GetValue<HighPrecisionType>(),
                                     expr_->lower_inclusive_,
                                     expr_->upper_inclusive_);
}

template <typename T>
ColumnVectorPtr
PhyBinaryRangeFilterExpr::IndexOverflowBatch(int64_t batch_size,
                                             OffsetVector* input) {
    TargetBitmap valid_res;
    if (expr_->column_.element_level_) {
        valid_res = TargetBitmap(batch_size, true);
        if (input == nullptr) {
            MoveCursor();
        }
    } else if (input != nullptr) {
        valid_res = ProcessChunksForValidByOffsets<T>(UseIndexCursor(), *input);
    } else {
        valid_res = ProcessChunksForValid<T>(UseIndexCursor());
    }
    return std::make_shared<ColumnVector>(TargetBitmap(batch_size),
                                          std::move(valid_res));
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForIndex(OffsetVector* input) {
    using Index = index::ScalarIndex<BinaryRangeIndexInnerType<T>>;
    using HighPrecisionType = BinaryRangeHighPrecisionType<T>;

    auto next_batch_size =
        GetNextRealBatchSize(input, expr_->column_.element_level_);
    if (!next_batch_size.has_value()) {
        return nullptr;
    }
    auto real_batch_size = *next_batch_size;
    if (auto res = AdvanceEmptyElementBatch(
            input, expr_->column_.element_level_, real_batch_size)) {
        return res;
    }
    const auto bounds = GetBinaryRangeBounds<T>();
    if (bounds.always_false) {
        return IndexOverflowBatch<T>(real_batch_size, input);
    }
    const HighPrecisionType val1 = bounds.lower;
    const HighPrecisionType val2 = bounds.upper;
    const bool lower_inclusive = bounds.lower_inclusive;
    const bool upper_inclusive = bounds.upper_inclusive;

    auto execute_sub_batch = [lower_inclusive, upper_inclusive](
                                 Index* index_ptr,
                                 HighPrecisionType val1,
                                 HighPrecisionType val2) {
        BinaryRangeIndexFunc<T> func;
        return func(index_ptr, val1, val2, lower_inclusive, upper_inclusive);
    };
    if (input != nullptr) {
        if (PinnedJsonIndexIsFlat()) {
            return ProcessIndexChunksAndGatherByOffsets<T>(
                execute_sub_batch, *input, val1, val2);
        }
        if (cached_result_ == nullptr) {
            auto scalar_index =
                dynamic_cast<const Index*>(pinned_index_[0].get());
            AssertInfo(scalar_index != nullptr, "invalid scalar index type");
            auto* index_ptr = const_cast<Index*>(scalar_index);
            cached_result_ = std::make_shared<TargetBitmap>(
                execute_sub_batch(index_ptr, val1, val2));
            cached_valid_result_ = std::make_shared<TargetBitmap>(
                GetCachedIndexValidBitmap(index_ptr).clone());
            AssertInfo(
                cached_result_->size() == static_cast<size_t>(active_count_),
                "index range result size {} does not match row count {}",
                cached_result_->size(),
                active_count_);
        }
        return GatherCachedResultByOffsets(
            *cached_result_, *cached_valid_result_, *input);
    }
    auto res = ProcessIndexChunks<T>(execute_sub_batch, val1, val2);
    AssertInfo(res->size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res->size(),
               real_batch_size);
    return res;
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForData(EvalCtx& context) {
    return EvalKernel<T>(
        context,
        BinaryRangeKernel<T>::FromBounds(GetBinaryRangeBounds<T>(), op_ctx_),
        expr_->column_.element_level_);
}

template <typename ValueType>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForJson(EvalCtx& context) {
    if (exec_path_ == ExprExecPath::JsonStats) {
        milvus::ScopedTimer timer(
            "binary_range_json_by_stats",
            [this](double us) { json_filter_stats_latency_us_ += us; });
        return ExecRangeVisitorImplForJsonStats<ValueType>(
            context.get_offset_input());
    }

    milvus::ScopedTimer timer(
        "binary_range_json_bruteforce",
        [this](double us) { json_filter_bruteforce_latency_us_ += us; });

    if (!arg_inited_) {
        lower_arg_.SetValue<ValueType>(expr_->lower_val_);
        upper_arg_.SetValue<ValueType>(expr_->upper_val_);
        arg_inited_ = true;
    }
    return EvalKernel<milvus::Json>(
        context,
        BinaryRangeJsonKernel<ValueType>{
            .lower = lower_arg_.GetValue<ValueType>(),
            .upper = upper_arg_.GetValue<ValueType>(),
            .lower_inclusive = expr_->lower_inclusive_,
            .upper_inclusive = expr_->upper_inclusive_,
            .pointer = milvus::Json::pointer(expr_->column_.nested_path_),
        },
        false);
}

template <typename ValueType>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForJsonStats(
    OffsetVector* input) {
    using GetType = std::conditional_t<std::is_same_v<ValueType, std::string>,
                                       std::string_view,
                                       ValueType>;
    auto real_batch_size =
        input != nullptr
            ? input->size()
            : std::min(batch_size_, active_count_ - current_data_global_pos_);
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto pointer = milvus::index::JsonPointer(expr_->column_.nested_path_);
    bool lower_inclusive = expr_->lower_inclusive_;
    bool upper_inclusive = expr_->upper_inclusive_;
    std::optional<ValueType> val1;
    std::optional<ValueType> val2;
    if constexpr (!std::is_same_v<GetType, int64_t> &&
                  !std::is_same_v<GetType, double>) {
        val1.emplace(GetValueWithCastNumber<ValueType>(expr_->lower_val_));
        val2.emplace(GetValueWithCastNumber<ValueType>(expr_->upper_val_));
    }

    if (cached_index_chunk_id_ != 0 && TryCacheGet()) {
        // Cache hit — skip Stats computation.
    } else if (cached_index_chunk_id_ != 0 &&
               segment_->type() == SegmentType::Sealed) {
        auto cache_compute_start = CacheClock::now();
        auto* segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);

        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data
        const auto& lower_bound = expr_->lower_val_;
        const auto& upper_bound = expr_->upper_val_;
        auto try_execute = [&](milvus::index::JSONType json_type,
                               auto GetType) {
            auto target_field = index->GetShreddingField(pointer, json_type);
            if (!target_field.empty()) {
                using ColType = decltype(GetType);
                TargetBitmap target_res(active_count_, false);
                TargetBitmapView target_res_view(target_res);
                TargetBitmap target_valid(active_count_, true);
                TargetBitmapView target_valid_view(target_valid);
                auto shredding_executor = [val1,
                                           val2,
                                           lower_inclusive,
                                           upper_inclusive,
                                           &lower_bound,
                                           &upper_bound](
                                              const ColType* src,
                                              ValidityView valid,
                                              size_t size,
                                              TargetBitmapView res,
                                              TargetBitmapView valid_res) {
                    for (size_t i = 0; i < size; ++i) {
                        if (valid && !valid[i]) {
                            res[i] = valid_res[i] = false;
                            continue;
                        }
                        if constexpr (std::is_same_v<ColType, int64_t> ||
                                      std::is_same_v<ColType, double>) {
                            auto lower_comparison =
                                CompareJsonNumberToBound(src[i], lower_bound);
                            auto upper_comparison =
                                CompareJsonNumberToBound(src[i], upper_bound);
                            if (!lower_comparison.has_value() ||
                                !upper_comparison.has_value()) {
                                res[i] = false;
                                continue;
                            }
                            const auto lower_matches =
                                lower_inclusive ? *lower_comparison >= 0
                                                : *lower_comparison > 0;
                            const auto upper_matches =
                                upper_inclusive ? *upper_comparison <= 0
                                                : *upper_comparison < 0;
                            res[i] = lower_matches && upper_matches;
                        } else if (lower_inclusive && upper_inclusive) {
                            res[i] = src[i] >= *val1 && src[i] <= *val2;
                        } else if (lower_inclusive && !upper_inclusive) {
                            res[i] = src[i] >= *val1 && src[i] < *val2;
                        } else if (!lower_inclusive && upper_inclusive) {
                            res[i] = src[i] > *val1 && src[i] <= *val2;
                        } else {
                            res[i] = src[i] > *val1 && src[i] < *val2;
                        }
                    }
                };
                index->ExecutorForShreddingData<ColType>(op_ctx_,
                                                         target_field,
                                                         shredding_executor,
                                                         nullptr,
                                                         target_res_view,
                                                         target_valid_view);
                res_view.inplace_or_with_count(target_res_view, active_count_);
                valid_res_view.inplace_or_with_count(target_valid_view,
                                                     active_count_);
                LOG_DEBUG("using shredding data's field: {} count {}",
                          target_field,
                          res_view.count());
            }
        };

        {
            milvus::ScopedTimer timer(
                "binary_range_json_stats_shredding_data",
                [this](double us) { json_stats_shredding_latency_us_ += us; });

            if constexpr (std::is_same_v<GetType, int64_t>) {
                // int64 compare
                try_execute(milvus::index::JSONType::INT64, int64_t{});
                // and double compare
                try_execute(milvus::index::JSONType::DOUBLE, double{});

            } else if constexpr (std::is_same_v<GetType, double>) {
                try_execute(milvus::index::JSONType::DOUBLE, double{});
                // and int64 compare
                try_execute(milvus::index::JSONType::INT64, int64_t{});
            } else if constexpr (std::is_same_v<GetType, std::string_view> ||
                                 std::is_same_v<GetType, std::string>) {
                try_execute(milvus::index::JSONType::STRING,
                            std::string_view{});
            }
        }

        // process shared data
        auto shared_executor = [val1,
                                val2,
                                lower_inclusive,
                                upper_inclusive,
                                &lower_bound,
                                &upper_bound,
                                &res_view,
                                &valid_res_view](milvus::BsonView bson,
                                                 uint32_t row_id,
                                                 uint32_t value_offset) {
            auto set_known = [&](bool value) {
                res_view[row_id] = value;
                valid_res_view[row_id] = true;
            };
            if constexpr (std::is_same_v<GetType, int64_t> ||
                          std::is_same_v<GetType, double>) {
                auto lower_comparison =
                    CompareBsonNumberToBound(bson, value_offset, lower_bound);
                auto upper_comparison =
                    CompareBsonNumberToBound(bson, value_offset, upper_bound);
                if (!lower_comparison.has_value() ||
                    !upper_comparison.has_value()) {
                    return;
                }
                const auto lower_matches = lower_inclusive
                                               ? *lower_comparison >= 0
                                               : *lower_comparison > 0;
                const auto upper_matches = upper_inclusive
                                               ? *upper_comparison <= 0
                                               : *upper_comparison < 0;
                set_known(lower_matches && upper_matches);
            } else {
                auto val = bson.ParseAsValueAtOffset<GetType>(value_offset);
                if (!val.has_value()) {
                    return;
                }
                if (lower_inclusive && upper_inclusive) {
                    set_known(val.value() >= *val1 && val.value() <= *val2);
                } else if (lower_inclusive && !upper_inclusive) {
                    set_known(val.value() >= *val1 && val.value() < *val2);
                } else if (!lower_inclusive && upper_inclusive) {
                    set_known(val.value() > *val1 && val.value() <= *val2);
                } else {
                    set_known(val.value() > *val1 && val.value() < *val2);
                }
            }
        };
        bool skip_shared_data = false;
        if constexpr (std::is_same_v<GetType, int64_t> ||
                      std::is_same_v<GetType, double>) {
            skip_shared_data =
                index->HasAllShreddingFields(pointer,
                                             {milvus::index::JSONType::INT64,
                                              milvus::index::JSONType::DOUBLE});
        } else if constexpr (std::is_same_v<GetType, std::string_view> ||
                             std::is_same_v<GetType, std::string>) {
            skip_shared_data = index->HasAllShreddingFields(
                pointer, {milvus::index::JSONType::STRING});
        }

        if (!skip_shared_data) {
            milvus::ScopedTimer timer(
                "binary_range_json_stats_shared_data",
                [this](double us) { json_stats_shared_latency_us_ += us; });

            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }
        cached_index_chunk_id_ = 0;
        CachePut(CacheElapsedUs(cache_compute_start));
    }

    if (input != nullptr) {
        return GatherCachedResultByOffsets(
            *cached_index_chunk_res_, *cached_index_chunk_valid_res_, *input);
    }
    auto res = MoveOrSliceBitmap(*cached_index_chunk_res_,
                                 *cached_index_chunk_valid_res_,
                                 current_data_global_pos_,
                                 real_batch_size);
    MoveCursor();
    return res;
}  // namespace exec

template <typename ValueType>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForArray(EvalCtx& context) {
    if (!arg_inited_) {
        lower_arg_.SetValue<ValueType>(expr_->lower_val_);
        upper_arg_.SetValue<ValueType>(expr_->upper_val_);
        arg_inited_ = true;
    }
    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }
    return EvalKernel<milvus::ArrayView>(
        context,
        BinaryRangeArrayKernel<ValueType>{
            .lower = lower_arg_.GetValue<ValueType>(),
            .upper = upper_arg_.GetValue<ValueType>(),
            .lower_inclusive = expr_->lower_inclusive_,
            .upper_inclusive = expr_->upper_inclusive_,
            .index = index,
        },
        false);
}

template <typename T>
VectorPtr
PhyBinaryRangeFilterExpr::ExecRangeVisitorImplForPk(EvalCtx& context) {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            PkInnerType;

    if (!arg_inited_) {
        lower_arg_.SetValue<PkInnerType>(expr_->lower_val_);
        upper_arg_.SetValue<PkInnerType>(expr_->upper_val_);
        arg_inited_ = true;
    }

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (cached_index_chunk_id_ != 0) {
        cached_index_chunk_id_ = 0;
        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        auto cache_view = cached_index_chunk_res_->view();

        PkType lower_pk = lower_arg_.GetValue<PkInnerType>();
        PkType upper_pk = upper_arg_.GetValue<PkInnerType>();
        segment_->pk_binary_range(op_ctx_,
                                  lower_pk,
                                  expr_->lower_inclusive_,
                                  upper_pk,
                                  expr_->upper_inclusive_,
                                  cache_view);
    }

    auto res = MoveOrSliceBitmap(
        *cached_index_chunk_res_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return res;
}

void
PhyBinaryRangeFilterExpr::DetermineExecPath() {
    // PkIndex (binary range only supports PK on sealed segments)
    if (is_pk_field_ && segment_->type() == SegmentType::Sealed) {
        exec_path_ = ExprExecPath::PkIndex;
        return;
    }

    // JsonStats
    if (CanUseJsonStatsAtInit()) {
        exec_path_ = ExprExecPath::JsonStats;
        return;
    }

    auto data_type = expr_->column_.data_type_;
    if (expr_->column_.element_level_) {
        data_type = expr_->column_.element_type_;
    }

    if (data_type == DataType::JSON) {
        const auto lower_type = expr_->lower_val_.val_case();
        const auto upper_type = expr_->upper_val_.val_case();
        const auto is_numeric =
            (lower_type == proto::plan::GenericValue::ValCase::kInt64Val ||
             lower_type == proto::plan::GenericValue::ValCase::kFloatVal) &&
            (upper_type == proto::plan::GenericValue::ValCase::kInt64Val ||
             upper_type == proto::plan::GenericValue::ValCase::kFloatVal);
        const auto requires_precise_int64_comparison =
            is_numeric &&
            (JsonNumericBoundRequiresPreciseInt64Comparison(
                 expr_->lower_val_) ||
             JsonNumericBoundRequiresPreciseInt64Comparison(expr_->upper_val_));
        if (requires_precise_int64_comparison) {
            exec_path_ = ExprExecPath::RawData;
            return;
        }
    }

    // ARRAY type cannot use scalar index.
    if (data_type == DataType::ARRAY) {
        exec_path_ = ExprExecPath::RawData;
        return;
    }

    SegmentExpr::DetermineExecPath();
    if (exec_path_ != ExprExecPath::ScalarIndex) {
        return;
    }

    if (data_type == DataType::JSON &&
        expr_->lower_val_.val_case() ==
            proto::plan::GenericValue::ValCase::kStringVal) {
        auto* index_ptr = dynamic_cast<const index::ScalarIndex<std::string>*>(
            pinned_index_[0].get());
        const auto supports_range =
            index_ptr != nullptr &&
            index_ptr->GetIndexType() != index::ScalarIndexType::NGRAM &&
            SegmentExpr::CanUseIndexForOp<std::string>(
                proto::plan::OpType::GreaterEqual) &&
            SegmentExpr::CanUseIndexForOp<std::string>(
                proto::plan::OpType::LessEqual);
        if (!supports_range) {
            exec_path_ = ExprExecPath::RawData;
        }
    }

    // A binary range needs both one-sided range operations. String indexes can
    // decline individual operations through ShouldUseOp; FMINDEX declines all
    // lexicographic ranges and must fall back before its Range() overload is
    // reached.
    if (data_type == DataType::VARCHAR) {
        const auto lower_op = expr_->lower_inclusive_
                                  ? proto::plan::OpType::GreaterEqual
                                  : proto::plan::OpType::GreaterThan;
        const auto upper_op = expr_->upper_inclusive_
                                  ? proto::plan::OpType::LessEqual
                                  : proto::plan::OpType::LessThan;
        if (!SegmentExpr::CanUseIndexForOp<std::string>(lower_op) ||
            !SegmentExpr::CanUseIndexForOp<std::string>(upper_op)) {
            exec_path_ = ExprExecPath::RawData;
        }
    }
}

void
PhyBinaryRangeFilterExpr::PrefetchRawData() {
    auto datatype = expr_->column_.data_type_;
    if (expr_->column_.element_level_) {
        datatype = expr_->column_.element_type_;
    }

    switch (datatype) {
        case DataType::BOOL:
            PrefetchRawData<bool>();
            break;
        case DataType::INT8:
            PrefetchRawData<int8_t>();
            break;
        case DataType::INT16:
            PrefetchRawData<int16_t>();
            break;
        case DataType::INT32:
            PrefetchRawData<int32_t>();
            break;
        case DataType::INT64:
            PrefetchRawData<int64_t>();
            break;
        case DataType::TIMESTAMPTZ:
            PrefetchRawData<int64_t>();
            break;
        case DataType::FLOAT:
            PrefetchRawData<float>();
            break;
        case DataType::DOUBLE:
            PrefetchRawData<double>();
            break;
        case DataType::VARCHAR:
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                PrefetchRawData<std::string>();
            } else {
                PrefetchRawData<std::string_view>();
            }
            break;
        default:
            SegmentExpr::PrefetchRawData(expr_->column_.field_id_);
            break;
    }
}

template <typename T>
void
PhyBinaryRangeFilterExpr::PrefetchRawData() {
    using U =
        std::conditional_t<std::is_same_v<T, std::string_view>, std::string, T>;
    using H =
        std::conditional_t<std::is_integral_v<U> && !std::is_same_v<bool, T>,
                           int64_t,
                           U>;
    H lower_val = GetValueWithCastNumber<H>(expr_->lower_val_);
    H upper_val = GetValueWithCastNumber<H>(expr_->upper_val_);
    auto skip_index = segment_->GetSkipIndex();

    std::vector<int64_t> chunks_may_hit;
    for (size_t i = RawDataPrefetchStartChunk(); i < num_data_chunk_; ++i) {
        auto skip = skip_index->CanSkipBinaryRange(field_id_,
                                                   i,
                                                   lower_val,
                                                   upper_val,
                                                   expr_->lower_inclusive_,
                                                   expr_->upper_inclusive_);
        if (!skip) {
            chunks_may_hit.push_back(i);
        }
    }

    segment_->prefetch_chunks(op_ctx_, field_id_, chunks_may_hit);
}
}  // namespace exec
}  // namespace milvus
