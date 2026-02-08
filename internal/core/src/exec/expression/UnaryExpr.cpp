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

#include "UnaryExpr.h"

#include <simdjson.h>
#include <algorithm>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <optional>
#include <cctype>
#include <set>
#include <unordered_set>
#include <variant>

#include "boost/container/vector.hpp"
#include "boost/cstdint.hpp"
#include "boost/regex/v5/basic_regex.hpp"
#include "boost/regex/v5/perl_matcher_common.hpp"
#include "boost/regex/v5/perl_matcher_non_recursive.hpp"
#include "boost/regex/v5/regex.hpp"
#include "boost/regex/v5/regex_fwd.hpp"
#include "boost/regex/v5/regex_search.hpp"
#include "bsoncxx/array/view.hpp"
#include "common/Consts.h"
#include "common/EasyAssert.h"
#include "common/Json.h"
#include "common/ScopedTimer.h"
#include "common/Span.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "exec/expression/ExprCache.h"
#include "fmt/core.h"
#include "folly/FBVector.h"
#include "glog/logging.h"
#include "index/NgramInvertedIndex.h"
#include "index/TextMatchIndex.h"
#include "index/json_stats/JsonKeyStats.h"
#include "index/json_stats/utils.h"
#include "log/Log.h"
#include "monitor/Monitor.h"
#include "opentelemetry/trace/span.h"
#include "prometheus/histogram.h"
#include "segcore/SegmentSealed.h"
#include "storage/MmapManager.h"
#include "storage/Types.h"

namespace milvus {
class SkipIndex;

namespace exec {
template <typename T>
bool
PhyUnaryRangeFilterExpr::CanUseIndexForArray() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;

    for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
        auto index_ptr = dynamic_cast<const Index*>(pinned_index_[i].get());

        if (index_ptr->GetIndexType() ==
                milvus::index::ScalarIndexType::HYBRID ||
            index_ptr->GetIndexType() ==
                milvus::index::ScalarIndexType::BITMAP) {
            return false;
        }
    }
    return true;
}

template <>
bool
PhyUnaryRangeFilterExpr::CanUseIndexForArray<milvus::Array>() {
    bool res;
    if (!SegmentExpr::CanUseIndex()) {
        use_index_ = res = false;
        return res;
    }
    switch (expr_->column_.element_type_) {
        case DataType::BOOL:
            res = CanUseIndexForArray<bool>();
            break;
        case DataType::INT8:
            res = CanUseIndexForArray<int8_t>();
            break;
        case DataType::INT16:
            res = CanUseIndexForArray<int16_t>();
            break;
        case DataType::INT32:
            res = CanUseIndexForArray<int32_t>();
            break;
        case DataType::INT64:
            res = CanUseIndexForArray<int64_t>();
            break;
        case DataType::FLOAT:
        case DataType::DOUBLE:
            // not accurate on floating point number, rollback to bruteforce.
            res = false;
            break;
        case DataType::VARCHAR:
        case DataType::STRING:
            res = CanUseIndexForArray<std::string_view>();
            break;
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported element type when execute array "
                      "equal for index: {}",
                      expr_->column_.element_type_);
    }
    use_index_ = res;
    return res;
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplArrayForIndex(EvalCtx& context) {
    return ExecRangeVisitorImplArray<T>(context);
}

template <>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplArrayForIndex<proto::plan::Array>(
    EvalCtx& context) {
    switch (expr_->op_type_) {
        case proto::plan::Equal:
        case proto::plan::NotEqual: {
            switch (expr_->column_.element_type_) {
                case DataType::BOOL: {
                    return ExecArrayEqualForIndex<bool>(
                        context, expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::INT8: {
                    return ExecArrayEqualForIndex<int8_t>(
                        context, expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::INT16: {
                    return ExecArrayEqualForIndex<int16_t>(
                        context, expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::INT32: {
                    return ExecArrayEqualForIndex<int32_t>(
                        context, expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::INT64: {
                    return ExecArrayEqualForIndex<int64_t>(
                        context, expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::FLOAT:
                case DataType::DOUBLE: {
                    // not accurate on floating point number, rollback to bruteforce.
                    return ExecRangeVisitorImplArray<proto::plan::Array>(
                        context);
                }
                case DataType::VARCHAR: {
                    if (segment_->type() == SegmentType::Growing) {
                        return ExecArrayEqualForIndex<std::string>(
                            context, expr_->op_type_ == proto::plan::NotEqual);
                    } else {
                        return ExecArrayEqualForIndex<std::string_view>(
                            context, expr_->op_type_ == proto::plan::NotEqual);
                    }
                }
                default:
                    ThrowInfo(DataTypeInvalid,
                              "unsupported element type when execute array "
                              "equal for index: {}",
                              expr_->column_.element_type_);
            }
        }
        default:
            return ExecRangeVisitorImplArray<proto::plan::Array>(context);
    }
}

void
PhyUnaryRangeFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    tracer::AutoSpan span(
        "PhyUnaryRangeFilterExpr::Eval", tracer::GetRootSpan(), true);
    span.GetSpan()->SetAttribute("data_type",
                                 static_cast<int>(expr_->column_.data_type_));
    span.GetSpan()->SetAttribute("op_type", static_cast<int>(expr_->op_type_));

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
        case DataType::TIMESTAMPTZ: {
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
            auto val_type = expr_->val_.val_case();
            auto val_type_inner = FromValCase(val_type);
            if (CanUseNgramIndex() && !has_offset_input_) {
                auto res = ExecNgramMatch(context);
                // If nullopt is returned, it means the query cannot be
                // optimized by ngram index. Forward it to the normal path.
                if (res.has_value()) {
                    result = res.value();
                    break;
                }
            }

            if (CanUseIndexForJson(val_type_inner) && !has_offset_input_) {
                switch (val_type) {
                    case proto::plan::GenericValue::ValCase::kBoolVal:
                        result = ExecRangeVisitorImplForIndex<bool>();
                        break;
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        if (expr_->val_.has_int64_val()) {
                            proto::plan::GenericValue double_val;
                            double_val.set_float_val(
                                static_cast<double>(expr_->val_.int64_val()));
                            value_arg_.SetValue<double>(double_val);
                            arg_inited_ = true;
                        }
                        result = ExecRangeVisitorImplForIndex<double>();
                        break;
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        result = ExecRangeVisitorImplForIndex<double>();
                        break;
                    case proto::plan::GenericValue::ValCase::kStringVal:
                        result = ExecRangeVisitorImplForIndex<std::string>();
                        break;
                    default:
                        ThrowInfo(
                            DataTypeInvalid, "unknown data type: {}", val_type);
                }
            } else {
                switch (val_type) {
                    case proto::plan::GenericValue::ValCase::kBoolVal:
                        result = ExecRangeVisitorImplJson<bool>(context);
                        break;
                    case proto::plan::GenericValue::ValCase::kInt64Val:
                        result = ExecRangeVisitorImplJson<int64_t>(context);
                        break;
                    case proto::plan::GenericValue::ValCase::kFloatVal:
                        result = ExecRangeVisitorImplJson<double>(context);
                        break;
                    case proto::plan::GenericValue::ValCase::kStringVal:
                        result = ExecRangeVisitorImplJson<std::string>(context);
                        break;
                    case proto::plan::GenericValue::ValCase::kArrayVal:
                        result = ExecRangeVisitorImplJson<proto::plan::Array>(
                            context);
                        break;
                    default:
                        ThrowInfo(
                            DataTypeInvalid, "unknown data type: {}", val_type);
                }
            }
            break;
        }
        case DataType::ARRAY: {
            auto val_type = expr_->val_.val_case();
            switch (val_type) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplArray<bool>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplArray<int64_t>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplArray<double>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplArray<std::string>(context);
                    break;
                case proto::plan::GenericValue::ValCase::kArrayVal:
                    if (!has_offset_input_ &&
                        CanUseIndexForArray<milvus::Array>()) {
                        result = ExecRangeVisitorImplArrayForIndex<
                            proto::plan::Array>(context);
                    } else {
                        result = ExecRangeVisitorImplArray<proto::plan::Array>(
                            context);
                    }
                    break;
                default:
                    ThrowInfo(
                        DataTypeInvalid, "unknown data type: {}", val_type);
            }
            break;
        }
        default:
            ThrowInfo(DataTypeInvalid,
                      "unsupported data type: {}",
                      expr_->column_.data_type_);
    }
}

template <typename ValueType>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplArray(EvalCtx& context) {
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
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

    if (!arg_inited_) {
        value_arg_.SetValue<ValueType>(expr_->val_);
        arg_inited_ = true;
    }
    ValueType val = value_arg_.GetValue<ValueType>();
    auto op_type = expr_->op_type_;
    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }
    int processed_cursor = 0;
    auto execute_sub_batch =
        [ op_type, &processed_cursor, &
          bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::ArrayView* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            ValueType val,
            int index) {
        switch (op_type) {
            case proto::plan::GreaterThan: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::GreaterThan,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::GreaterEqual: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::GreaterEqual,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::LessThan: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::LessThan,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::LessEqual: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::LessEqual,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::Equal: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::Equal,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::NotEqual: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::NotEqual,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::PrefixMatch: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::PrefixMatch,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::Match: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::Match,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::PostfixMatch: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::PostfixMatch,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::InnerMatch: {
                UnaryElementFuncForArray<ValueType,
                                         proto::plan::InnerMatch,
                                         filter_type>
                    func;
                func(data,
                     valid_data,
                     size,
                     val,
                     index,
                     res,
                     valid_res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            default:
                ThrowInfo(
                    OpTypeInvalid,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op_type));
        }
        processed_cursor += size;
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size =
            ProcessDataByOffsets<milvus::ArrayView>(execute_sub_batch,
                                                    std::nullptr_t{},
                                                    input,
                                                    res,
                                                    valid_res,
                                                    val,
                                                    index);
    } else {
        processed_size = ProcessDataChunks<milvus::ArrayView>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, val, index);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecArrayEqualForIndex(EvalCtx& context,
                                                bool reverse) {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    // get all elements.
    auto val = GetValueFromProto<proto::plan::Array>(expr_->val_);
    if (val.array_size() == 0) {
        // rollback to bruteforce. no candidates will be filtered out via index.
        return ExecRangeVisitorImplArray<proto::plan::Array>(context);
    }

    // cache the result to suit the framework.
    auto batch_res = ProcessIndexChunks<IndexInnerType>([this, &val, reverse](
                                                            Index* _) {
        boost::container::vector<IndexInnerType> elems;
        for (auto const& element : val.array()) {
            auto e = GetValueFromProto<IndexInnerType>(element);
            if (std::find(elems.begin(), elems.end(), e) == elems.end()) {
                elems.push_back(e);
            }
        }

        // filtering by index, get candidates.
        std::function<bool(milvus::proto::plan::Array& /*val*/,
                           int64_t /*offset*/)>
            is_same;

        if (segment_->is_chunked()) {
            is_same = [this, reverse](milvus::proto::plan::Array& val,
                                      int64_t offset) -> bool {
                auto [chunk_idx, chunk_offset] =
                    segment_->get_chunk_by_offset(field_id_, offset);
                auto pw = segment_->template chunk_view<milvus::ArrayView>(
                    op_ctx_, field_id_, chunk_idx);
                auto chunk = pw.get();
                return chunk.first[chunk_offset].is_same_array(val) ^ reverse;
            };
        } else {
            auto size_per_chunk = segment_->size_per_chunk();
            is_same = [this, size_per_chunk, reverse](
                          milvus::proto::plan::Array& val,
                          int64_t offset) -> bool {
                auto chunk_idx = offset / size_per_chunk;
                auto chunk_offset = offset % size_per_chunk;
                auto pw = segment_->template chunk_data<milvus::ArrayView>(
                    op_ctx_, field_id_, chunk_idx);
                auto chunk = pw.get();
                auto array_view = chunk.data() + chunk_offset;
                return array_view->is_same_array(val) ^ reverse;
            };
        }

        // collect all candidates.
        std::unordered_set<size_t> candidates;
        std::unordered_set<size_t> tmp_candidates;
        auto first_callback = [&candidates](size_t offset) -> void {
            candidates.insert(offset);
        };
        auto callback = [&candidates, &tmp_candidates](size_t offset) -> void {
            if (candidates.find(offset) != candidates.end()) {
                tmp_candidates.insert(offset);
            }
        };
        auto execute_sub_batch =
            [](Index* index_ptr,
               const IndexInnerType& val,
               const std::function<void(size_t /* offset */)>& callback) {
                index_ptr->InApplyCallback(1, &val, callback);
            };

        // run in-filter.
        for (size_t idx = 0; idx < elems.size(); idx++) {
            if (idx == 0) {
                ProcessIndexChunksV2<IndexInnerType>(
                    execute_sub_batch, elems[idx], first_callback);
            } else {
                ProcessIndexChunksV2<IndexInnerType>(
                    execute_sub_batch, elems[idx], callback);
                candidates = std::move(tmp_candidates);
            }
            // the size of candidates is small enough.
            if (candidates.size() * 100 < active_count_) {
                break;
            }
        }
        TargetBitmap res(active_count_);
        // run post-filter. The filter will only be executed once in the framework.
        for (const auto& candidate : candidates) {
            res[candidate] = is_same(val, candidate);
        }
        return res;
    });
    AssertInfo(batch_res->size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               batch_res->size(),
               real_batch_size);

    // return the result.
    return batch_res;
}

template <typename ExprValueType>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplJson(EvalCtx& context) {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();
    FieldId field_id = expr_->column_.field_id_;

    if (!has_offset_input_ &&
        CanUseJsonStats(context, field_id, expr_->column_.nested_path_)) {
        return ExecRangeVisitorImplJsonByStats<ExprValueType>();
    }

    auto real_batch_size =
        has_offset_input_ ? input->size() : GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (!arg_inited_) {
        value_arg_.SetValue<ExprValueType>(expr_->val_);
        arg_inited_ = true;
    }
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);

    ExprValueType val = value_arg_.GetValue<ExprValueType>();
    auto op_type = expr_->op_type_;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

#define UnaryRangeJSONCompare(cmp)                                  \
    do {                                                            \
        auto x = data[offset].template at<GetType>(pointer);        \
        if (x.error()) {                                            \
            if constexpr (std::is_same_v<GetType, int64_t>) {       \
                auto x = data[offset].template at<double>(pointer); \
                res[i] = !x.error() && (cmp);                       \
                break;                                              \
            }                                                       \
            res[i] = false;                                         \
            break;                                                  \
        }                                                           \
        res[i] = (cmp);                                             \
    } while (false)

#define UnaryRangeJSONCompareNotEqual(cmp)                          \
    do {                                                            \
        auto x = data[offset].template at<GetType>(pointer);        \
        if (x.error()) {                                            \
            if constexpr (std::is_same_v<GetType, int64_t>) {       \
                auto x = data[offset].template at<double>(pointer); \
                res[i] = x.error() || (cmp);                        \
                break;                                              \
            }                                                       \
            res[i] = true;                                          \
            break;                                                  \
        }                                                           \
        res[i] = (cmp);                                             \
    } while (false)

    int processed_cursor = 0;
    auto execute_sub_batch =
        [ op_type, pointer, &processed_cursor, &
          bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const milvus::Json* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            ExprValueType val) {
        bool has_bitmap_input = !bitmap_input.empty();
        switch (op_type) {
            case proto::plan::GreaterThan: {
                for (size_t i = 0; i < size; ++i) {
                    auto offset = i;
                    if constexpr (filter_type == FilterType::random) {
                        offset = (offsets) ? offsets[i] : i;
                    }
                    if (valid_data != nullptr && !valid_data[offset]) {
                        res[i] = valid_res[i] = false;
                        continue;
                    }
                    if (has_bitmap_input &&
                        !bitmap_input[i + processed_cursor]) {
                        continue;
                    }
                    if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                        res[i] = false;
                    } else {
                        UnaryRangeJSONCompare(x.value() > val);
                    }
                }
                break;
            }
            case proto::plan::GreaterEqual: {
                for (size_t i = 0; i < size; ++i) {
                    auto offset = i;
                    if constexpr (filter_type == FilterType::random) {
                        offset = (offsets) ? offsets[i] : i;
                    }
                    if (valid_data != nullptr && !valid_data[offset]) {
                        res[i] = valid_res[i] = false;
                        continue;
                    }
                    if (has_bitmap_input &&
                        !bitmap_input[i + processed_cursor]) {
                        continue;
                    }
                    if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                        res[i] = false;
                    } else {
                        UnaryRangeJSONCompare(x.value() >= val);
                    }
                }
                break;
            }
            case proto::plan::LessThan: {
                for (size_t i = 0; i < size; ++i) {
                    auto offset = i;
                    if constexpr (filter_type == FilterType::random) {
                        offset = (offsets) ? offsets[i] : i;
                    }
                    if (valid_data != nullptr && !valid_data[offset]) {
                        res[i] = valid_res[i] = false;
                        continue;
                    }
                    if (has_bitmap_input &&
                        !bitmap_input[i + processed_cursor]) {
                        continue;
                    }
                    if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                        res[i] = false;
                    } else {
                        UnaryRangeJSONCompare(x.value() < val);
                    }
                }
                break;
            }
            case proto::plan::LessEqual: {
                for (size_t i = 0; i < size; ++i) {
                    auto offset = i;
                    if constexpr (filter_type == FilterType::random) {
                        offset = (offsets) ? offsets[i] : i;
                    }
                    if (valid_data != nullptr && !valid_data[offset]) {
                        res[i] = valid_res[i] = false;
                        continue;
                    }
                    if (has_bitmap_input &&
                        !bitmap_input[i + processed_cursor]) {
                        continue;
                    }
                    if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                        res[i] = false;
                    } else {
                        UnaryRangeJSONCompare(x.value() <= val);
                    }
                }
                break;
            }
            case proto::plan::Equal: {
                for (size_t i = 0; i < size; ++i) {
                    auto offset = i;
                    if constexpr (filter_type == FilterType::random) {
                        offset = (offsets) ? offsets[i] : i;
                    }
                    if (valid_data != nullptr && !valid_data[offset]) {
                        res[i] = valid_res[i] = false;
                        continue;
                    }
                    if (has_bitmap_input &&
                        !bitmap_input[i + processed_cursor]) {
                        continue;
                    }
                    if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                        auto doc = data[i].doc();
                        auto array = doc.at_pointer(pointer).get_array();
                        if (array.error()) {
                            res[i] = false;
                            continue;
                        }
                        res[i] = CompareTwoJsonArray(array, val);
                    } else {
                        UnaryRangeJSONCompare(x.value() == val);
                    }
                }
                break;
            }
            case proto::plan::NotEqual: {
                for (size_t i = 0; i < size; ++i) {
                    auto offset = i;
                    if constexpr (filter_type == FilterType::random) {
                        offset = (offsets) ? offsets[i] : i;
                    }
                    if (valid_data != nullptr && !valid_data[offset]) {
                        valid_res[i] = false;
                        res[i] = true;
                        continue;
                    }
                    if (has_bitmap_input &&
                        !bitmap_input[i + processed_cursor]) {
                        continue;
                    }
                    if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                        auto doc = data[i].doc();
                        auto array = doc.at_pointer(pointer).get_array();
                        if (array.error()) {
                            res[i] = false;
                            continue;
                        }
                        res[i] = !CompareTwoJsonArray(array, val);
                    } else {
                        UnaryRangeJSONCompareNotEqual(x.value() != val);
                    }
                }
                break;
            }
            case proto::plan::InnerMatch:
            case proto::plan::PostfixMatch:
            case proto::plan::PrefixMatch: {
                for (size_t i = 0; i < size; ++i) {
                    auto offset = i;
                    if constexpr (filter_type == FilterType::random) {
                        offset = (offsets) ? offsets[i] : i;
                    }
                    if (valid_data != nullptr && !valid_data[offset]) {
                        res[i] = valid_res[i] = false;
                        continue;
                    }
                    if (has_bitmap_input &&
                        !bitmap_input[i + processed_cursor]) {
                        continue;
                    }
                    if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                        res[i] = false;
                    } else {
                        UnaryRangeJSONCompare(milvus::query::Match(
                            ExprValueType(x.value()), val, op_type));
                    }
                }
                break;
            }
            case proto::plan::Match: {
                if constexpr (std::is_same_v<ExprValueType, std::string>) {
                    LikePatternMatcher matcher(val);
                    for (size_t i = 0; i < size; ++i) {
                        auto offset = i;
                        if constexpr (filter_type == FilterType::random) {
                            offset = (offsets) ? offsets[i] : i;
                        }
                        if (valid_data != nullptr && !valid_data[offset]) {
                            res[i] = valid_res[i] = false;
                            continue;
                        }
                        if (has_bitmap_input &&
                            !bitmap_input[i + processed_cursor]) {
                            continue;
                        }
                        UnaryRangeJSONCompare(
                            matcher(ExprValueType(x.value())));
                    }
                } else {
                    ThrowInfo(OpTypeInvalid,
                              "Match operation only supports string type");
                }
                break;
            }
            default:
                ThrowInfo(
                    OpTypeInvalid,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op_type));
        }
        processed_cursor += size;
    };
    int64_t processed_size;
    if (has_offset_input_) {
        processed_size = ProcessDataByOffsets<milvus::Json>(
            execute_sub_batch, std::nullptr_t{}, input, res, valid_res, val);

    } else {
        processed_size = ProcessDataChunks<milvus::Json>(
            execute_sub_batch, std::nullptr_t{}, res, valid_res, val);
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

std::pair<std::string, std::string>
PhyUnaryRangeFilterExpr::SplitAtFirstSlashDigit(std::string input) {
    // Find pattern /\d+ (slash followed by ASCII digits) without regex
    // Use explicit ASCII range check to avoid locale-dependent std::isdigit behavior
    auto is_ascii_digit = [](char c) { return c >= '0' && c <= '9'; };
    for (size_t i = 0; i < input.size(); ++i) {
        if (input[i] == '/' && i + 1 < input.size() &&
            is_ascii_digit(input[i + 1])) {
            return {input.substr(0, i), input.substr(i)};
        }
    }
    return {input, ""};
}

template <typename ExprValueType>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplJsonByStats() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (cached_index_chunk_id_ != 0 &&
        segment_->type() == SegmentType::Sealed) {
        auto pointerpath = milvus::Json::pointer(expr_->column_.nested_path_);
        auto pointerpair = SplitAtFirstSlashDigit(pointerpath);
        std::string pointer = pointerpair.first;
        size_t array_index = pointerpair.second.empty()
                                 ? INVALID_ARRAY_INDEX
                                 : std::stoi(pointerpair.second);

        ExprValueType val = GetValueFromProto<ExprValueType>(expr_->val_);
        // for NotEqual: compute Equal and flip the result
        // this avoids handling NULL values differently in multiple places
        auto op_type = (expr_->op_type_ == proto::plan::OpType::NotEqual)
                           ? proto::plan::OpType::Equal
                           : expr_->op_type_;

        auto segment = static_cast<const segcore::SegmentSealed*>(segment_);
        auto field_id = expr_->column_.field_id_;
        auto index = segment->GetJsonStats(op_ctx_, field_id);
        Assert(index.get() != nullptr);
        cached_index_chunk_res_ =
            (op_type == proto::plan::OpType::NotEqual)
                ? std::make_shared<TargetBitmap>(active_count_, true)
                : std::make_shared<TargetBitmap>(active_count_);
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(active_count_, true);
        TargetBitmapView res_view(*cached_index_chunk_res_);
        TargetBitmapView valid_res_view(*cached_index_chunk_valid_res_);

        // process shredding data
        auto try_execute = [&](milvus::index::JSONType json_type,
                               TargetBitmapView& res_view,
                               TargetBitmapView& valid_res_view,
                               auto GetType,
                               auto ValType) {
            auto target_field = index->GetShreddingField(pointer, json_type);
            if (!target_field.empty()) {
                using ColType = decltype(GetType);
                using ValType = decltype(ValType);
                ShreddingExecutor<ColType, ValType> executor(
                    op_type, pointer, val);
                index->ExecutorForShreddingData<ColType>(op_ctx_,
                                                         target_field,
                                                         executor,
                                                         nullptr,
                                                         res_view,
                                                         valid_res_view);
                LOG_DEBUG(
                    "using shredding data's field: {} with value {}, count {} "
                    "for segment {}",
                    target_field,
                    val,
                    res_view.count(),
                    segment_->get_segment_id());
            }
        };

        {
            milvus::ScopedTimer timer(
                "unary_json_stats_shredding_data", [](double ms) {
                    milvus::monitor::internal_json_stats_latency_shredding
                        .Observe(ms);
                });

            if constexpr (std::is_same_v<GetType, bool>) {
                try_execute(milvus::index::JSONType::BOOL,
                            res_view,
                            valid_res_view,
                            bool{},
                            bool{});
            } else if constexpr (std::is_same_v<GetType, int64_t>) {
                try_execute(milvus::index::JSONType::INT64,
                            res_view,
                            valid_res_view,
                            int64_t{},
                            int64_t{});

                // and double compare
                TargetBitmap res_double(active_count_, false);
                TargetBitmapView res_double_view(res_double);
                TargetBitmap res_double_valid(active_count_, true);
                TargetBitmapView valid_res_double_view(res_double_valid);
                try_execute(milvus::index::JSONType::DOUBLE,
                            res_double_view,
                            valid_res_double_view,
                            double{},
                            int64_t{});
                res_view.inplace_or_with_count(res_double_view, active_count_);
                valid_res_view.inplace_or_with_count(valid_res_double_view,
                                                     active_count_);
            } else if constexpr (std::is_same_v<GetType, double>) {
                try_execute(milvus::index::JSONType::DOUBLE,
                            res_view,
                            valid_res_view,
                            double{},
                            double{});

                // add int64 compare
                TargetBitmap res_int64(active_count_, false);
                TargetBitmapView res_int64_view(res_int64);
                TargetBitmap res_int64_valid(active_count_, true);
                TargetBitmapView valid_res_int64_view(res_int64_valid);
                try_execute(milvus::index::JSONType::INT64,
                            res_int64_view,
                            valid_res_int64_view,
                            int64_t{},
                            double{});
                res_view.inplace_or_with_count(res_int64_view, active_count_);
                valid_res_view.inplace_or_with_count(valid_res_int64_view,
                                                     active_count_);
            } else if constexpr (std::is_same_v<GetType, std::string> ||
                                 std::is_same_v<GetType, std::string_view>) {
                try_execute(milvus::index::JSONType::STRING,
                            res_view,
                            valid_res_view,
                            GetType{},
                            GetType{});
            } else if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                // ARRAY shredding data: stored as BSON binary in binary column
                auto target_field = index->GetShreddingField(
                    pointer, milvus::index::JSONType::ARRAY);
                if (!target_field.empty()) {
                    ShreddingArrayBsonExecutor executor(op_type, pointer, val);
                    index->ExecutorForShreddingData<std::string_view>(
                        op_ctx_,
                        target_field,
                        executor,
                        nullptr,
                        res_view,
                        valid_res_view);
                    LOG_DEBUG("using shredding array field: {}, count {}",
                              target_field,
                              res_view.count());
                }
            }
        }

        // process shared data
        auto shared_executor = [op_type, val, array_index, &res_view](
                                   milvus::BsonView bson,
                                   uint32_t row_id,
                                   uint32_t value_offset) {
            if constexpr (std::is_same_v<GetType, proto::plan::Array>) {
                Assert(op_type == proto::plan::OpType::Equal ||
                       op_type == proto::plan::OpType::NotEqual);
                if (array_index != INVALID_ARRAY_INDEX) {
                    auto array_value = bson.ParseAsArrayAtOffset(value_offset);
                    if (!array_value.has_value()) {
                        // For NotEqual: path not exists means "not equal", keep true
                        // For Equal: path not exists means no match, set false
                        res_view[row_id] =
                            (op_type == proto::plan::OpType::NotEqual);
                        return;
                    }
                    auto sub_array = milvus::BsonView::GetNthElementInArray<
                        bsoncxx::array::view>(array_value.value().data(),
                                              array_index);
                    if (!sub_array.has_value()) {
                        res_view[row_id] =
                            (op_type == proto::plan::OpType::NotEqual);
                        return;
                    }
                    res_view[row_id] =
                        op_type == proto::plan::OpType::Equal
                            ? CompareTwoJsonArray(sub_array.value(), val)
                            : !CompareTwoJsonArray(sub_array.value(), val);
                } else {
                    auto array_value = bson.ParseAsArrayAtOffset(value_offset);
                    if (!array_value.has_value()) {
                        res_view[row_id] =
                            (op_type == proto::plan::OpType::NotEqual);
                        return;
                    }
                    res_view[row_id] =
                        op_type == proto::plan::OpType::Equal
                            ? CompareTwoJsonArray(array_value.value(), val)
                            : !CompareTwoJsonArray(array_value.value(), val);
                }
            } else {
                std::optional<GetType> get_value;
                if (array_index != INVALID_ARRAY_INDEX) {
                    auto array_value = bson.ParseAsArrayAtOffset(value_offset);
                    if (!array_value.has_value()) {
                        // Path not exists: NotEqual->true, others->false
                        res_view[row_id] =
                            (op_type == proto::plan::OpType::NotEqual);
                        return;
                    }
                    get_value = milvus::BsonView::GetNthElementInArray<GetType>(
                        array_value.value().data(), array_index);
                    // If GetType is int and value is not found, try double
                    if constexpr (std::is_same_v<GetType, int64_t>) {
                        if (!get_value.has_value()) {
                            auto get_value =
                                milvus::BsonView::GetNthElementInArray<double>(
                                    array_value.value().data(), array_index);
                            if (get_value.has_value()) {
                                res_view[row_id] = UnaryCompare(
                                    get_value.value(), val, op_type);
                            } else {
                                // Type mismatch: NotEqual->true, others->false
                                res_view[row_id] =
                                    (op_type == proto::plan::OpType::NotEqual);
                            }
                            return;
                        }
                    } else if constexpr (std::is_same_v<GetType, double>) {
                        if (!get_value.has_value()) {
                            auto get_value =
                                milvus::BsonView::GetNthElementInArray<int64_t>(
                                    array_value.value().data(), array_index);
                            if (get_value.has_value()) {
                                res_view[row_id] = UnaryCompare(
                                    get_value.value(), val, op_type);
                            } else {
                                res_view[row_id] =
                                    (op_type == proto::plan::OpType::NotEqual);
                            }
                            return;
                        }
                    }
                } else {
                    get_value =
                        bson.ParseAsValueAtOffset<GetType>(value_offset);
                    // If GetType is int and value is not found, try double
                    if constexpr (std::is_same_v<GetType, int64_t>) {
                        if (!get_value.has_value()) {
                            auto get_value =
                                bson.ParseAsValueAtOffset<double>(value_offset);
                            if (get_value.has_value()) {
                                res_view[row_id] = UnaryCompare(
                                    get_value.value(), val, op_type);
                            } else {
                                res_view[row_id] =
                                    (op_type == proto::plan::OpType::NotEqual);
                            }
                            return;
                        }
                    } else if constexpr (std::is_same_v<GetType, double>) {
                        if (!get_value.has_value()) {
                            auto get_value = bson.ParseAsValueAtOffset<int64_t>(
                                value_offset);
                            if (get_value.has_value()) {
                                res_view[row_id] = UnaryCompare(
                                    get_value.value(), val, op_type);
                            } else {
                                res_view[row_id] =
                                    (op_type == proto::plan::OpType::NotEqual);
                            }
                            return;
                        }
                    }
                }
                if (!get_value.has_value()) {
                    res_view[row_id] =
                        (op_type == proto::plan::OpType::NotEqual);
                    return;
                }
                res_view[row_id] =
                    UnaryCompare(get_value.value(), val, op_type);
            }
        };

        std::set<milvus::index::JSONType> target_types;
        if constexpr (std::is_same_v<GetType, std::string>) {
            target_types.insert(milvus::index::JSONType::STRING);
        } else if constexpr (std::is_same_v<GetType, int64_t> ||
                             std::is_same_v<GetType, double>) {
            target_types.insert(milvus::index::JSONType::INT64);
            target_types.insert(milvus::index::JSONType::DOUBLE);
        } else if constexpr (std::is_same_v<GetType, bool>) {
            target_types.insert(milvus::index::JSONType::BOOL);
        }

        {
            milvus::ScopedTimer timer(
                "unary_json_stats_shared_data", [](double ms) {
                    milvus::monitor::internal_json_stats_latency_shared.Observe(
                        ms);
                });

            index->ExecuteForSharedData(
                op_ctx_, bson_index_, pointer, shared_executor);
        }

        // for NotEqual: flip the result
        if (expr_->op_type_ == proto::plan::OpType::NotEqual) {
            cached_index_chunk_res_->flip();
        }
        cached_index_chunk_id_ = 0;
    }

    TargetBitmap result;
    result.append(
        *cached_index_chunk_res_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImpl(EvalCtx& context) {
    if (expr_->op_type_ == proto::plan::OpType::TextMatch ||
        expr_->op_type_ == proto::plan::OpType::PhraseMatch) {
        if (has_offset_input_) {
            ThrowInfo(
                OpTypeInvalid,
                fmt::format("match query does not support iterative filter"));
        }
        return ExecTextMatch();
    } else if (CanUseNgramIndex()) {
        auto res = ExecNgramMatch(context);
        // If nullopt is returned, it means the query cannot be
        // optimized by ngram index. Forward it to the normal path.
        if (res.has_value()) {
            return res.value();
        }
    }

    if (!has_offset_input_ && is_pk_field_ && IsCompareOp(expr_->op_type_)) {
        if (pk_type_ == DataType::VARCHAR) {
            return ExecRangeVisitorImplForPk<std::string_view>(context);
        } else {
            return ExecRangeVisitorImplForPk<int64_t>(context);
        }
    }

    if (CanUseIndex<T>() && !has_offset_input_) {
        return ExecRangeVisitorImplForIndex<T>();
    } else {
        return ExecRangeVisitorImplForData<T>(context);
    }
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForPk(EvalCtx& context) {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;

    if (!arg_inited_) {
        value_arg_.SetValue<IndexInnerType>(expr_->val_);
        arg_inited_ = true;
    }
    if (auto res = PreCheckOverflow<T>()) {
        return res;
    }

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (cached_index_chunk_id_ != 0) {
        cached_index_chunk_id_ = 0;
        cached_index_chunk_res_ = std::make_shared<TargetBitmap>(active_count_);
        auto cache_view = cached_index_chunk_res_->view();

        auto op_type = expr_->op_type_;
        PkType pk = value_arg_.GetValue<IndexInnerType>();
        if (op_type == proto::plan::NotEqual) {
            segment_->pk_range(op_ctx_, proto::plan::Equal, pk, cache_view);
            cache_view.flip();
        } else {
            segment_->pk_range(op_ctx_, op_type, pk, cache_view);
        }
    }

    TargetBitmap result;
    result.append(
        *cached_index_chunk_res_, current_data_global_pos_, real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(result),
                                          TargetBitmap(real_batch_size, true));
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForIndex() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    if (!arg_inited_) {
        value_arg_.SetValue<IndexInnerType>(expr_->val_);
        arg_inited_ = true;
    }
    if (auto res = PreCheckOverflow<T>()) {
        return res;
    }

    auto real_batch_size =
        GetNextRealBatchSize(nullptr, expr_->column_.element_level_);
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto op_type = expr_->op_type_;
    auto execute_sub_batch = [op_type](Index* index_ptr, IndexInnerType val) {
        TargetBitmap res;
        switch (op_type) {
            case proto::plan::GreaterThan: {
                UnaryIndexFunc<T, proto::plan::GreaterThan> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::GreaterEqual: {
                UnaryIndexFunc<T, proto::plan::GreaterEqual> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::LessThan: {
                UnaryIndexFunc<T, proto::plan::LessThan> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::LessEqual: {
                UnaryIndexFunc<T, proto::plan::LessEqual> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::Equal: {
                UnaryIndexFunc<T, proto::plan::Equal> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::NotEqual: {
                UnaryIndexFunc<T, proto::plan::NotEqual> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::PrefixMatch: {
                UnaryIndexFunc<T, proto::plan::PrefixMatch> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::PostfixMatch: {
                UnaryIndexFunc<T, proto::plan::PostfixMatch> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::InnerMatch: {
                UnaryIndexFunc<T, proto::plan::InnerMatch> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            case proto::plan::Match: {
                UnaryIndexFunc<T, proto::plan::Match> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            default:
                ThrowInfo(
                    OpTypeInvalid,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op_type));
        }
        return res;
    };
    IndexInnerType val = value_arg_.GetValue<IndexInnerType>();
    auto res = ProcessIndexChunks<T>(execute_sub_batch, val);
    AssertInfo(res->size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res->size(),
               real_batch_size);
    return res;
}

template <typename T>
ColumnVectorPtr
PhyUnaryRangeFilterExpr::PreCheckOverflow(OffsetVector* input) {
    if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
        auto val = GetValueFromProto<int64_t>(expr_->val_);

        if (milvus::query::out_of_range<T>(val)) {
            int64_t batch_size;
            if (input != nullptr) {
                batch_size = input->size();
            } else {
                batch_size = overflow_check_pos_ + batch_size_ >= active_count_
                                 ? active_count_ - overflow_check_pos_
                                 : batch_size_;
                overflow_check_pos_ += batch_size;
            }
            auto valid =
                (input != nullptr)
                    ? ProcessChunksForValidByOffsets<T>(
                          SegmentExpr::CanUseIndex(), *input)
                    : ProcessChunksForValid<T>(SegmentExpr::CanUseIndex());
            auto res_vec = std::make_shared<ColumnVector>(
                TargetBitmap(batch_size), std::move(valid));
            TargetBitmapView res(res_vec->GetRawData(), batch_size);
            TargetBitmapView valid_res(res_vec->GetValidRawData(), batch_size);
            switch (expr_->op_type_) {
                case proto::plan::GreaterThan:
                case proto::plan::GreaterEqual: {
                    if (milvus::query::lt_lb<T>(val)) {
                        res.set();
                        res &= valid_res;
                        return res_vec;
                    }
                    return res_vec;
                }
                case proto::plan::LessThan:
                case proto::plan::LessEqual: {
                    if (milvus::query::gt_ub<T>(val)) {
                        res.set();
                        res &= valid_res;
                        return res_vec;
                    }
                    return res_vec;
                }
                case proto::plan::Equal: {
                    res.reset();
                    return res_vec;
                }
                case proto::plan::NotEqual: {
                    res.set();
                    res &= valid_res;
                    return res_vec;
                }
                default: {
                    ThrowInfo(OpTypeInvalid,
                              "unsupported range node {}",
                              expr_->op_type_);
                }
            }
        }
    }
    return nullptr;
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForData(EvalCtx& context) {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    auto* input = context.get_offset_input();
    const auto& bitmap_input = context.get_bitmap_input();

    if (auto res = PreCheckOverflow<T>(input)) {
        return res;
    }

    auto real_batch_size =
        GetNextRealBatchSize(input, expr_->column_.element_level_);
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (!arg_inited_) {
        value_arg_.SetValue<IndexInnerType>(expr_->val_);
        arg_inited_ = true;
    }
    IndexInnerType val = GetValueFromProto<IndexInnerType>(expr_->val_);
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size, false),
                                       TargetBitmap(real_batch_size, true));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    TargetBitmapView valid_res(res_vec->GetValidRawData(), real_batch_size);
    auto expr_type = expr_->op_type_;

    size_t processed_cursor = 0;
    auto execute_sub_batch =
        [ expr_type, &processed_cursor, &
          bitmap_input ]<FilterType filter_type = FilterType::sequential>(
            const T* data,
            const bool* valid_data,
            const int32_t* offsets,
            const int size,
            TargetBitmapView res,
            TargetBitmapView valid_res,
            IndexInnerType val) {
        // If data is nullptr, this chunk was skipped by SkipIndex.
        // We only need to update processed_cursor for bitmap_input indexing.
        if (data == nullptr) {
            processed_cursor += size;
            return;
        }
        switch (expr_type) {
            case proto::plan::GreaterThan: {
                UnaryElementFunc<T, proto::plan::GreaterThan, filter_type> func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::GreaterEqual: {
                UnaryElementFunc<T, proto::plan::GreaterEqual, filter_type>
                    func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::LessThan: {
                UnaryElementFunc<T, proto::plan::LessThan, filter_type> func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::LessEqual: {
                UnaryElementFunc<T, proto::plan::LessEqual, filter_type> func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::Equal: {
                UnaryElementFunc<T, proto::plan::Equal, filter_type> func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::NotEqual: {
                UnaryElementFunc<T, proto::plan::NotEqual, filter_type> func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::PrefixMatch: {
                UnaryElementFunc<T, proto::plan::PrefixMatch, filter_type> func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::PostfixMatch: {
                UnaryElementFunc<T, proto::plan::PostfixMatch, filter_type>
                    func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::InnerMatch: {
                UnaryElementFunc<T, proto::plan::InnerMatch, filter_type> func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            case proto::plan::Match: {
                UnaryElementFunc<T, proto::plan::Match, filter_type> func;
                func(data,
                     size,
                     val,
                     res,
                     bitmap_input,
                     processed_cursor,
                     offsets);
                break;
            }
            default:
                ThrowInfo(
                    OpTypeInvalid,
                    fmt::format("unsupported operator type for unary expr: {}",
                                expr_type));
        }
        // there is a batch operation in BinaryRangeElementFunc,
        // so not divide data again for the reason that it may reduce performance if the null distribution is scattered
        // but to mask res with valid_data after the batch operation.
        if (valid_data != nullptr) {
            bool has_bitmap_input = !bitmap_input.empty();
            for (int i = 0; i < size; i++) {
                if (has_bitmap_input && !bitmap_input[i + processed_cursor]) {
                    continue;
                }
                auto offset = i;
                if constexpr (filter_type == FilterType::random) {
                    offset = (offsets) ? offsets[i] : i;
                }
                if (!valid_data[offset]) {
                    res[i] = valid_res[i] = false;
                }
            }
        }
        processed_cursor += size;
    };

    auto skip_index_func = [expr_type, val](const SkipIndex& skip_index,
                                            FieldId field_id,
                                            int64_t chunk_id) {
        return skip_index.CanSkipUnaryRange<T>(
            field_id, chunk_id, expr_type, val);
    };

    int64_t processed_size;
    if (has_offset_input_) {
        if (expr_->column_.element_level_) {
            // For element-level filtering with offset input
            processed_size = ProcessElementLevelByOffsets<T>(
                execute_sub_batch, skip_index_func, input, res, valid_res, val);
        } else {
            processed_size = ProcessDataByOffsets<T>(
                execute_sub_batch, skip_index_func, input, res, valid_res, val);
        }
    } else {
        if (expr_->column_.element_level_) {
            // For element-level filtering without offset input (brute force)
            processed_size = ProcessDataChunksForElementLevel<T>(
                execute_sub_batch, skip_index_func, res, valid_res, val);
        } else {
            processed_size = ProcessDataChunks<T>(
                execute_sub_batch, skip_index_func, res, valid_res, val);
        }
    }
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}, related params[active_count:{}, "
               "current_data_chunk:{}, num_data_chunk:{}, current_data_pos:{}]",
               processed_size,
               real_batch_size,
               active_count_,
               current_data_chunk_,
               num_data_chunk_,
               current_data_chunk_pos_);
    return res_vec;
}

template <typename T>
bool
PhyUnaryRangeFilterExpr::CanUseIndex() {
    use_index_ = SegmentExpr::CanUseIndex() &&
                 SegmentExpr::CanUseIndexForOp<T>(expr_->op_type_);
    return use_index_;
}

bool
PhyUnaryRangeFilterExpr::CanUseIndexForJson(DataType val_type) {
    if (!SegmentExpr::CanUseIndex()) {
        use_index_ = false;
        return false;
    }
    bool has_index = pinned_index_.size() > 0;
    switch (val_type) {
        case DataType::STRING:
        case DataType::VARCHAR:
            use_index_ = has_index &&
                         expr_->op_type_ != proto::plan::OpType::Match &&
                         expr_->op_type_ != proto::plan::OpType::PostfixMatch &&
                         expr_->op_type_ != proto::plan::OpType::InnerMatch;
            break;
        default:
            use_index_ = has_index;
    }
    return use_index_;
}

VectorPtr
PhyUnaryRangeFilterExpr::ExecTextMatch() {
    using Index = index::TextMatchIndex;
    if (!arg_inited_) {
        value_arg_.SetValue<std::string>(expr_->val_);
        arg_inited_ = true;
    }
    auto query = value_arg_.GetValue<std::string>();

    int64_t slop = 0;
    if (expr_->op_type_ == proto::plan::PhraseMatch) {
        // It should be larger than 0 in normal cases. Check it incase of receiving old version proto.
        if (expr_->extra_values_.size() > 0) {
            slop = GetValueFromProto<int64_t>(expr_->extra_values_[0]);
        }
        if (slop < 0 || slop > std::numeric_limits<uint32_t>::max()) {
            throw SegcoreError(
                ErrorCode::InvalidParameter,
                fmt::format(
                    "Slop {} is invalid in phrase match query. Should be "
                    "within [0, UINT32_MAX].",
                    slop));
        }
    }
    auto op_type = expr_->op_type_;

    // Process-level LRU cache lookup by (segment_id, expr signature)
    if (cached_match_res_ == nullptr &&
        exec::ExprResCacheManager::IsEnabled() &&
        segment_->type() == SegmentType::Sealed) {
        exec::ExprResCacheManager::Key key{segment_->get_segment_id(),
                                           this->ToString()};
        exec::ExprResCacheManager::Value v;
        if (exec::ExprResCacheManager::Instance().Get(key, v)) {
            cached_match_res_ = v.result;
            cached_index_chunk_valid_res_ = v.valid_result;
            AssertInfo(cached_match_res_->size() == active_count_,
                       "internal error: expr res cache size {} not equal "
                       "expect active count {}",
                       cached_match_res_->size(),
                       active_count_);
        }
    }

    uint32_t min_should_match = 1;  // default value
    if (op_type == proto::plan::OpType::TextMatch &&
        expr_->extra_values_.size() > 0) {
        // min_should_match is stored in the first extra value
        min_should_match = static_cast<uint32_t>(
            GetValueFromProto<int64_t>(expr_->extra_values_[0]));
    }

    auto func = [op_type, slop, min_should_match](
                    Index* index, const std::string& query) -> TargetBitmap {
        if (op_type == proto::plan::OpType::TextMatch) {
            return index->MatchQuery(query, min_should_match);
        } else if (op_type == proto::plan::OpType::PhraseMatch) {
            return index->PhraseMatchQuery(query, slop);
        } else {
            ThrowInfo(OpTypeInvalid,
                      "unsupported operator type for match query: {}",
                      op_type);
        }
    };

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    if (cached_match_res_ == nullptr) {
        auto pw = segment_->GetTextIndex(op_ctx_, field_id_);
        auto index = pw.get();
        auto res = std::move(func(index, query));
        auto valid_res = index->IsNotNull();
        cached_match_res_ = std::make_shared<TargetBitmap>(std::move(res));
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(std::move(valid_res));
        if (cached_match_res_->size() < active_count_) {
            // some entities are not visible in inverted index.
            // only happend on growing segment.
            TargetBitmap tail(active_count_ - cached_match_res_->size());
            cached_match_res_->append(tail);
            cached_index_chunk_valid_res_->append(tail);
        }

        // Insert into process-level cache
        if (exec::ExprResCacheManager::IsEnabled() &&
            segment_->type() == SegmentType::Sealed) {
            exec::ExprResCacheManager::Key key{segment_->get_segment_id(),
                                               this->ToString()};
            exec::ExprResCacheManager::Value v;
            v.result = cached_match_res_;
            v.valid_result = cached_index_chunk_valid_res_;
            v.active_count = active_count_;
            exec::ExprResCacheManager::Instance().Put(key, v);
        }
    }

    TargetBitmap result;
    TargetBitmap valid_result;
    result.append(
        *cached_match_res_, current_data_global_pos_, real_batch_size);
    valid_result.append(*cached_index_chunk_valid_res_,
                        current_data_global_pos_,
                        real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(result),
                                          std::move(valid_result));
};

bool
PhyUnaryRangeFilterExpr::CanUseNgramIndex() const {
    if (pinned_ngram_index_.get() == nullptr || has_offset_input_) {
        return false;
    }
    auto literal = GetValueFromProto<std::string>(expr_->val_);
    return pinned_ngram_index_.get()->CanHandleLiteral(literal,
                                                       expr_->op_type_);
}

void
PhyUnaryRangeFilterExpr::ExecuteNgramPhase1(TargetBitmap& candidates) {
    if (!arg_inited_) {
        value_arg_.SetValue<std::string>(expr_->val_);
        arg_inited_ = true;
    }

    auto literal = value_arg_.GetValue<std::string>();
    auto index = pinned_ngram_index_.get();
    AssertInfo(index != nullptr,
               "ngram index should not be null, field_id: {}",
               field_id_.get());

    index->ExecutePhase1(literal, expr_->op_type_, candidates);
}

void
PhyUnaryRangeFilterExpr::ExecuteNgramPhase2(TargetBitmap& candidates,
                                            int64_t segment_offset,
                                            int64_t batch_size) {
    if (!arg_inited_) {
        value_arg_.SetValue<std::string>(expr_->val_);
        arg_inited_ = true;
    }

    auto literal = value_arg_.GetValue<std::string>();
    auto index = pinned_ngram_index_.get();
    AssertInfo(index != nullptr,
               "ngram index should not be null, field_id: {}",
               field_id_.get());

    index->ExecutePhase2(
        literal, expr_->op_type_, this, candidates, segment_offset, batch_size);
}

std::optional<VectorPtr>
PhyUnaryRangeFilterExpr::ExecNgramMatch(EvalCtx& context) {
    if (!arg_inited_) {
        value_arg_.SetValue<std::string>(expr_->val_);
        arg_inited_ = true;
    }

    auto literal = value_arg_.GetValue<std::string>();
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return std::nullopt;
    }

    auto index = pinned_ngram_index_.get();
    AssertInfo(index != nullptr,
               "ngram index should not be null, field_id: {}",
               field_id_.get());

    // Phase 1: Execute once for entire segment (no bitmap_input needed)
    if (cached_phase1_res_ == nullptr) {
        if (!index->CanHandleLiteral(literal, expr_->op_type_)) {
            return std::nullopt;
        }

        auto total_count = static_cast<size_t>(index->Count());
        TargetBitmap candidates(total_count, true);
        index->ExecutePhase1(literal, expr_->op_type_, candidates);
        cached_phase1_res_ =
            std::make_shared<TargetBitmap>(std::move(candidates));
        cached_index_chunk_valid_res_ =
            std::make_shared<TargetBitmap>(std::move(index->IsNotNull()));
    }

    // Phase 2: Execute per batch with batch-level bitmap_input
    TargetBitmap batch_candidates;
    batch_candidates.append(
        *cached_phase1_res_, current_data_global_pos_, real_batch_size);

    // Apply batch-level pre_filter from previous expressions in conjunction
    const auto& bitmap_input = context.get_bitmap_input();
    if (!bitmap_input.empty()) {
        AssertInfo(static_cast<int64_t>(bitmap_input.size()) == real_batch_size,
                   "bitmap_input size {} != real_batch_size {}",
                   bitmap_input.size(),
                   real_batch_size);
        batch_candidates &= bitmap_input;
    }

    // Execute Phase2 (post-filter) on this batch
    if (!batch_candidates.none()) {
        index->ExecutePhase2(literal,
                             expr_->op_type_,
                             this,
                             batch_candidates,
                             current_data_global_pos_,
                             real_batch_size);
    }

    TargetBitmap valid_result;
    valid_result.append(*cached_index_chunk_valid_res_,
                        current_data_global_pos_,
                        real_batch_size);
    MoveCursor();
    return std::make_shared<ColumnVector>(std::move(batch_candidates),
                                          std::move(valid_result));
}

}  // namespace exec
}  // namespace milvus
