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
#include <optional>
#include "common/Json.h"
#include "common/Types.h"
#include "common/type_c.h"
#include "log/Log.h"

#include <boost/regex.hpp>
namespace milvus {
namespace exec {
template <typename T>
bool
PhyUnaryRangeFilterExpr::CanUseIndexForArray() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;

    for (size_t i = current_index_chunk_; i < num_index_chunk_; i++) {
        auto pw = segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);
        auto index_ptr = const_cast<Index*>(pw.get());

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
    if (!is_index_mode_) {
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
            PanicInfo(DataTypeInvalid,
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
                    PanicInfo(DataTypeInvalid,
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
    auto input = context.get_offset_input();
    SetHasOffsetInput((input != nullptr));
    switch (expr_->column_.data_type_) {
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
            auto val_type = expr_->val_.val_case();
            if (CanUseIndexForJson(FromValCase(val_type)) &&
                !has_offset_input_) {
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
                    case proto::plan::GenericValue::ValCase::kArrayVal:
                        result =
                            ExecRangeVisitorImplForIndex<proto::plan::Array>();
                        break;
                    default:
                        PanicInfo(
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
                        PanicInfo(
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
                    PanicInfo(
                        DataTypeInvalid, "unknown data type: {}", val_type);
            }
            break;
        }
        default:
            PanicInfo(DataTypeInvalid,
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
            default:
                PanicInfo(
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
                    field_id_, chunk_idx);
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
                    field_id_, chunk_idx);
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

    if (CanUseJsonKeyIndex(field_id) && !has_offset_input_) {
        return ExecRangeVisitorImplJsonForIndex<ExprValueType>();
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
                        res[i] = !CompareTwoJsonArray(array, val);
                    } else {
                        UnaryRangeJSONCompareNotEqual(x.value() != val);
                    }
                }
                break;
            }
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
                PatternMatchTranslator translator;
                auto regex_pattern = translator(val);
                RegexMatcher matcher(regex_pattern);
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
                        UnaryRangeJSONCompare(
                            matcher(ExprValueType(x.value())));
                    }
                }
                break;
            }
            default:
                PanicInfo(
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
    boost::regex rgx("/\\d+");
    boost::smatch match;
    if (boost::regex_search(input, match, rgx)) {
        std::string firstPart = input.substr(0, match.position());
        std::string secondPart = input.substr(match.position());
        return {firstPart, secondPart};
    } else {
        return {input, ""};
    }
}

template <typename ExprValueType>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplJsonForIndex() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    auto pointerpath = milvus::Json::pointer(expr_->column_.nested_path_);
    auto pointerpair = SplitAtFirstSlashDigit(pointerpath);
    std::string pointer = pointerpair.first;
    std::string arrayIndex = pointerpair.second;

#define UnaryRangeJSONIndexCompare(cmp)                       \
    do {                                                      \
        auto x = json.at<GetType>(offset, size);              \
        if (x.error()) {                                      \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto x = json.at<double>(offset, size);       \
                return !x.error() && (cmp);                   \
            }                                                 \
            return false;                                     \
        }                                                     \
        return (cmp);                                         \
    } while (false)

#define UnaryJSONTypeCompare(cmp)                                              \
    do {                                                                       \
        if constexpr (std::is_same_v<GetType, std::string_view>) {             \
            if (type == uint8_t(milvus::index::JSONType::STRING)) {            \
                auto x = json.at_string(offset, size);                         \
                return (cmp);                                                  \
            } else {                                                           \
                return false;                                                  \
            }                                                                  \
        } else if constexpr (std::is_same_v<GetType, double>) {                \
            if (type == uint8_t(milvus::index::JSONType::INT64)) {             \
                auto x =                                                       \
                    std::stoll(std::string(json.at_string(offset, size)));     \
                return (cmp);                                                  \
            } else if (type == uint8_t(milvus::index::JSONType::DOUBLE)) {     \
                auto x = std::stod(std::string(json.at_string(offset, size))); \
                return (cmp);                                                  \
            } else {                                                           \
                return false;                                                  \
            }                                                                  \
        } else if constexpr (std::is_same_v<GetType, int64_t>) {               \
            if (type == uint8_t(milvus::index::JSONType::INT64)) {             \
                auto x =                                                       \
                    std::stoll(std::string(json.at_string(offset, size)));     \
                return (cmp);                                                  \
            } else if (type == uint8_t(milvus::index::JSONType::DOUBLE)) {     \
                auto x = std::stod(std::string(json.at_string(offset, size))); \
                return (cmp);                                                  \
            } else {                                                           \
                return false;                                                  \
            }                                                                  \
        }                                                                      \
    } while (false)

#define UnaryJSONTypeCompareWithValue(cmp)                         \
    do {                                                           \
        if constexpr (std::is_same_v<GetType, int64_t>) {          \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) { \
                float x = *reinterpret_cast<float*>(&value);       \
                return (cmp);                                      \
            } else {                                               \
                int64_t x = value;                                 \
                return (cmp);                                      \
            }                                                      \
        } else if constexpr (std::is_same_v<GetType, double>) {    \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) { \
                float x = *reinterpret_cast<float*>(&value);       \
                return (cmp);                                      \
            } else {                                               \
                int64_t x = value;                                 \
                return (cmp);                                      \
            }                                                      \
        } else if constexpr (std::is_same_v<GetType, bool>) {      \
            bool x = *reinterpret_cast<bool*>(&value);             \
            return (cmp);                                          \
        }                                                          \
    } while (false)

#define CompareValueWithOpType(type, value, val, op_type)                    \
    switch (op_type) {                                                       \
        case proto::plan::GreaterThan:                                       \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) {           \
                UnaryJSONTypeCompareWithValue(x > static_cast<float>(val));  \
            } else {                                                         \
                UnaryJSONTypeCompareWithValue(x > val);                      \
            }                                                                \
            break;                                                           \
        case proto::plan::GreaterEqual:                                      \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) {           \
                UnaryJSONTypeCompareWithValue(x >= static_cast<float>(val)); \
            } else {                                                         \
                UnaryJSONTypeCompareWithValue(x >= val);                     \
            }                                                                \
            break;                                                           \
        case proto::plan::LessThan:                                          \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) {           \
                UnaryJSONTypeCompareWithValue(x < static_cast<float>(val));  \
            } else {                                                         \
                UnaryJSONTypeCompareWithValue(x < val);                      \
            }                                                                \
            break;                                                           \
        case proto::plan::LessEqual:                                         \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) {           \
                UnaryJSONTypeCompareWithValue(x <= static_cast<float>(val)); \
            } else {                                                         \
                UnaryJSONTypeCompareWithValue(x <= val);                     \
            }                                                                \
            break;                                                           \
        case proto::plan::Equal:                                             \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) {           \
                UnaryJSONTypeCompareWithValue(x == static_cast<float>(val)); \
            } else {                                                         \
                UnaryJSONTypeCompareWithValue(x == val);                     \
            }                                                                \
            break;                                                           \
        case proto::plan::NotEqual:                                          \
            if (type == uint8_t(milvus::index::JSONType::FLOAT)) {           \
                UnaryJSONTypeCompareWithValue(x != static_cast<float>(val)); \
            } else {                                                         \
                UnaryJSONTypeCompareWithValue(x != val);                     \
            }                                                                \
            break;                                                           \
        default:                                                             \
            return false;                                                    \
    }

#define UnaryRangeJSONIndexCompareWithArrayIndex(cmp)                     \
    do {                                                                  \
        if (type != uint8_t(milvus::index::JSONType::UNKNOWN)) {          \
            return false;                                                 \
        }                                                                 \
        auto array = json.array_at(offset, size);                         \
        if (array.error()) {                                              \
            return false;                                                 \
        }                                                                 \
        auto value = array.at_pointer(arrayIndex);                        \
        if (value.error()) {                                              \
            return false;                                                 \
        }                                                                 \
        if constexpr (std::is_same_v<GetType, int64_t> ||                 \
                      std::is_same_v<GetType, double>) {                  \
            if (!value.is_number()) {                                     \
                return false;                                             \
            }                                                             \
        } else if constexpr (std::is_same_v<GetType, std::string_view>) { \
            if (!value.is_string()) {                                     \
                return false;                                             \
            }                                                             \
        } else if constexpr (std::is_same_v<GetType, bool>) {             \
            if (!value.is_bool()) {                                       \
                return false;                                             \
            }                                                             \
        }                                                                 \
        auto x = value.get<GetType>();                                    \
        if (x.error()) {                                                  \
            if constexpr (std::is_same_v<GetType, int64_t>) {             \
                auto x = value.get<double>();                             \
                return !x.error() && (cmp);                               \
            }                                                             \
        }                                                                 \
        return (cmp);                                                     \
    } while (false)

#define UnaryRangeJSONIndexCompareNotEqual(cmp)               \
    do {                                                      \
        auto x = json.at<GetType>(offset, size);              \
        if (x.error()) {                                      \
            if constexpr (std::is_same_v<GetType, int64_t>) { \
                auto x = json.at<double>(offset, size);       \
                return x.error() || (cmp);                    \
            }                                                 \
            return true;                                      \
        }                                                     \
        return (cmp);                                         \
    } while (false)
#define UnaryRangeJSONIndexCompareNotEqualWithArrayIndex(cmp)             \
    do {                                                                  \
        auto array = json.array_at(offset, size);                         \
        if (array.error()) {                                              \
            return false;                                                 \
        }                                                                 \
        auto value = array.at_pointer(arrayIndex);                        \
        if (value.error()) {                                              \
            return false;                                                 \
        }                                                                 \
        if constexpr (std::is_same_v<GetType, int64_t> ||                 \
                      std::is_same_v<GetType, double>) {                  \
            if (!value.is_number()) {                                     \
                return false;                                             \
            }                                                             \
        } else if constexpr (std::is_same_v<GetType, std::string_view>) { \
            if (!value.is_string()) {                                     \
                return false;                                             \
            }                                                             \
        } else if constexpr (std::is_same_v<GetType, bool>) {             \
            if (!value.is_bool()) {                                       \
                return false;                                             \
            }                                                             \
        }                                                                 \
        auto x = value.get<GetType>();                                    \
        if (x.error()) {                                                  \
            if constexpr (std::is_same_v<GetType, int64_t>) {             \
                auto x = value.get<double>();                             \
                return x.error() || (cmp);                                \
            }                                                             \
        }                                                                 \
        return (cmp);                                                     \
    } while (false)

#define CHECKISJSONTYPEWITHOFFSET(type)                  \
    (type == uint8_t(milvus::index::JSONType::STRING) || \
     type == uint8_t(milvus::index::JSONType::DOUBLE) || \
     type == uint8_t(milvus::index::JSONType::INT64))

#define CHECKJSONTYPEISNUMBER(type)                           \
    if ((type != uint8_t(milvus::index::JSONType::INT32)) &&  \
        (type != uint8_t(milvus::index::JSONType::INT64)) &&  \
        (type != uint8_t(milvus::index::JSONType::FLOAT)) &&  \
        (type != uint8_t(milvus::index::JSONType::DOUBLE))) { \
        return false;                                         \
    }

#define ISVALIDJSONTYPE(type, GetType)                                   \
    if constexpr (std::is_same_v<GetType, int64_t>) {                    \
        CHECKJSONTYPEISNUMBER(type)                                      \
    } else if constexpr (std::is_same_v<GetType, std::string_view>) {    \
        if ((type != uint8_t(milvus::index::JSONType::STRING)) &&        \
            (type != uint8_t(milvus::index::JSONType::STRING_ESCAPE))) { \
            return false;                                                \
        }                                                                \
    } else if constexpr (std::is_same_v<GetType, double>) {              \
        CHECKJSONTYPEISNUMBER(type)                                      \
    } else if constexpr (std::is_same_v<GetType, bool>) {                \
        if (type != uint8_t(milvus::index::JSONType::BOOL)) {            \
            return false;                                                \
        }                                                                \
    }

    ExprValueType val = GetValueFromProto<ExprValueType>(expr_->val_);
    auto op_type = expr_->op_type_;

    if (cached_index_chunk_id_ != 0) {
        cached_index_chunk_id_ = 0;
        const segcore::SegmentInternalInterface* segment = nullptr;
        if (segment_->type() == SegmentType::Growing) {
            segment =
                dynamic_cast<const segcore::SegmentGrowingImpl*>(segment_);
        } else if (segment_->type() == SegmentType::Sealed) {
            segment = dynamic_cast<const segcore::SegmentSealed*>(segment_);
        }
        auto field_id = expr_->column_.field_id_;
        auto* index = segment->GetJsonKeyIndex(field_id);
        Assert(index != nullptr);
        Assert(segment != nullptr);
        auto filter_func = [segment,
                            field_id,
                            op_type,
                            val,
                            arrayIndex,
                            pointer](bool valid,
                                     uint8_t type,
                                     uint32_t row_id,
                                     uint16_t offset,
                                     uint16_t size,
                                     int32_t value) {
            if (valid) {
                if (type == uint8_t(milvus::index::JSONType::UNKNOWN) ||
                    !arrayIndex.empty()) {
                    return false;
                }
                ISVALIDJSONTYPE(type, GetType);
                switch (op_type) {
                    case proto::plan::GreaterThan:
                        CompareValueWithOpType(type, value, val, op_type);
                    case proto::plan::GreaterEqual:
                        CompareValueWithOpType(type, value, val, op_type);
                    case proto::plan::LessThan:
                        CompareValueWithOpType(type, value, val, op_type);
                    case proto::plan::LessEqual:
                        CompareValueWithOpType(type, value, val, op_type);
                    case proto::plan::Equal:
                        CompareValueWithOpType(type, value, val, op_type);
                    case proto::plan::NotEqual:
                        CompareValueWithOpType(type, value, val, op_type);
                    case proto::plan::PrefixMatch:
                    case proto::plan::Match:
                    default:
                        return false;
                }
            } else {
                auto json_pair = segment->GetJsonData(field_id, row_id);
                if (!json_pair.second) {
                    return false;
                }
                auto& json = json_pair.first;
                switch (op_type) {
                    case proto::plan::GreaterThan:
                        if constexpr (std::is_same_v<GetType,
                                                     proto::plan::Array>) {
                            return false;
                        } else {
                            if (!arrayIndex.empty()) {
                                UnaryRangeJSONIndexCompareWithArrayIndex(
                                    ExprValueType(x.value()) > val);
                            } else {
                                if (CHECKISJSONTYPEWITHOFFSET(type)) {
                                    UnaryJSONTypeCompare(x > val);
                                } else {
                                    UnaryRangeJSONIndexCompare(
                                        ExprValueType(x.value()) > val);
                                }
                            }
                        }
                    case proto::plan::GreaterEqual:
                        if constexpr (std::is_same_v<GetType,
                                                     proto::plan::Array>) {
                            return false;
                        } else {
                            if (!arrayIndex.empty()) {
                                UnaryRangeJSONIndexCompareWithArrayIndex(
                                    ExprValueType(x.value()) >= val);
                            } else {
                                if (CHECKISJSONTYPEWITHOFFSET(type)) {
                                    UnaryJSONTypeCompare(x >= val);
                                } else {
                                    UnaryRangeJSONIndexCompare(
                                        ExprValueType(x.value()) >= val);
                                }
                            }
                        }
                    case proto::plan::LessThan:
                        if constexpr (std::is_same_v<GetType,
                                                     proto::plan::Array>) {
                            return false;
                        } else {
                            if (!arrayIndex.empty()) {
                                UnaryRangeJSONIndexCompareWithArrayIndex(
                                    ExprValueType(x.value()) < val);
                            } else {
                                if (CHECKISJSONTYPEWITHOFFSET(type)) {
                                    UnaryJSONTypeCompare(x < val);
                                } else {
                                    UnaryRangeJSONIndexCompare(
                                        ExprValueType(x.value()) < val);
                                }
                            }
                        }
                    case proto::plan::LessEqual:
                        if constexpr (std::is_same_v<GetType,
                                                     proto::plan::Array>) {
                            return false;
                        } else {
                            if (!arrayIndex.empty()) {
                                UnaryRangeJSONIndexCompareWithArrayIndex(
                                    ExprValueType(x.value()) <= val);
                            } else {
                                if (CHECKISJSONTYPEWITHOFFSET(type)) {
                                    UnaryJSONTypeCompare(x <= val);
                                } else {
                                    UnaryRangeJSONIndexCompare(
                                        ExprValueType(x.value()) <= val);
                                }
                            }
                        }

                    case proto::plan::Equal:
                        if constexpr (std::is_same_v<GetType,
                                                     proto::plan::Array>) {
                            if (type !=
                                uint8_t(milvus::index::JSONType::UNKNOWN)) {
                                return false;
                            }
                            auto array = json.array_at(offset, size);
                            if (array.error()) {
                                return false;
                            }
                            return CompareTwoJsonArray(array.value(), val);
                        } else {
                            if (!arrayIndex.empty()) {
                                UnaryRangeJSONIndexCompareWithArrayIndex(
                                    ExprValueType(x.value()) == val);
                            } else {
                                if (CHECKISJSONTYPEWITHOFFSET(type)) {
                                    UnaryJSONTypeCompare(x == val);
                                } else {
                                    UnaryRangeJSONIndexCompare(
                                        ExprValueType(x.value()) == val);
                                }
                            }
                        }
                    case proto::plan::NotEqual:
                        if constexpr (std::is_same_v<GetType,
                                                     proto::plan::Array>) {
                            if (type !=
                                uint8_t(milvus::index::JSONType::UNKNOWN)) {
                                return false;
                            }
                            auto array = json.array_at(offset, size);
                            if (array.error()) {
                                return false;
                            }
                            return !CompareTwoJsonArray(array.value(), val);
                        } else {
                            if (!arrayIndex.empty()) {
                                UnaryRangeJSONIndexCompareNotEqualWithArrayIndex(
                                    ExprValueType(x.value()) != val);
                            } else {
                                if (CHECKISJSONTYPEWITHOFFSET(type)) {
                                    UnaryJSONTypeCompare(x != val);
                                } else {
                                    UnaryRangeJSONIndexCompareNotEqual(
                                        ExprValueType(x.value()) != val);
                                }
                            }
                        }
                    case proto::plan::PrefixMatch:
                        if constexpr (std::is_same_v<GetType,
                                                     proto::plan::Array>) {
                            return false;
                        } else {
                            if (!arrayIndex.empty()) {
                                UnaryRangeJSONIndexCompareWithArrayIndex(
                                    milvus::query::Match(
                                        ExprValueType(x.value()),
                                        val,
                                        op_type));
                            } else {
                                if (CHECKISJSONTYPEWITHOFFSET(type)) {
                                    UnaryJSONTypeCompare(
                                        milvus::query::Match(x, val, op_type));
                                } else {
                                    UnaryRangeJSONIndexCompare(
                                        milvus::query::Match(
                                            ExprValueType(x.value()),
                                            val,
                                            op_type));
                                }
                            }
                        }
                    case proto::plan::Match:
                        if constexpr (std::is_same_v<GetType,
                                                     proto::plan::Array>) {
                            return false;
                        } else {
                            PatternMatchTranslator translator;
                            auto regex_pattern = translator(val);
                            RegexMatcher matcher(regex_pattern);
                            if (!arrayIndex.empty()) {
                                UnaryRangeJSONIndexCompareWithArrayIndex(
                                    matcher(ExprValueType(x.value())));
                            } else {
                                UnaryRangeJSONIndexCompare(
                                    matcher(ExprValueType(x.value())));
                            }
                        }
                    default:
                        return false;
                }
            }
        };
        bool is_growing = segment_->type() == SegmentType::Growing;
        bool is_strong_consistency = consistency_level_ == 0;
        cached_index_chunk_res_ = index
                                      ->FilterByPath(pointer,
                                                     active_count_,
                                                     is_growing,
                                                     is_strong_consistency,
                                                     filter_func)
                                      .clone();
    }
    TargetBitmap result;
    result.append(
        cached_index_chunk_res_, current_data_global_pos_, real_batch_size);
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
            PanicInfo(
                OpTypeInvalid,
                fmt::format("match query does not support iterative filter"));
        }
        return ExecTextMatch();
    }

    if (CanUseIndex<T>() && !has_offset_input_) {
        return ExecRangeVisitorImplForIndex<T>();
    } else {
        return ExecRangeVisitorImplForData<T>(context);
    }
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

    auto real_batch_size = GetNextBatchSize();
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
            case proto::plan::Match: {
                UnaryIndexFunc<T, proto::plan::Match> func;
                res = std::move(func(index_ptr, val));
                break;
            }
            default:
                PanicInfo(
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
                    ? ProcessChunksForValidByOffsets<T>(is_index_mode_, *input)
                    : ProcessChunksForValid<T>(is_index_mode_);
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
                    PanicInfo(OpTypeInvalid,
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
        has_offset_input_ ? input->size() : GetNextBatchSize();
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
                PanicInfo(
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
        processed_size = ProcessDataByOffsets<T>(
            execute_sub_batch, skip_index_func, input, res, valid_res, val);
    } else {
        processed_size = ProcessDataChunks<T>(
            execute_sub_batch, skip_index_func, res, valid_res, val);
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
    bool res = is_index_mode_ && SegmentExpr::CanUseIndex<T>(expr_->op_type_);
    use_index_ = res;
    return res;
}

bool
PhyUnaryRangeFilterExpr::CanUseIndexForJson(DataType val_type) {
    use_index_ =
        segment_->HasIndex(field_id_,
                           milvus::Json::pointer(expr_->column_.nested_path_),
                           val_type);
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
    auto func = [op_type, slop](Index* index,
                                const std::string& query) -> TargetBitmap {
        if (op_type == proto::plan::OpType::TextMatch) {
            return index->MatchQuery(query);
        } else if (op_type == proto::plan::OpType::PhraseMatch) {
            return index->PhraseMatchQuery(query, slop);
        } else {
            PanicInfo(OpTypeInvalid,
                      "unsupported operator type for match query: {}",
                      op_type);
        }
    };
    auto res = ProcessTextMatchIndex(func, query);
    return res;
};

}  // namespace exec
}  // namespace milvus
