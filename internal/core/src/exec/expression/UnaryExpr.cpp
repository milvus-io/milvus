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
#include "common/Json.h"

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
        const Index& index =
            segment_->chunk_scalar_index<IndexInnerType>(field_id_, i);

        if (index.GetIndexType() == milvus::index::ScalarIndexType::BITMAP) {
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
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplArrayForIndex() {
    return ExecRangeVisitorImplArray<T>();
}

template <>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplArrayForIndex<
    proto::plan::Array>() {
    switch (expr_->op_type_) {
        case proto::plan::Equal:
        case proto::plan::NotEqual: {
            switch (expr_->column_.element_type_) {
                case DataType::BOOL: {
                    return ExecArrayEqualForIndex<bool>(expr_->op_type_ ==
                                                        proto::plan::NotEqual);
                }
                case DataType::INT8: {
                    return ExecArrayEqualForIndex<int8_t>(
                        expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::INT16: {
                    return ExecArrayEqualForIndex<int16_t>(
                        expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::INT32: {
                    return ExecArrayEqualForIndex<int32_t>(
                        expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::INT64: {
                    return ExecArrayEqualForIndex<int64_t>(
                        expr_->op_type_ == proto::plan::NotEqual);
                }
                case DataType::FLOAT:
                case DataType::DOUBLE: {
                    // not accurate on floating point number, rollback to bruteforce.
                    return ExecRangeVisitorImplArray<proto::plan::Array>();
                }
                case DataType::VARCHAR: {
                    if (segment_->type() == SegmentType::Growing) {
                        return ExecArrayEqualForIndex<std::string>(
                            expr_->op_type_ == proto::plan::NotEqual);
                    } else {
                        return ExecArrayEqualForIndex<std::string_view>(
                            expr_->op_type_ == proto::plan::NotEqual);
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
            return ExecRangeVisitorImplArray<proto::plan::Array>();
    }
}

void
PhyUnaryRangeFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
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
            auto val_type = expr_->val_.val_case();
            switch (val_type) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    result = ExecRangeVisitorImplJson<bool>();
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    result = ExecRangeVisitorImplJson<int64_t>();
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    result = ExecRangeVisitorImplJson<double>();
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    result = ExecRangeVisitorImplJson<std::string>();
                    break;
                case proto::plan::GenericValue::ValCase::kArrayVal:
                    result = ExecRangeVisitorImplJson<proto::plan::Array>();
                    break;
                default:
                    PanicInfo(
                        DataTypeInvalid, "unknown data type: {}", val_type);
            }
            break;
        }
        case DataType::ARRAY: {
            auto val_type = expr_->val_.val_case();
            switch (val_type) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplArray<bool>();
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplArray<int64_t>();
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplArray<double>();
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    SetNotUseIndex();
                    result = ExecRangeVisitorImplArray<std::string>();
                    break;
                case proto::plan::GenericValue::ValCase::kArrayVal:
                    if (CanUseIndexForArray<milvus::Array>()) {
                        result = ExecRangeVisitorImplArrayForIndex<
                            proto::plan::Array>();
                    } else {
                        result =
                            ExecRangeVisitorImplArray<proto::plan::Array>();
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
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplArray() {
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

    ValueType val = GetValueFromProto<ValueType>(expr_->val_);
    auto op_type = expr_->op_type_;
    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }
    auto execute_sub_batch = [op_type](const milvus::ArrayView* data,
                                       const int size,
                                       TargetBitmapView res,
                                       ValueType val,
                                       int index) {
        switch (op_type) {
            case proto::plan::GreaterThan: {
                UnaryElementFuncForArray<ValueType, proto::plan::GreaterThan>
                    func;
                func(data, size, val, index, res);
                break;
            }
            case proto::plan::GreaterEqual: {
                UnaryElementFuncForArray<ValueType, proto::plan::GreaterEqual>
                    func;
                func(data, size, val, index, res);
                break;
            }
            case proto::plan::LessThan: {
                UnaryElementFuncForArray<ValueType, proto::plan::LessThan> func;
                func(data, size, val, index, res);
                break;
            }
            case proto::plan::LessEqual: {
                UnaryElementFuncForArray<ValueType, proto::plan::LessEqual>
                    func;
                func(data, size, val, index, res);
                break;
            }
            case proto::plan::Equal: {
                UnaryElementFuncForArray<ValueType, proto::plan::Equal> func;
                func(data, size, val, index, res);
                break;
            }
            case proto::plan::NotEqual: {
                UnaryElementFuncForArray<ValueType, proto::plan::NotEqual> func;
                func(data, size, val, index, res);
                break;
            }
            case proto::plan::PrefixMatch: {
                UnaryElementFuncForArray<ValueType, proto::plan::PrefixMatch>
                    func;
                func(data, size, val, index, res);
                break;
            }
            case proto::plan::Match: {
                UnaryElementFuncForArray<ValueType, proto::plan::Match> func;
                func(data, size, val, index, res);
                break;
            }
            default:
                PanicInfo(
                    OpTypeInvalid,
                    fmt::format("unsupported operator type for unary expr: {}",
                                op_type));
        }
    };
    int64_t processed_size = ProcessDataChunks<milvus::ArrayView>(
        execute_sub_batch, std::nullptr_t{}, res, val, index);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecArrayEqualForIndex(bool reverse) {
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
        return ExecRangeVisitorImplArray<proto::plan::Array>();
    }

    // cache the result to suit the framework.
    auto batch_res =
        ProcessIndexChunks<IndexInnerType>([this, &val, reverse](Index* _) {
            boost::container::vector<IndexInnerType> elems;
            for (auto const& element : val.array()) {
                auto e = GetValueFromProto<IndexInnerType>(element);
                if (std::find(elems.begin(), elems.end(), e) == elems.end()) {
                    elems.push_back(e);
                }
            }

            // filtering by index, get candidates.
            auto size_per_chunk = segment_->size_per_chunk();
            auto retrieve = [ size_per_chunk, this ](int64_t offset) -> auto {
                auto chunk_idx = offset / size_per_chunk;
                auto chunk_offset = offset % size_per_chunk;
                const auto& chunk =
                    segment_->template chunk_data<milvus::ArrayView>(field_id_,
                                                                     chunk_idx);
                return chunk.data() + chunk_offset;
            };

            // compare the array via the raw data.
            auto filter = [&retrieve, &val, reverse](size_t offset) -> bool {
                auto data_ptr = retrieve(offset);
                return data_ptr->is_same_array(val) ^ reverse;
            };

            // collect all candidates.
            std::unordered_set<size_t> candidates;
            std::unordered_set<size_t> tmp_candidates;
            auto first_callback = [&candidates](size_t offset) -> void {
                candidates.insert(offset);
            };
            auto callback = [&candidates,
                             &tmp_candidates](size_t offset) -> void {
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
                res[candidate] = filter(candidate);
            }
            return res;
        });
    AssertInfo(batch_res.size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               batch_res.size(),
               real_batch_size);

    // return the result.
    return std::make_shared<ColumnVector>(std::move(batch_res));
}

template <typename ExprValueType>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplJson() {
    using GetType =
        std::conditional_t<std::is_same_v<ExprValueType, std::string>,
                           std::string_view,
                           ExprValueType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    ExprValueType val = GetValueFromProto<ExprValueType>(expr_->val_);
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    auto op_type = expr_->op_type_;
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

#define UnaryRangeJSONCompare(cmp)                             \
    do {                                                       \
        auto x = data[i].template at<GetType>(pointer);        \
        if (x.error()) {                                       \
            if constexpr (std::is_same_v<GetType, int64_t>) {  \
                auto x = data[i].template at<double>(pointer); \
                res[i] = !x.error() && (cmp);                  \
                break;                                         \
            }                                                  \
            res[i] = false;                                    \
            break;                                             \
        }                                                      \
        res[i] = (cmp);                                        \
    } while (false)

#define UnaryRangeJSONCompareNotEqual(cmp)                     \
    do {                                                       \
        auto x = data[i].template at<GetType>(pointer);        \
        if (x.error()) {                                       \
            if constexpr (std::is_same_v<GetType, int64_t>) {  \
                auto x = data[i].template at<double>(pointer); \
                res[i] = x.error() || (cmp);                   \
                break;                                         \
            }                                                  \
            res[i] = true;                                     \
            break;                                             \
        }                                                      \
        res[i] = (cmp);                                        \
    } while (false)

    auto execute_sub_batch = [op_type, pointer](const milvus::Json* data,
                                                const int size,
                                                TargetBitmapView res,
                                                ExprValueType val) {
        switch (op_type) {
            case proto::plan::GreaterThan: {
                for (size_t i = 0; i < size; ++i) {
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
    };
    int64_t processed_size = ProcessDataChunks<milvus::Json>(
        execute_sub_batch, std::nullptr_t{}, res, val);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImpl() {
    if (CanUseIndex<T>()) {
        return ExecRangeVisitorImplForIndex<T>();
    } else {
        return ExecRangeVisitorImplForData<T>();
    }
}

template <typename T>
VectorPtr
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForIndex() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
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
    auto val = GetValueFromProto<IndexInnerType>(expr_->val_);
    auto res = ProcessIndexChunks<T>(execute_sub_batch, val);
    AssertInfo(res.size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res.size(),
               real_batch_size);
    return std::make_shared<ColumnVector>(std::move(res));
}

template <typename T>
ColumnVectorPtr
PhyUnaryRangeFilterExpr::PreCheckOverflow() {
    if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
        int64_t val = GetValueFromProto<int64_t>(expr_->val_);

        if (milvus::query::out_of_range<T>(val)) {
            int64_t batch_size =
                overflow_check_pos_ + batch_size_ >= active_count_
                    ? active_count_ - overflow_check_pos_
                    : batch_size_;
            overflow_check_pos_ += batch_size;
            if (cached_overflow_res_ != nullptr &&
                cached_overflow_res_->size() == batch_size) {
                return cached_overflow_res_;
            }
            switch (expr_->op_type_) {
                case proto::plan::GreaterThan:
                case proto::plan::GreaterEqual: {
                    auto res_vec = std::make_shared<ColumnVector>(
                        TargetBitmap(batch_size));
                    cached_overflow_res_ = res_vec;
                    TargetBitmapView res(res_vec->GetRawData(), batch_size);

                    if (milvus::query::lt_lb<T>(val)) {
                        res.set();
                        return res_vec;
                    }
                    return res_vec;
                }
                case proto::plan::LessThan:
                case proto::plan::LessEqual: {
                    auto res_vec = std::make_shared<ColumnVector>(
                        TargetBitmap(batch_size));
                    cached_overflow_res_ = res_vec;
                    TargetBitmapView res(res_vec->GetRawData(), batch_size);

                    if (milvus::query::gt_ub<T>(val)) {
                        res.set();
                        return res_vec;
                    }
                    return res_vec;
                }
                case proto::plan::Equal: {
                    auto res_vec = std::make_shared<ColumnVector>(
                        TargetBitmap(batch_size));
                    cached_overflow_res_ = res_vec;
                    TargetBitmapView res(res_vec->GetRawData(), batch_size);

                    res.reset();
                    return res_vec;
                }
                case proto::plan::NotEqual: {
                    auto res_vec = std::make_shared<ColumnVector>(
                        TargetBitmap(batch_size));
                    cached_overflow_res_ = res_vec;
                    TargetBitmapView res(res_vec->GetRawData(), batch_size);

                    res.set();
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
PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForData() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    if (auto res = PreCheckOverflow<T>()) {
        return res;
    }

    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }
    IndexInnerType val = GetValueFromProto<IndexInnerType>(expr_->val_);
    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);
    auto expr_type = expr_->op_type_;
    auto execute_sub_batch = [expr_type](const T* data,
                                         const int size,
                                         TargetBitmapView res,
                                         IndexInnerType val) {
        switch (expr_type) {
            case proto::plan::GreaterThan: {
                UnaryElementFunc<T, proto::plan::GreaterThan> func;
                func(data, size, val, res);
                break;
            }
            case proto::plan::GreaterEqual: {
                UnaryElementFunc<T, proto::plan::GreaterEqual> func;
                func(data, size, val, res);
                break;
            }
            case proto::plan::LessThan: {
                UnaryElementFunc<T, proto::plan::LessThan> func;
                func(data, size, val, res);
                break;
            }
            case proto::plan::LessEqual: {
                UnaryElementFunc<T, proto::plan::LessEqual> func;
                func(data, size, val, res);
                break;
            }
            case proto::plan::Equal: {
                UnaryElementFunc<T, proto::plan::Equal> func;
                func(data, size, val, res);
                break;
            }
            case proto::plan::NotEqual: {
                UnaryElementFunc<T, proto::plan::NotEqual> func;
                func(data, size, val, res);
                break;
            }
            case proto::plan::PrefixMatch: {
                UnaryElementFunc<T, proto::plan::PrefixMatch> func;
                func(data, size, val, res);
                break;
            }
            case proto::plan::Match: {
                UnaryElementFunc<T, proto::plan::Match> func;
                func(data, size, val, res);
                break;
            }
            default:
                PanicInfo(
                    OpTypeInvalid,
                    fmt::format("unsupported operator type for unary expr: {}",
                                expr_type));
        }
    };
    auto skip_index_func = [expr_type, val](const SkipIndex& skip_index,
                                            FieldId field_id,
                                            int64_t chunk_id) {
        return skip_index.CanSkipUnaryRange<T>(
            field_id, chunk_id, expr_type, val);
    };
    int64_t processed_size =
        ProcessDataChunks<T>(execute_sub_batch, skip_index_func, res, val);
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

}  // namespace exec
}  // namespace milvus
