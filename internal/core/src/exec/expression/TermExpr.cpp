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

#include "TermExpr.h"
#include "query/Utils.h"
namespace milvus {
namespace exec {

void
PhyTermFilterExpr::Eval(EvalCtx& context, VectorPtr& result) {
    if (is_pk_field_) {
        result = ExecPkTermImpl();
        return;
    }
    switch (expr_->column_.data_type_) {
        case DataType::BOOL: {
            result = ExecVisitorImpl<bool>();
            break;
        }
        case DataType::INT8: {
            result = ExecVisitorImpl<int8_t>();
            break;
        }
        case DataType::INT16: {
            result = ExecVisitorImpl<int16_t>();
            break;
        }
        case DataType::INT32: {
            result = ExecVisitorImpl<int32_t>();
            break;
        }
        case DataType::INT64: {
            result = ExecVisitorImpl<int64_t>();
            break;
        }
        case DataType::FLOAT: {
            result = ExecVisitorImpl<float>();
            break;
        }
        case DataType::DOUBLE: {
            result = ExecVisitorImpl<double>();
            break;
        }
        case DataType::VARCHAR: {
            if (segment_->type() == SegmentType::Growing &&
                !storage::MmapManager::GetInstance()
                     .GetMmapConfig()
                     .growing_enable_mmap) {
                result = ExecVisitorImpl<std::string>();
            } else {
                result = ExecVisitorImpl<std::string_view>();
            }
            break;
        }
        case DataType::JSON: {
            if (expr_->vals_.size() == 0) {
                result = ExecVisitorImplTemplateJson<bool>();
                break;
            }
            auto type = expr_->vals_[0].val_case();
            switch (type) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    result = ExecVisitorImplTemplateJson<bool>();
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    result = ExecVisitorImplTemplateJson<int64_t>();
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    result = ExecVisitorImplTemplateJson<double>();
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    result = ExecVisitorImplTemplateJson<std::string>();
                    break;
                default:
                    PanicInfo(DataTypeInvalid, "unknown data type: {}", type);
            }
            break;
        }
        case DataType::ARRAY: {
            if (expr_->vals_.size() == 0) {
                SetNotUseIndex();
                result = ExecVisitorImplTemplateArray<bool>();
                break;
            }
            auto type = expr_->vals_[0].val_case();
            switch (type) {
                case proto::plan::GenericValue::ValCase::kBoolVal:
                    SetNotUseIndex();
                    result = ExecVisitorImplTemplateArray<bool>();
                    break;
                case proto::plan::GenericValue::ValCase::kInt64Val:
                    SetNotUseIndex();
                    result = ExecVisitorImplTemplateArray<int64_t>();
                    break;
                case proto::plan::GenericValue::ValCase::kFloatVal:
                    SetNotUseIndex();
                    result = ExecVisitorImplTemplateArray<double>();
                    break;
                case proto::plan::GenericValue::ValCase::kStringVal:
                    SetNotUseIndex();
                    result = ExecVisitorImplTemplateArray<std::string>();
                    break;
                default:
                    PanicInfo(DataTypeInvalid, "unknown data type: {}", type);
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
bool
PhyTermFilterExpr::CanSkipSegment() {
    const auto& skip_index = segment_->GetSkipIndex();
    T min, max;
    for (auto i = 0; i < expr_->vals_.size(); i++) {
        auto val = GetValueFromProto<T>(expr_->vals_[i]);
        max = i == 0 ? val : std::max(val, max);
        min = i == 0 ? val : std::min(val, min);
    }
    // using skip index to help skipping this segment
    if (segment_->type() == SegmentType::Sealed &&
        skip_index.CanSkipBinaryRange<T>(field_id_, 0, min, max, true, true)) {
        cached_bits_.resize(active_count_, false);
        cached_offsets_ = std::make_shared<ColumnVector>(DataType::INT64, 0);
        cached_offsets_inited_ = true;
        return true;
    }
    return false;
}

void
PhyTermFilterExpr::InitPkCacheOffset() {
    auto id_array = std::make_unique<IdArray>();
    switch (pk_type_) {
        case DataType::INT64: {
            if (CanSkipSegment<int64_t>()) {
                return;
            }
            auto dst_ids = id_array->mutable_int_id();
            for (const auto& id : expr_->vals_) {
                dst_ids->add_data(GetValueFromProto<int64_t>(id));
            }
            break;
        }
        case DataType::VARCHAR: {
            if (CanSkipSegment<std::string>()) {
                return;
            }
            auto dst_ids = id_array->mutable_str_id();
            for (const auto& id : expr_->vals_) {
                dst_ids->add_data(GetValueFromProto<std::string>(id));
            }
            break;
        }
        default: {
            PanicInfo(DataTypeInvalid, "unsupported data type {}", pk_type_);
        }
    }

    auto [uids, seg_offsets] =
        segment_->search_ids(*id_array, query_timestamp_);
    cached_bits_.resize(active_count_, false);
    cached_offsets_ =
        std::make_shared<ColumnVector>(DataType::INT64, seg_offsets.size());
    int64_t* cached_offsets_ptr = (int64_t*)cached_offsets_->GetRawData();
    int i = 0;
    for (const auto& offset : seg_offsets) {
        auto _offset = (int64_t)offset.get();
        cached_bits_[_offset] = true;
        cached_offsets_ptr[i++] = _offset;
    }
    cached_offsets_inited_ = true;
}

VectorPtr
PhyTermFilterExpr::ExecPkTermImpl() {
    if (!cached_offsets_inited_) {
        InitPkCacheOffset();
    }

    auto real_batch_size =
        current_data_chunk_pos_ + batch_size_ >= active_count_
            ? active_count_ - current_data_chunk_pos_
            : batch_size_;

    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    auto current_chunk_view =
        cached_bits_.view(current_data_chunk_pos_, real_batch_size);
    res |= current_chunk_view;
    current_data_chunk_pos_ += real_batch_size;

    if (use_cache_offsets_) {
        std::vector<VectorPtr> vecs{res_vec, cached_offsets_};
        return std::make_shared<RowVector>(vecs);
    } else {
        return res_vec;
    }
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplTemplateJson() {
    if (expr_->is_in_field_) {
        return ExecTermJsonVariableInField<ValueType>();
    } else {
        return ExecTermJsonFieldInVariable<ValueType>();
    }
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplTemplateArray() {
    if (expr_->is_in_field_) {
        return ExecTermArrayVariableInField<ValueType>();
    } else {
        return ExecTermArrayFieldInVariable<ValueType>();
    }
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecTermArrayVariableInField() {
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

    AssertInfo(expr_->vals_.size() == 1,
               "element length in json array must be one");
    ValueType target_val = GetValueFromProto<ValueType>(expr_->vals_[0]);

    auto execute_sub_batch = [](const ArrayView* data,
                                const int size,
                                TargetBitmapView res,
                                const ValueType& target_val) {
        auto executor = [&](size_t i) {
            for (int i = 0; i < data[i].length(); i++) {
                auto val = data[i].template get_data<GetType>(i);
                if (val == target_val) {
                    return true;
                }
            }
            return false;
        };
        for (int i = 0; i < size; ++i) {
            executor(i);
        }
    };

    int64_t processed_size = ProcessDataChunks<milvus::ArrayView>(
        execute_sub_batch, std::nullptr_t{}, res, target_val);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecTermArrayFieldInVariable() {
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

    int index = -1;
    if (expr_->column_.nested_path_.size() > 0) {
        index = std::stoi(expr_->column_.nested_path_[0]);
    }
    std::unordered_set<ValueType> term_set;
    for (const auto& element : expr_->vals_) {
        term_set.insert(GetValueFromProto<ValueType>(element));
    }

    if (term_set.empty()) {
        res.reset();
        MoveCursor();
        return res_vec;
    }

    auto execute_sub_batch = [](const ArrayView* data,
                                const int size,
                                TargetBitmapView res,
                                int index,
                                const std::unordered_set<ValueType>& term_set) {
        for (int i = 0; i < size; ++i) {
            if (index >= data[i].length()) {
                res[i] = false;
                continue;
            }
            auto value = data[i].get_data<GetType>(index);
            res[i] = term_set.find(ValueType(value)) != term_set.end();
        }
    };

    int64_t processed_size = ProcessDataChunks<milvus::ArrayView>(
        execute_sub_batch, std::nullptr_t{}, res, index, term_set);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecTermJsonVariableInField() {
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

    AssertInfo(expr_->vals_.size() == 1,
               "element length in json array must be one");
    ValueType val = GetValueFromProto<ValueType>(expr_->vals_[0]);
    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);

    auto execute_sub_batch = [](const Json* data,
                                const int size,
                                TargetBitmapView res,
                                const std::string pointer,
                                const ValueType& target_val) {
        auto executor = [&](size_t i) {
            auto doc = data[i].doc();
            auto array = doc.at_pointer(pointer).get_array();
            if (array.error())
                return false;
            for (auto it = array.begin(); it != array.end(); ++it) {
                auto val = (*it).template get<GetType>();
                if (val.error()) {
                    return false;
                }
                if (val.value() == target_val) {
                    return true;
                }
            }
            return false;
        };
        for (size_t i = 0; i < size; ++i) {
            res[i] = executor(i);
        }
    };
    int64_t processed_size = ProcessDataChunks<milvus::Json>(
        execute_sub_batch, std::nullptr_t{}, res, pointer, val);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename ValueType>
VectorPtr
PhyTermFilterExpr::ExecTermJsonFieldInVariable() {
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

    auto pointer = milvus::Json::pointer(expr_->column_.nested_path_);
    std::unordered_set<ValueType> term_set;
    for (const auto& element : expr_->vals_) {
        term_set.insert(GetValueFromProto<ValueType>(element));
    }

    if (term_set.empty()) {
        res.reset();
        MoveCursor();
        return res_vec;
    }

    auto execute_sub_batch = [](const Json* data,
                                const int size,
                                TargetBitmapView res,
                                const std::string pointer,
                                const std::unordered_set<ValueType>& terms) {
        auto executor = [&](size_t i) {
            auto x = data[i].template at<GetType>(pointer);
            if (x.error()) {
                if constexpr (std::is_same_v<GetType, std::int64_t>) {
                    auto x = data[i].template at<double>(pointer);
                    if (x.error()) {
                        return false;
                    }

                    auto value = x.value();
                    // if the term set is {1}, and the value is 1.1, we should not return true.
                    return std::floor(value) == value &&
                           terms.find(ValueType(value)) != terms.end();
                }
                return false;
            }
            return terms.find(ValueType(x.value())) != terms.end();
        };
        for (size_t i = 0; i < size; ++i) {
            res[i] = executor(i);
        }
    };
    int64_t processed_size = ProcessDataChunks<milvus::Json>(
        execute_sub_batch, std::nullptr_t{}, res, pointer, term_set);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

template <typename T>
VectorPtr
PhyTermFilterExpr::ExecVisitorImpl() {
    if (is_index_mode_) {
        return ExecVisitorImplForIndex<T>();
    } else {
        return ExecVisitorImplForData<T>();
    }
}

template <typename T>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplForIndex() {
    typedef std::
        conditional_t<std::is_same_v<T, std::string_view>, std::string, T>
            IndexInnerType;
    using Index = index::ScalarIndex<IndexInnerType>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    std::vector<IndexInnerType> vals;
    for (auto& val : expr_->vals_) {
        // Integral overflow process
        bool overflowed = false;
        auto converted_val = GetValueFromProtoWithOverflow<T>(val, overflowed);
        if (!overflowed) {
            vals.emplace_back(converted_val);
        }
    }
    auto execute_sub_batch = [](Index* index_ptr,
                                const std::vector<IndexInnerType>& vals) {
        TermIndexFunc<T> func;
        return func(index_ptr, vals.size(), vals.data());
    };
    auto res = ProcessIndexChunks<T>(execute_sub_batch, vals);
    AssertInfo(res.size() == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               res.size(),
               real_batch_size);
    return std::make_shared<ColumnVector>(std::move(res));
}

template <>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplForIndex<bool>() {
    using Index = index::ScalarIndex<bool>;
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    std::vector<uint8_t> vals;
    for (auto& val : expr_->vals_) {
        vals.emplace_back(GetValueFromProto<bool>(val) ? 1 : 0);
    }
    auto execute_sub_batch = [](Index* index_ptr,
                                const std::vector<uint8_t>& vals) {
        TermIndexFunc<bool> func;
        return std::move(func(index_ptr, vals.size(), (bool*)vals.data()));
    };
    auto res = ProcessIndexChunks<bool>(execute_sub_batch, vals);
    return std::make_shared<ColumnVector>(std::move(res));
}

template <typename T>
VectorPtr
PhyTermFilterExpr::ExecVisitorImplForData() {
    auto real_batch_size = GetNextBatchSize();
    if (real_batch_size == 0) {
        return nullptr;
    }

    auto res_vec =
        std::make_shared<ColumnVector>(TargetBitmap(real_batch_size));
    TargetBitmapView res(res_vec->GetRawData(), real_batch_size);

    std::vector<T> vals;
    for (auto& val : expr_->vals_) {
        // Integral overflow process
        bool overflowed = false;
        auto converted_val = GetValueFromProtoWithOverflow<T>(val, overflowed);
        if (!overflowed) {
            vals.emplace_back(converted_val);
        }
    }
    std::unordered_set<T> vals_set(vals.begin(), vals.end());
    auto execute_sub_batch = [](const T* data,
                                const int size,
                                TargetBitmapView res,
                                const std::unordered_set<T>& vals) {
        TermElementFuncSet<T> func;
        for (size_t i = 0; i < size; ++i) {
            res[i] = func(vals, data[i]);
        }
    };
    int64_t processed_size = ProcessDataChunks<T>(
        execute_sub_batch, std::nullptr_t{}, res, vals_set);
    AssertInfo(processed_size == real_batch_size,
               "internal error: expr processed rows {} not equal "
               "expect batch size {}",
               processed_size,
               real_batch_size);
    return res_vec;
}

}  //namespace exec
}  // namespace milvus
