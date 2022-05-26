// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <algorithm>
#include <memory>
#include <utility>
#include <pb/schema.pb.h>
#include <vector>
#include <string>
#include "knowhere/common/Log.h"
#include "Meta.h"
#include "common/Utils.h"

namespace milvus::scalar {

template <typename T>
inline ScalarIndexSort<T>::ScalarIndexSort() : is_built_(false), data_() {
}

template <typename T>
inline ScalarIndexSort<T>::ScalarIndexSort(const size_t n, const T* values) : is_built_(false) {
    ScalarIndexSort<T>::Build(n, values);
}

template <typename T>
inline void
ScalarIndexSort<T>::BuildWithDataset(const DatasetPtr& dataset) {
    auto size = dataset->Get<int64_t>(knowhere::meta::ROWS);
    auto data = dataset->Get<const void*>(knowhere::meta::TENSOR);
    Build(size, reinterpret_cast<const T*>(data));
}

template <typename T>
inline void
ScalarIndexSort<T>::Build(const size_t n, const T* values) {
    data_.reserve(n);
    idx_to_offsets_.resize(n);
    T* p = const_cast<T*>(values);
    for (size_t i = 0; i < n; ++i) {
        data_.emplace_back(IndexStructure(*p++, i));
    }
    build();
}

template <typename T>
inline void
ScalarIndexSort<T>::build() {
    if (is_built_)
        return;
    if (data_.size() == 0) {
        // todo: throw an exception
        throw std::invalid_argument("ScalarIndexSort cannot build null values!");
    }
    std::sort(data_.begin(), data_.end());
    for (size_t i = 0; i < data_.size(); ++i) {
        idx_to_offsets_[data_[i].idx_] = i;
    }
    is_built_ = true;
}

template <typename T>
inline BinarySet
ScalarIndexSort<T>::Serialize(const Config& config) {
    AssertInfo(is_built_, "index has not been built");

    auto index_data_size = data_.size() * sizeof(IndexStructure<T>);
    std::shared_ptr<uint8_t[]> index_data(new uint8_t[index_data_size]);
    memcpy(index_data.get(), data_.data(), index_data_size);

    std::shared_ptr<uint8_t[]> index_length(new uint8_t[sizeof(size_t)]);
    auto index_size = data_.size();
    memcpy(index_length.get(), &index_size, sizeof(size_t));

    BinarySet res_set;
    res_set.Append("index_data", index_data, index_data_size);
    res_set.Append("index_length", index_length, sizeof(size_t));
    return res_set;
}

template <typename T>
inline void
ScalarIndexSort<T>::Load(const BinarySet& index_binary) {
    size_t index_size;
    auto index_length = index_binary.GetByName("index_length");
    memcpy(&index_size, index_length->data.get(), (size_t)index_length->size);

    auto index_data = index_binary.GetByName("index_data");
    data_.resize(index_size);
    idx_to_offsets_.resize(index_size);
    memcpy(data_.data(), index_data->data.get(), (size_t)index_data->size);
    for (size_t i = 0; i < data_.size(); ++i) {
        idx_to_offsets_[data_[i].idx_] = i;
    }
    is_built_ = true;
}

template <typename T>
inline const TargetBitmapPtr
ScalarIndexSort<T>::In(const size_t n, const T* values) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmapPtr bitset = std::make_unique<TargetBitmap>(data_.size());
    for (size_t i = 0; i < n; ++i) {
        auto lb = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        auto ub = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        for (; lb < ub; ++lb) {
            if (lb->a_ != *(values + i)) {
                std::cout << "error happens in ScalarIndexSort<T>::In, experted value is: " << *(values + i)
                          << ", but real value is: " << lb->a_;
            }
            bitset->set(lb->idx_);
        }
    }
    return bitset;
}

template <typename T>
inline const TargetBitmapPtr
ScalarIndexSort<T>::NotIn(const size_t n, const T* values) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmapPtr bitset = std::make_unique<TargetBitmap>(data_.size());
    bitset->set();
    for (size_t i = 0; i < n; ++i) {
        auto lb = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        auto ub = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        for (; lb < ub; ++lb) {
            if (lb->a_ != *(values + i)) {
                std::cout << "error happens in ScalarIndexSort<T>::NotIn, experted value is: " << *(values + i)
                          << ", but real value is: " << lb->a_;
            }
            bitset->reset(lb->idx_);
        }
    }
    return bitset;
}

template <typename T>
inline const TargetBitmapPtr
ScalarIndexSort<T>::Range(const T value, const OpType op) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmapPtr bitset = std::make_unique<TargetBitmap>(data_.size());
    auto lb = data_.begin();
    auto ub = data_.end();
    switch (op) {
        case OpType::LessThan:
            ub = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OpType::LessEqual:
            ub = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OpType::GreaterThan:
            lb = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OpType::GreaterEqual:
            lb = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        default:
            throw std::invalid_argument(std::string("Invalid OperatorType: ") + std::to_string((int)op) + "!");
    }
    for (; lb < ub; ++lb) {
        bitset->set(lb->idx_);
    }
    return bitset;
}

template <typename T>
inline const TargetBitmapPtr
ScalarIndexSort<T>::Range(T lower_bound_value, bool lb_inclusive, T upper_bound_value, bool ub_inclusive) {
    AssertInfo(is_built_, "index has not been built");
    TargetBitmapPtr bitset = std::make_unique<TargetBitmap>(data_.size());
    if (lower_bound_value > upper_bound_value ||
        (lower_bound_value == upper_bound_value && !(lb_inclusive && ub_inclusive))) {
        return bitset;
    }
    auto lb = data_.begin();
    auto ub = data_.end();
    if (lb_inclusive) {
        lb = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(lower_bound_value));
    } else {
        lb = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(lower_bound_value));
    }
    if (ub_inclusive) {
        ub = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(upper_bound_value));
    } else {
        ub = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(upper_bound_value));
    }
    for (; lb < ub; ++lb) {
        bitset->set(lb->idx_);
    }
    return bitset;
}

template <typename T>
inline T
ScalarIndexSort<T>::Reverse_Lookup(size_t idx) const {
    AssertInfo(idx < idx_to_offsets_.size(), "out of range of total count");
    AssertInfo(is_built_, "index has not been built");

    auto offset = idx_to_offsets_[idx];
    return data_[offset].a_;
}

}  // namespace milvus::scalar
