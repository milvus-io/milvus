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
#include "knowhere/common/Log.h"
#include "knowhere/index/structured_index/StructuredIndexSort.h"

namespace milvus {
namespace knowhere {

template <typename T>
StructuredIndexSort<T>::StructuredIndexSort() : is_built_(false), data_() {
}

template <typename T>
StructuredIndexSort<T>::StructuredIndexSort(const size_t n, const T* values) : is_built_(false) {
    StructuredIndexSort<T>::Build(n, values);
}

template <typename T>
StructuredIndexSort<T>::~StructuredIndexSort() {
}

template <typename T>
void
StructuredIndexSort<T>::Build(const size_t n, const T* values) {
    data_.reserve(n);
    T* p = const_cast<T*>(values);
    for (size_t i = 0; i < n; ++i) {
        data_.emplace_back(IndexStructure(*p++, i));
    }
    build();
}

template <typename T>
void
StructuredIndexSort<T>::build() {
    if (is_built_)
        return;
    if (data_.size() == 0) {
        // todo: throw an exception
        KNOWHERE_THROW_MSG("StructuredIndexSort cannot build null values!");
    }
    std::sort(data_.begin(), data_.end());
    is_built_ = true;
}

template <typename T>
BinarySet
StructuredIndexSort<T>::Serialize(const milvus::knowhere::Config& config) {
    if (!is_built_) {
        build();
    }

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
void
StructuredIndexSort<T>::Load(const milvus::knowhere::BinarySet& index_binary) {
    try {
        size_t index_size;
        auto index_length = index_binary.GetByName("index_length");
        memcpy(&index_size, index_length->data.get(), (size_t)index_length->size);

        auto index_data = index_binary.GetByName("index_data");
        data_.resize(index_size);
        memcpy(data_.data(), index_data->data.get(), (size_t)index_data->size);
        is_built_ = true;
    } catch (...) {
        KNOHWERE_ERROR_MSG("StructuredIndexSort Load failed!");
    }
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexSort<T>::In(const size_t n, const T* values) {
    if (!is_built_) {
        build();
    }
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(data_.size());
    for (size_t i = 0; i < n; ++i) {
        auto lb = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        auto ub = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        for (; lb < ub; ++lb) {
            if (lb->a_ != *(values + i)) {
                LOG_KNOWHERE_ERROR_ << "error happens in StructuredIndexSort<T>::In, experted value is: "
                                    << *(values + i) << ", but real value is: " << lb->a_;
            }
            bitset->set(lb->idx_);
        }
    }
    return bitset;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexSort<T>::NotIn(const size_t n, const T* values) {
    if (!is_built_) {
        build();
    }
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(data_.size(), 0xff);
    for (size_t i = 0; i < n; ++i) {
        auto lb = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        auto ub = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(*(values + i)));
        for (; lb < ub; ++lb) {
            if (lb->a_ != *(values + i)) {
                LOG_KNOWHERE_ERROR_ << "error happens in StructuredIndexSort<T>::NotIn, experted value is: "
                                    << *(values + i) << ", but real value is: " << lb->a_;
            }
            bitset->clear(lb->idx_);
        }
    }
    return bitset;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexSort<T>::Range(const T value, const OperatorType op) {
    if (!is_built_) {
        build();
    }
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(data_.size());
    auto lb = data_.begin();
    auto ub = data_.end();
    switch (op) {
        case OperatorType::LT:
            ub = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OperatorType::LE:
            ub = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OperatorType::GT:
            lb = std::upper_bound(data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        case OperatorType::GE:
            lb = std::lower_bound(data_.begin(), data_.end(), IndexStructure<T>(value));
            break;
        default:
            KNOWHERE_THROW_MSG("Invalid OperatorType:" + std::to_string((int)op) + "!");
    }
    for (; lb < ub; ++lb) {
        bitset->set(lb->idx_);
    }
    return bitset;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexSort<T>::Range(T lower_bound_value, bool lb_inclusive, T upper_bound_value, bool ub_inclusive) {
    if (!is_built_) {
        build();
    }
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(data_.size());
    if (lower_bound_value > upper_bound_value) {
        std::swap(lower_bound_value, upper_bound_value);
        std::swap(lb_inclusive, ub_inclusive);
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

}  // namespace knowhere
}  // namespace milvus
