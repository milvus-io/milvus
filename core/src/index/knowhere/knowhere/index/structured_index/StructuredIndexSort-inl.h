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
#include "knowhere/index/structured_index/StructuredIndexSort.h"

namespace milvus {
namespace knowhere {

template <typename T>
StructuredIndexSort<T>::StructuredIndexSort() : is_built_(false), data_(nullptr), n_(0) {
}

template <typename T>
StructuredIndexSort<T>::StructuredIndexSort(const size_t n, const T* values) : is_built_(false), n_(n) {
    Build(n, values);
}

template <typename T>
StructuredIndexSort<T>::~StructuredIndexSort() {
}

template <typename T>
void
StructuredIndexSort<T>::Build(const size_t n, const T* values) {
    data_.reserve(n);
    T* p = const_cast<T*>(values);
    for (auto i = 0; i < n; ++i) {
        //        data_[i].a_ = *p ++;
        //        data_[i].idx_ = i;
        data_.emplace_back(IndexStructure(*p++, i));
    }
    build();
}

template <typename T>
void
StructuredIndexSort<T>::build() {
    if (is_built_)
        return;
    if (data_.size() == 0 || n_ == 0) {
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

    auto index_data_size = n_ * sizeof(IndexStructure<T>);
    std::shared_ptr<uint8_t[]> index_data(new uint8_t[index_data_size]);
    memcpy(index_data.get(), data_.data(), index_data_size);

    std::shared_ptr<uint8_t[]> index_length(new uint8_t[sizeof(size_t)]);
    memcpy(index_length.get(), &n_, sizeof(size_t));

    BinarySet res_set;
    res_set.Append("index_data", index_data, index_data_size);
    res_set.Append("index_length", index_length, sizeof(size_t));
    return res_set;
}

template <typename T>
void
StructuredIndexSort<T>::Load(const milvus::knowhere::BinarySet& index_binary) {
    try {
        auto index_length = index_binary.GetByName("index_length");
        memcpy(&n_, index_length->data.get(), (size_t)index_length->size);

        auto index_data = index_binary.GetByName("index_data");
        data_.reserve(n_);
        memcpy(data_.data(), index_data->data.get(), (size_t)index_data->size);
        is_built_ = true;
    } catch (...) {
        KNOHWERE_ERROR_MSG("StructuredIndexSort Load failed!");
    }
}

// find the first element's offset which is no less than given value
template <typename T>
size_t
StructuredIndexSort<T>::lower_bound(const T& value) {
    size_t low = 0, high = n_, mid;
    while (low < high) {
        mid = low + ((high - low) >> 1);
        if (data_[mid].a_ < value) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    //    return data_[low].a == value ? data_[low].idx : -1;
    return low;
}

// find the first element's offset which is greater than given value
template <typename T>
size_t
StructuredIndexSort<T>::upper_bound(const T& value) {
    size_t low = 0, high = n_, mid;
    while (low < high) {
        mid = low + ((high - low) >> 1);
        if (data_[mid].a_ <= value) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    //    return data_[low].a == value ? data_[low].idx : -1;
    return low;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexSort<T>::In(const size_t n, const T* values) {
    if (!is_built_) {
        build();
    }
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(n_);
    for (auto i = 0; i < n; ++i) {
        auto lb = lower_bound(*(values + i));
        auto ub = upper_bound(*(values + i));
        for (auto j = lb; j < ub; ++j) {
            assert(data_[j].a_ == *(values + i));
            bitset->set(data_[j].idx_);
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
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(n_, 255);
    for (auto i = 0; i < n; ++i) {
        auto lb = lower_bound(*(values + i));
        auto ub = upper_bound(*(values + i));
        for (auto j = lb; j < ub; ++j) {
            assert(data_[j].a_ == *(values + i));
            bitset->clear(data_[j].idx_);
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
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(n_);
    size_t lb = 0, ub = n_;
    switch (op) {
        case OperatorType::LT:
            ub = lower_bound(value);
            break;
        case OperatorType::LE:
            ub = upper_bound(value);
            break;
        case OperatorType::GT:
            lb = upper_bound(value);
            break;
        case OperatorType::GE:
            lb = lower_bound(value);
            break;
        default:
            KNOWHERE_THROW_MSG("Invalid OperatorType:" + std::to_string((int)op) + "!");
    }
    for (auto i = lb; i < ub; ++i) {
        bitset->set(data_[i].idx_);
    }
    return bitset;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexSort<T>::Range(T lower_bound_value, bool lb_inclusive, T upper_bound_value, bool ub_inclusive) {
    if (!is_built_) {
        build();
    }
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(n_);
    if (lower_bound_value > upper_bound_value) {
        std::swap(lower_bound_value, upper_bound_value);
        std::swap(lb_inclusive, ub_inclusive);
    }
    size_t lb = 0, ub = n_;
    if (lb_inclusive) {
        lb = lower_bound(lower_bound_value);
    } else {
        lb = upper_bound(lower_bound_value);
    }
    if (ub_inclusive) {
        ub = upper_bound(upper_bound_value);
    } else {
        ub = lower_bound(upper_bound_value);
    }
    for (auto i = lb; i < ub; ++i) {
        bitset->set(data_[i].idx_);
    }
    return bitset;
}

}  // namespace knowhere
}  // namespace milvus
