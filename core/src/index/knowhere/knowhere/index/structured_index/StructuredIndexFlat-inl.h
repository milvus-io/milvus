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
#include "knowhere/index/structured_index/StructuredIndexFlat.h"

namespace milvus {
namespace knowhere {

template <typename T>
StructuredIndexFlat<T>::StructuredIndexFlat() : is_built_(false), data_() {
}

template <typename T>
StructuredIndexFlat<T>::StructuredIndexFlat(const size_t n, const T* values) : is_built_(false) {
    Build(n, values);
}

template <typename T>
StructuredIndexFlat<T>::~StructuredIndexFlat() {
}

template <typename T>
BinarySet
StructuredIndexFlat<T>::Serialize(const milvus::knowhere::Config& config) {
    // TODO
    return BinarySet();
}

template <typename T>
void
StructuredIndexFlat<T>::Load(const milvus::knowhere::BinarySet& index_binary) {
}

template <typename T>
void
StructuredIndexFlat<T>::Build(const size_t n, const T* values) {
    data_.reserve(n);
    T* p = const_cast<T*>(values);
    for (size_t i = 0; i < n; ++i) {
        data_.emplace_back(IndexStructure(*p++, i));
    }
    is_built_ = true;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexFlat<T>::In(const size_t n, const T* values) {
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(data_.size());
    for (size_t i = 0; i < n; ++i) {
        for (const auto& index : data_) {
            if (index.a_ == *(values + i)) {
                bitset->set(index.idx_);
            }
        }
    }
    return bitset;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexFlat<T>::NotIn(const size_t n, const T* values) {
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(data_.size(), 0xff);
    for (size_t i = 0; i < n; ++i) {
        for (const auto& index : data_) {
            if (index.a_ == *(values + i)) {
                bitset->clear(index.idx_);
            }
        }
    }
    return bitset;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexFlat<T>::Range(const T value, const OperatorType op) {
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(data_.size());
    auto lb = data_.begin();
    auto ub = data_.end();
    for (; lb < ub; lb++) {
        switch (op) {
            case OperatorType::LT:
                if (*lb < IndexStructure<T>(value)) {
                    bitset->set(lb->idx_);
                }
                break;
            case OperatorType::LE:
                if (*lb <= IndexStructure<T>(value)) {
                    bitset->set(lb->idx_);
                }
                break;
            case OperatorType::GT:
                if (*lb > IndexStructure<T>(value)) {
                    bitset->set(lb->idx_);
                }
                break;
            case OperatorType::GE:
                if (*lb >= IndexStructure<T>(value)) {
                    bitset->set(lb->idx_);
                }
                break;
            default:
                KNOWHERE_THROW_MSG("Invalid OperatorType:" + std::to_string((int)op) + "!");
        }
    }
    return bitset;
}

template <typename T>
const faiss::ConcurrentBitsetPtr
StructuredIndexFlat<T>::Range(T lower_bound_value, bool lb_inclusive, T upper_bound_value, bool ub_inclusive) {
    faiss::ConcurrentBitsetPtr bitset = std::make_shared<faiss::ConcurrentBitset>(data_.size());
    if (lower_bound_value > upper_bound_value) {
        std::swap(lower_bound_value, upper_bound_value);
        std::swap(lb_inclusive, ub_inclusive);
    }
    auto lb = data_.begin();
    auto ub = data_.end();
    for (; lb < ub; ++lb) {
        if (lb_inclusive && ub_inclusive) {
            if (*lb >= IndexStructure<T>(lower_bound_value) && *lb <= IndexStructure<T>(upper_bound_value)) {
                bitset->set(lb->idx_);
            }
        } else if (lb_inclusive && !ub_inclusive) {
            if (*lb >= IndexStructure<T>(lower_bound_value) && *lb < IndexStructure<T>(upper_bound_value)) {
                bitset->set(lb->idx_);
            }
        } else if (!lb_inclusive && ub_inclusive) {
            if (*lb > IndexStructure<T>(lower_bound_value) && *lb <= IndexStructure<T>(upper_bound_value)) {
                bitset->set(lb->idx_);
            }
        } else {
            if (*lb > IndexStructure<T>(lower_bound_value) && *lb < IndexStructure<T>(upper_bound_value)) {
                bitset->set(lb->idx_);
            }
        }
    }
    return bitset;
}

}  // namespace knowhere
}  // namespace milvus
