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

#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>
#include "knowhere/common/Exception.h"
#include "knowhere/index/structured_index/StructuredIndex.h"

namespace milvus {
namespace knowhere {

template <typename T>
class StructuredIndexFlat : public StructuredIndex<T> {
 public:
    StructuredIndexFlat();
    StructuredIndexFlat(const size_t n, const T* values);
    ~StructuredIndexFlat();

    BinarySet
    Serialize(const Config& config = Config()) override;

    void
    Load(const BinarySet& index_binary) override;

    void
    Build(const size_t n, const T* values) override;

    const faiss::ConcurrentBitsetPtr
    In(const size_t n, const T* values) override;

    const faiss::ConcurrentBitsetPtr
    NotIn(const size_t n, const T* values) override;

    const faiss::ConcurrentBitsetPtr
    Range(const T value, const OperatorType op) override;

    const faiss::ConcurrentBitsetPtr
    Range(T lower_bound_value, bool lb_inclusive, T upper_bound_value, bool ub_inclusive) override;

    const std::vector<IndexStructure<T>>&
    GetData() {
        return data_;
    }

    int64_t
    Size() override {
        return (int64_t)data_.size() * sizeof(IndexStructure<T>);
    }

    bool
    IsBuilt() const {
        return is_built_;
    }

 private:
    bool is_built_;
    std::vector<IndexStructure<T>> data_;
};

template <typename T>
using StructuredIndexFlatPtr = std::shared_ptr<StructuredIndexFlat<T>>;
}  // namespace knowhere
}  // namespace milvus

#include "knowhere/index/structured_index/StructuredIndexFlat-inl.h"
