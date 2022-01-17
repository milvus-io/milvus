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

#include <map>
#include <memory>
#include <string>
#include "faiss/utils/BitsetView.h"
#include "knowhere/index/Index.h"

namespace milvus {
namespace knowhere {

enum OperatorType { LT = 0, LE = 1, GT = 3, GE = 4 };

static std::map<std::string, OperatorType> s_map_operator_type = {
    {"LT", OperatorType::LT}, {"LTE", OperatorType::LE}, {"GT", OperatorType::GT}, {"GTE", OperatorType::GE},
    {"lt", OperatorType::LT}, {"lte", OperatorType::LE}, {"gt", OperatorType::GT}, {"gte", OperatorType::GE},
};

template <typename T>
struct IndexStructure {
    IndexStructure() : a_(0), idx_(0) {
    }
    explicit IndexStructure(const T a) : a_(a), idx_(0) {
    }
    IndexStructure(const T a, const size_t idx) : a_(a), idx_(idx) {
    }
    bool
    operator<(const IndexStructure& b) const {
        return a_ < b.a_;
    }
    bool
    operator<=(const IndexStructure& b) const {
        return a_ <= b.a_;
    }
    bool
    operator>(const IndexStructure& b) const {
        return a_ > b.a_;
    }
    bool
    operator>=(const IndexStructure& b) const {
        return a_ >= b.a_;
    }
    bool
    operator==(const IndexStructure& b) const {
        return a_ == b.a_;
    }
    T a_;
    size_t idx_;
};

template <typename T>
class StructuredIndex : public Index {
 public:
    virtual void
    Build(const size_t n, const T* values) = 0;

    virtual const faiss::ConcurrentBitsetPtr
    In(const size_t n, const T* values) = 0;

    virtual const faiss::ConcurrentBitsetPtr
    NotIn(const size_t n, const T* values) = 0;

    virtual const faiss::ConcurrentBitsetPtr
    Range(const T value, const OperatorType op) = 0;

    virtual const faiss::ConcurrentBitsetPtr
    Range(const T lower_bound_value, bool lb_inclusive, const T upper_bound_value, bool ub_inclusive) = 0;
};

template <typename T>
using StructuredIndexPtr = std::shared_ptr<StructuredIndex<T>>;
}  // namespace knowhere
}  // namespace milvus
