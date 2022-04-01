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
#include <boost/dynamic_bitset.hpp>
#include "index/Index.h"

namespace milvus::scalar {

template <typename T>
class ScalarIndex : public IndexBase {
 public:
    virtual void
    Build(size_t n, const T* values) = 0;

    virtual const TargetBitmapPtr
    In(size_t n, const T* values) = 0;

    virtual const TargetBitmapPtr
    NotIn(size_t n, const T* values) = 0;

    virtual const TargetBitmapPtr
    Range(T value, OperatorType op) = 0;

    virtual const TargetBitmapPtr
    Range(T lower_bound_value, bool lb_inclusive, T upper_bound_value, bool ub_inclusive) = 0;

    const TargetBitmapPtr
    Query(const DatasetPtr& dataset) override;
};

template <typename T>
using ScalarIndexPtr = std::unique_ptr<ScalarIndex<T>>;

}  // namespace milvus::scalar

#include "index/ScalarIndex-inl.h"
