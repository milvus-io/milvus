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

#pragma once

#include <map>
#include <memory>
#include <string>
#include <boost/dynamic_bitset.hpp>
#include "index/Index.h"
#include "common/Types.h"
#include "exceptions/EasyAssert.h"

namespace milvus::index {

template <typename T>
class ScalarIndex : public IndexBase {
 public:
    void
    BuildWithRawData(size_t n, const void* values, const Config& config = {}) override;

    void
    BuildWithDataset(const DatasetPtr& dataset, const Config& config = {}) override {
        PanicInfo("scalar index don't support build index with dataset");
    };

 public:
    virtual void
    Build(size_t n, const T* values) = 0;

    virtual const TargetBitmap
    In(size_t n, const T* values) = 0;

    virtual const TargetBitmap
    NotIn(size_t n, const T* values) = 0;

    virtual const TargetBitmap
    Range(T value, OpType op) = 0;

    virtual const TargetBitmap
    Range(T lower_bound_value, bool lb_inclusive, T upper_bound_value, bool ub_inclusive) = 0;

    virtual T
    Reverse_Lookup(size_t offset) const = 0;

    virtual const TargetBitmap
    Query(const DatasetPtr& dataset);

    virtual int64_t
    Size() = 0;
};

template <typename T>
using ScalarIndexPtr = std::unique_ptr<ScalarIndex<T>>;

}  // namespace milvus::index

#include "index/ScalarIndex-inl.h"
