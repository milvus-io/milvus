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

#include <boost/dynamic_bitset.hpp>
#include <map>
#include <memory>
#include <string>

#include "common/Types.h"
#include "common/EasyAssert.h"
#include "index/Index.h"
#include "fmt/format.h"

namespace milvus::index {

enum class ScalarIndexType {
    NONE = 0,
    BITMAP,
    STLSORT,
    MARISA,
    INVERTED,
    HYBRID,
};

inline std::string
ToString(ScalarIndexType type) {
    switch (type) {
        case ScalarIndexType::NONE:
            return "NONE";
        case ScalarIndexType::BITMAP:
            return "BITMAP";
        case ScalarIndexType::STLSORT:
            return "STLSORT";
        case ScalarIndexType::MARISA:
            return "MARISA";
        case ScalarIndexType::INVERTED:
            return "INVERTED";
        case ScalarIndexType::HYBRID:
            return "HYBRID";
        default:
            return "UNKNOWN";
    }
}

template <typename T>
class ScalarIndex : public IndexBase {
 public:
    ScalarIndex(const std::string& index_type) : IndexBase(index_type) {
    }

    void
    BuildWithRawData(size_t n,
                     const void* values,
                     const Config& config = {}) override;

    void
    BuildWithDataset(const DatasetPtr& dataset,
                     const Config& config = {}) override {
        PanicInfo(Unsupported,
                  "scalar index don't support build index with dataset");
    };

 public:
    virtual ScalarIndexType
    GetIndexType() const = 0;

    virtual void
    Build(size_t n, const T* values) = 0;

    virtual const TargetBitmap
    In(size_t n, const T* values) = 0;

    virtual const TargetBitmap
    IsNull() = 0;

    virtual const TargetBitmap
    IsNotNull() = 0;

    virtual const TargetBitmap
    InApplyFilter(size_t n,
                  const T* values,
                  const std::function<bool(size_t /* offset */)>& filter) {
        PanicInfo(ErrorCode::Unsupported, "InApplyFilter is not implemented");
    }

    virtual void
    InApplyCallback(size_t n,
                    const T* values,
                    const std::function<void(size_t /* offset */)>& callback) {
        PanicInfo(ErrorCode::Unsupported, "InApplyCallback is not implemented");
    }

    virtual const TargetBitmap
    NotIn(size_t n, const T* values) = 0;

    virtual const TargetBitmap
    Range(T value, OpType op) = 0;

    virtual const TargetBitmap
    Range(T lower_bound_value,
          bool lb_inclusive,
          T upper_bound_value,
          bool ub_inclusive) = 0;

    virtual T
    Reverse_Lookup(size_t offset) const = 0;

    virtual const TargetBitmap
    Query(const DatasetPtr& dataset);

    virtual bool
    SupportPatternMatch() const {
        return false;
    }

    virtual const TargetBitmap
    PatternMatch(const std::string& pattern) {
        PanicInfo(Unsupported, "pattern match is not supported");
    }

    virtual int64_t
    Size() = 0;

    virtual bool
    SupportRegexQuery() const {
        return false;
    }

    virtual const TargetBitmap
    RegexQuery(const std::string& pattern) {
        PanicInfo(Unsupported, "regex query is not supported");
    }

    virtual void
    BuildWithFieldData(const std::vector<FieldDataPtr>& field_datas) {
        PanicInfo(Unsupported, "BuildwithFieldData is not supported");
    }

    virtual void
    LoadWithoutAssemble(const BinarySet& binary_set, const Config& config) {
        PanicInfo(Unsupported, "LoadWithoutAssemble is not supported");
    }
};

template <typename T>
using ScalarIndexPtr = std::unique_ptr<ScalarIndex<T>>;

}  // namespace milvus::index
