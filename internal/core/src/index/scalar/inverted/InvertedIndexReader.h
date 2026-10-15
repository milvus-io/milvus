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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "common/Types.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/INullReader.h"
#include "index/contracts/query/IPatternMatchReader.h"
#include "index/contracts/query/IScalarPredicateReader.h"
#include "tantivy-wrapper.h"

namespace milvus::storage {
class LocalDirectory;
}

namespace milvus::index {

namespace inverted_params {

inline bool
IsSupportedType(DataType type) {
    return type == DataType::BOOL || type == DataType::INT8 ||
           type == DataType::INT16 || type == DataType::INT32 ||
           type == DataType::INT64 || type == DataType::TIMESTAMPTZ ||
           type == DataType::FLOAT || type == DataType::DOUBLE ||
           IsStringDataType(type);
}

}  // namespace inverted_params

template <typename T>
class InvertedIndexReader final
    : public IIndexReaderBase,
      public IScalarPredicateReader<T>,
      public PatternMatchReaderAdapter<InvertedIndexReader<T>, T, true>,
      public INullReader {
 public:
    InvertedIndexReader(
        std::shared_ptr<storage::LocalDirectory> directory,
        std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine,
        std::shared_ptr<const std::vector<size_t>> null_offsets,
        DataType value_type,
        bool nested,
        bool mmap,
        size_t engine_bytes,
        size_t engine_path_bytes);

    ~InvertedIndexReader() override;

    ReaderCaps
    Caps() const override;

    Domain
    CoordDomain() const override;

    int64_t
    Count() const override;

    DataType
    ValueType() const override;

    int64_t
    MemoryUsage() const override;

    cachinglayer::ResourceUsage
    CellByteSize() const override;

    TargetBitmap
    In(size_t n, const T* values) const override;

    TargetBitmap
    NotIn(size_t n, const T* values) const override;

    TargetBitmap
    Range(const T& value, CompareOp op) const override;

    TargetBitmap
    Range(const T& lo, bool lo_inc, const T& hi, bool hi_inc) const override;

    TargetBitmap
    IsNull() const override;

    TargetBitmap
    IsNotNull() const override;

 private:
    template <typename Derived, typename U, bool DelegateShouldUseForOp>
    friend class PatternMatchReaderAdapter;

    bool
    ShouldUseForOpImpl(PatternOp op, std::string_view pattern) const;

    TargetBitmap
    PatternMatchImpl(std::string_view pattern, PatternOp op) const;

    TargetBitmap
    PatternQuery(std::string_view pattern) const;

    void
    ApplyValidityMask(TargetBitmap& bitset) const;

    // The engine is destroyed before the directory owner, so mapped files stay
    // alive through the Tantivy reader's entire lifetime.
    std::shared_ptr<storage::LocalDirectory> directory_;
    std::shared_ptr<milvus::tantivy::TantivyIndexWrapper> engine_;
    // Validity materialized once at open instead of replaying the null-offset
    // vector on every call. A sealed index has an immutable null set, and
    // IsNull/IsNotNull/NotIn are on the query hot path (profiled at 13.2% of
    // query-cluster CPU on an inverted-index workload), so they now clone or
    // AND a ready bitmap. The offset vector itself is not retained: it would
    // duplicate the same information at 8 bytes per null row.
    //
    // `all_valid_` leaves valid_bitmap_ empty rather than allocating rows/8
    // bytes of all-ones for a non-nullable field or a field with no nulls. A
    // nested index is also all-valid in its own (element) domain: a null row
    // emits no element, so every indexed element is valid, and the persisted
    // offsets are row offsets that only segment-level validity can apply.
    bool all_valid_{false};
    TargetBitmap valid_bitmap_;
    DataType value_type_{DataType::NONE};
    bool nested_{false};
    bool mmap_{false};
    // Exact staged file bytes on mmap, or the file payload bytes copied into
    // Tantivy's RamDirectory. Tantivy exposes no measurement for its reader,
    // hash-map, Arc, or allocator overhead; that narrow engine accounting gap
    // is not replaced with an admission estimate here.
    size_t engine_bytes_{0};
    // Heap bytes for the path string copied into TantivyIndexWrapper. The
    // wrapper exposes no accessor, so its source directory measures the copy
    // before heap-mode staging is released.
    size_t engine_path_bytes_{0};
    uint32_t count_{0};
};

}  // namespace milvus::index
