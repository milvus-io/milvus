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

#include <memory>
#include <boost/dynamic_bitset.hpp>
#include "cachinglayer/CacheSlot.h"
#include "common/FieldData.h"
#include "common/EasyAssert.h"
#include "common/File.h"
#include "common/JsonCastType.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "index/Meta.h"
#include "index/IndexStats.h"

namespace milvus::index {

class IndexBase {
 public:
    IndexBase() = default;
    virtual ~IndexBase() = default;

    virtual BinarySet
    Serialize(const Config& config) = 0;

    virtual void
    Load(const BinarySet& binary_set, const Config& config = {}) = 0;

    virtual void
    Load(milvus::tracer::TraceContext ctx, const Config& config = {}) = 0;

    virtual void
    BuildWithRawDataForUT(size_t n,
                          const void* values,
                          const Config& config = {}) = 0;

    virtual void
    BuildWithDataset(const DatasetPtr& dataset, const Config& config = {}) = 0;

    virtual void
    Build(const Config& config = {}) = 0;

    virtual int64_t
    Count() = 0;

    virtual IndexStatsPtr
    Upload(const Config& config = {}) = 0;

    virtual const bool
    HasRawData() const = 0;

    virtual bool
    IsMmapSupported() const = 0;

    // Check if this index is a nested index (for array element-level indexing)
    virtual bool
    IsNestedIndex() const {
        return false;
    }

    const IndexType&
    Type() const {
        return index_type_;
    }

    virtual JsonCastType
    GetCastType() const {
        return JsonCastType::UNKNOWN;
    }

    // TODO: how to get the cell byte size?
    virtual cachinglayer::ResourceUsage
    CellByteSize() const {
        return cell_size_;
    }

    virtual void
    SetCellSize(cachinglayer::ResourceUsage cell_size) {
        cell_size_ = cell_size;
    }

    // Returns the memory usage in bytes that scales with data size (O(n)).
    // Fixed overhead are minimal and thus not included.
    //
    // NOTE: This method returns a cached value computed by ComputeByteSize().
    // It is designed for SEALED SEGMENTS only, where the index is fully built
    // or loaded and no more data will be added. For GROWING SEGMENTS with
    // ongoing inserts, the cached value will NOT be updated automatically.
    // ComputeByteSize() should only be called after Build() or Load() completes
    // on sealed segments.
    int64_t
    ByteSize() const {
        return cached_byte_size_;
    }

    // Computes and caches the memory usage in bytes.
    // Subclasses should override this method to calculate their specific memory usage
    // and store the result in cached_byte_size_.
    // This method should be called at the end of Build() or Load() for sealed segments.
    virtual void
    ComputeByteSize() {
        cached_byte_size_ = 0;
    }

 protected:
    explicit IndexBase(IndexType index_type)
        : index_type_(std::move(index_type)) {
    }

    IndexType index_type_ = "";
    cachinglayer::ResourceUsage cell_size_ = {0, 0};
    mutable int64_t cached_byte_size_ = 0;

    std::unique_ptr<MmapFileRAII> mmap_file_raii_;
};

using IndexBasePtr = std::unique_ptr<IndexBase>;

template <typename T>
using CacheIndexPtr = std::shared_ptr<milvus::cachinglayer::CacheSlot<T>>;

using CacheIndexBasePtr = CacheIndexPtr<IndexBase>;

}  // namespace milvus::index
