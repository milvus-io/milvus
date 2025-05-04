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

    const IndexType&
    Type() const {
        return index_type_;
    }

    virtual enum DataType
    JsonCastType() const {
        return DataType::NONE;
    }

    // TODO: how to get the cell byte size?
    virtual size_t
    CellByteSize() const {
        return cell_size_;
    }

    virtual void
    SetCellSize(size_t cell_size) {
        cell_size_ = cell_size;
    }

 protected:
    explicit IndexBase(IndexType index_type)
        : index_type_(std::move(index_type)) {
    }

    IndexType index_type_ = "";
    size_t cell_size_ = 0;
};

using IndexBasePtr = std::unique_ptr<IndexBase>;

template <typename T>
using CacheIndexPtr = std::shared_ptr<milvus::cachinglayer::CacheSlot<T>>;

using CacheIndexBasePtr = CacheIndexPtr<IndexBase>;

}  // namespace milvus::index
