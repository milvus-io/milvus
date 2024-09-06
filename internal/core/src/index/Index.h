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
#include "common/FieldData.h"
#include "common/EasyAssert.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "index/Meta.h"

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
    BuildWithRawData(size_t n,
                     const void* values,
                     const Config& config = {}) = 0;

    virtual void
    BuildWithDataset(const DatasetPtr& dataset, const Config& config = {}) = 0;

    virtual void
    Build(const Config& config = {}) = 0;

    virtual int64_t
    Count() = 0;

    virtual BinarySet
    Upload(const Config& config = {}) = 0;

    virtual const bool
    HasRawData() const = 0;

    bool
    IsMmapSupported() const {
        return index_type_ == knowhere::IndexEnum::INDEX_HNSW ||
               index_type_ == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT ||
               index_type_ == knowhere::IndexEnum::INDEX_FAISS_IVFFLAT_CC ||
               index_type_ == knowhere::IndexEnum::INDEX_FAISS_IVFPQ ||
               index_type_ == knowhere::IndexEnum::INDEX_FAISS_IVFSQ8 ||
               index_type_ == knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT ||
               index_type_ == knowhere::IndexEnum::INDEX_FAISS_IDMAP ||
               index_type_ == knowhere::IndexEnum::INDEX_FAISS_BIN_IDMAP ||
               index_type_ ==
                   knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX ||
               index_type_ == knowhere::IndexEnum::INDEX_SPARSE_WAND ||
               // support mmap for bitmap/hybrid index
               index_type_ == milvus::index::BITMAP_INDEX_TYPE ||
               index_type_ == milvus::index::HYBRID_INDEX_TYPE;
    }

    const IndexType&
    Type() const {
        return index_type_;
    }

 protected:
    explicit IndexBase(IndexType index_type)
        : index_type_(std::move(index_type)) {
    }

    IndexType index_type_ = "";
};

using IndexBasePtr = std::unique_ptr<IndexBase>;
}  // namespace milvus::index
