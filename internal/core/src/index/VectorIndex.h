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

#include "knowhere/index/VecIndex.h"
#include "index/Index.h"
#include "common/Types.h"
#include "common/BitsetView.h"
#include "common/QueryResult.h"
#include "common/QueryInfo.h"

namespace milvus::index {

class VectorIndex : public IndexBase {
 public:
    explicit VectorIndex(const IndexType& index_type, const IndexMode& index_mode, const MetricType& metric_type)
        : index_type_(index_type), index_mode_(index_mode), metric_type_(metric_type) {
    }

 public:
    void
    BuildWithRawData(size_t n, const void* values, const Config& config = {}) override {
        PanicInfo("vector index don't support build index with raw data");
    };

    virtual std::unique_ptr<SearchResult>
    Query(const DatasetPtr dataset, const SearchInfo& search_info, const BitsetView& bitset) = 0;

    IndexType
    GetIndexType() const {
        return index_type_;
    }

    MetricType
    GetMetricType() const {
        return metric_type_;
    }

    IndexMode
    GetIndexMode() const {
        return index_mode_;
    }

    int64_t
    GetDim() const {
        return dim_;
    }

    void
    SetDim(int64_t dim) {
        dim_ = dim;
    }

    virtual void
    CleanLocalData() {
    }

 private:
    IndexType index_type_;
    IndexMode index_mode_;
    MetricType metric_type_;
    int64_t dim_;
};

using VectorIndexPtr = std::unique_ptr<VectorIndex>;
}  // namespace milvus::index
