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
#include <vector>
#include <boost/dynamic_bitset.hpp>
#include "knowhere/factory.h"
#include "index/VectorIndex.h"
#include "storage/MemFileManagerImpl.h"

namespace milvus::index {

class VectorMemIndex : public VectorIndex {
 public:
    explicit VectorMemIndex(const IndexType& index_type,
                            const MetricType& metric_type,
                            storage::FileManagerImplPtr file_manager = nullptr);

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& binary_set, const Config& config = {}) override;

    void
    Load(const Config& config = {}) override;

    void
    BuildWithDataset(const DatasetPtr& dataset,
                     const Config& config = {}) override;

    void
    Build(const Config& config = {}) override;

    void
    AddWithDataset(const DatasetPtr& dataset, const Config& config) override;

    int64_t
    Count() override {
        return index_.Count();
    }

    std::unique_ptr<SearchResult>
    Query(const DatasetPtr dataset,
          const SearchInfo& search_info,
          const BitsetView& bitset) override;

    const bool
    HasRawData() const override;

    const std::vector<uint8_t>
    GetVector(const DatasetPtr dataset) const override;

    BinarySet
    Upload(const Config& config = {}) override;

 protected:
    virtual void
    LoadWithoutAssemble(const BinarySet& binary_set, const Config& config);

 protected:
    Config config_;
    knowhere::Index<knowhere::IndexNode> index_;
    std::shared_ptr<storage::MemFileManagerImpl> file_manager_;
};

using VectorMemIndexPtr = std::unique_ptr<VectorMemIndex>;
}  // namespace milvus::index
