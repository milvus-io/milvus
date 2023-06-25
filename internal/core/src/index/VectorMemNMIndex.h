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

#include "index/Utils.h"
#include "index/VectorMemIndex.h"

namespace milvus::index {

class VectorMemNMIndex : public VectorMemIndex {
 public:
    explicit VectorMemNMIndex(
        const IndexType& index_type,
        const MetricType& metric_type,
        storage::FileManagerImplPtr file_manager = nullptr)
        : VectorMemIndex(index_type, metric_type, file_manager) {
        AssertInfo(is_in_nm_list(index_type), "not valid nm index type");
    }

    BinarySet
    Serialize(const Config& config) override;

    void
    BuildWithDataset(const DatasetPtr& dataset,
                     const Config& config = {}) override;

    void
    AddWithDataset(const DatasetPtr& dataset, const Config& config) override;

    void
    Load(const BinarySet& binary_set, const Config& config = {}) override;

    std::unique_ptr<SearchResult>
    Query(const DatasetPtr dataset,
          const SearchInfo& search_info,
          const BitsetView& bitset) override;

    void
    LoadWithoutAssemble(const BinarySet& binary_set,
                        const Config& config) override;

 private:
    void
    store_raw_data(const DatasetPtr& dataset);

    void
    LoadRawData();

 private:
    std::vector<uint8_t> raw_data_;
    std::once_flag raw_data_loaded_;
};

using VectorMemNMIndexPtr = std::unique_ptr<VectorMemNMIndex>;
}  // namespace milvus::index
