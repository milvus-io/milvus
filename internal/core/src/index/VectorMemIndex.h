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

#include "index/VectorIndex.h"

namespace milvus::index {

class VectorMemIndex : public VectorIndex {
 public:
    explicit VectorMemIndex(const IndexType& index_type, const MetricType& metric_type, const IndexMode& index_mode);

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& binary_set, const Config& config = {}) override;

    void
    BuildWithDataset(const DatasetPtr& dataset, const Config& config = {}) override;

    int64_t
    Count() override {
        return index_->Count();
    }

    std::unique_ptr<SearchResult>
    Query(const DatasetPtr dataset, const SearchInfo& search_info, const BitsetView& bitset) override;

 private:
    void
    store_raw_data(const knowhere::DatasetPtr& dataset);

    void
    parse_config(Config& config);

    void
    LoadRawData();

 private:
    Config config_;
    knowhere::VecIndexPtr index_ = nullptr;
    std::vector<uint8_t> raw_data_;
    std::once_flag raw_data_loaded_;
};

using VectorMemIndexPtr = std::unique_ptr<VectorMemIndex>;
}  // namespace milvus::index
