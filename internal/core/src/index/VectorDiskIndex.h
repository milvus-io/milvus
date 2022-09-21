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

#include "index/VectorIndex.h"
#include "storage/DiskFileManagerImpl.h"
#include "knowhere/index/vector_index/IndexDiskANN.h"
#include "knowhere/index/vector_index/IndexDiskANNConfig.h"

namespace milvus::index {

#ifdef BUILD_DISK_ANN

template <typename T>
class VectorDiskAnnIndex : public VectorIndex {
 public:
    explicit VectorDiskAnnIndex(const IndexType& index_type,
                                const MetricType& metric_type,
                                const IndexMode& index_mode,
                                storage::FileManagerImplPtr file_manager);
    BinarySet
    Serialize(const Config& config) override {
        auto remote_paths_to_size = file_manager_->GetRemotePathsToFileSize();
        BinarySet binary_set;
        for (auto& file : remote_paths_to_size) {
            binary_set.Append(file.first, nullptr, file.second);
        }

        return binary_set;
    }

    int64_t
    Count() override {
        return index_->Count();
    }

    void
    Load(const BinarySet& binary_set /* not used */, const Config& config = {}) override;

    void
    BuildWithDataset(const DatasetPtr& dataset, const Config& config = {}) override;

    std::unique_ptr<SearchResult>
    Query(const DatasetPtr dataset, const SearchInfo& search_info, const BitsetView& bitset) override;

    void
    CleanLocalData() override;

 private:
    knowhere::DiskANNBuildConfig
    parse_build_config(const Config& config);

    knowhere::DiskANNPrepareConfig
    parse_prepare_config(const Config& config);

    void
    parse_config(Config& config);

 private:
    std::unique_ptr<knowhere::IndexDiskANN<T>> index_;
    std::shared_ptr<storage::DiskFileManagerImpl> file_manager_;
};

template <typename T>
using VectorDiskAnnIndexPtr = std::unique_ptr<VectorDiskAnnIndex<T>>;
#endif

}  // namespace milvus::index
