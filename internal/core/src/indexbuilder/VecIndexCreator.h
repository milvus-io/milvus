// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "indexbuilder/IndexCreatorBase.h"
#include "index/VectorIndex.h"
#include "index/IndexInfo.h"
#include "storage/Types.h"

namespace milvus::indexbuilder {

// TODO: better to distinguish binary vec & float vec.
class VecIndexCreator : public IndexCreatorBase {
 public:
    explicit VecIndexCreator(DataType data_type,
                             Config& config,
                             storage::FileManagerImplPtr file_manager);

    void
    Build(const milvus::DatasetPtr& dataset) override;

    void
    Build() override;

    milvus::BinarySet
    Serialize() override;

    void
    Load(const milvus::BinarySet& binary_set) override;

    int64_t
    dim();

    std::unique_ptr<SearchResult>
    Query(const milvus::DatasetPtr& dataset,
          const SearchInfo& search_info,
          const BitsetView& bitset);

    BinarySet
    Upload() override;

 public:
    void
    CleanLocalData();

 private:
    milvus::index::IndexBasePtr index_ = nullptr;
    Config config_;
    DataType data_type_;
};

}  // namespace milvus::indexbuilder
