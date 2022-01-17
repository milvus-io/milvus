// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <NGT/lib/NGT/Command.h>
#include <NGT/lib/NGT/Common.h>
#include <NGT/lib/NGT/Index.h>

#include <knowhere/common/Exception.h>
#include <knowhere/index/IndexType.h>
#include <knowhere/index/vector_index/VecIndex.h>
#include <memory>

namespace milvus {
namespace knowhere {

class IndexNGT : public VecIndex {
 public:
    IndexNGT() {
        index_type_ = IndexEnum::INVALID;
    }

    BinarySet
    Serialize(const Config& config) override;

    void
    Load(const BinarySet& index_binary) override;

    void
    BuildAll(const DatasetPtr& dataset_ptr, const Config& config) override;

    void
    Train(const DatasetPtr& dataset_ptr, const Config& config) override {
        KNOWHERE_THROW_MSG("NGT not support add item dynamically, please invoke BuildAll interface.");
    }

    void
    AddWithoutIds(const DatasetPtr& dataset_ptr, const Config& config) override {
        KNOWHERE_THROW_MSG("Incremental index is not supported");
    }

    DatasetPtr
    Query(const DatasetPtr& dataset_ptr, const Config& config, const faiss::BitsetView bitset) override;

    int64_t
    Count() override;

    int64_t
    Dim() override;

    void
    UpdateIndexSize() override;

 protected:
    std::shared_ptr<NGT::Index> index_ = nullptr;
};

}  // namespace knowhere
}  // namespace milvus
