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

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "knowhere/index/vector_index/FaissBaseBinaryIndex.h"
#include "knowhere/index/vector_index/VecIndex.h"

namespace milvus {
namespace knowhere {

class BinaryIDMAP : public VecIndex, public FaissBaseBinaryIndex {
 public:
    BinaryIDMAP() : FaissBaseBinaryIndex(nullptr) {
        index_type_ = IndexEnum::INDEX_FAISS_BIN_IDMAP;
    }

    explicit BinaryIDMAP(std::shared_ptr<faiss::IndexBinary> index) : FaissBaseBinaryIndex(std::move(index)) {
        index_type_ = IndexEnum::INDEX_FAISS_BIN_IDMAP;
    }

    BinarySet
    Serialize(const Config& config = Config()) override;

    void
    Load(const BinarySet& index_binary) override;

    void
    Train(const DatasetPtr&, const Config&) override;

    void
    Add(const DatasetPtr&, const Config&) override;

    void
    AddWithoutIds(const DatasetPtr&, const Config&) override;

    DatasetPtr
    Query(const DatasetPtr&, const Config&) override;

#if 0
    DatasetPtr
    QueryById(const DatasetPtr& dataset_ptr, const Config& config) override;
#endif

    int64_t
    Count() override {
        return index_->ntotal;
    }

    int64_t
    Dim() override {
        return index_->d;
    }

    int64_t
    IndexSize() override {
        return Count() * Dim() / 8;
    }

#if 0
    DatasetPtr
    GetVectorById(const DatasetPtr& dataset_ptr, const Config& config) override;
#endif

    virtual const uint8_t*
    GetRawVectors();

    virtual const int64_t*
    GetRawIds();

 protected:
    virtual void
    QueryImpl(int64_t n, const uint8_t* data, int64_t k, float* distances, int64_t* labels, const Config& config);

 protected:
    std::mutex mutex_;
};

using BinaryIDMAPPtr = std::shared_ptr<BinaryIDMAP>;

}  // namespace knowhere
}  // namespace milvus
