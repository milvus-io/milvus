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

#include <faiss/IndexIVF.h>

#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/FaissBaseBinaryIndex.h"
#include "knowhere/index/vector_index/VecIndex.h"

namespace milvus {
namespace knowhere {

class BinaryIVF : public VecIndex, public FaissBaseBinaryIndex {
 public:
    BinaryIVF() : FaissBaseBinaryIndex(nullptr) {
        index_type_ = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    }

    explicit BinaryIVF(std::shared_ptr<faiss::IndexBinary> index) : FaissBaseBinaryIndex(std::move(index)) {
        index_type_ = IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    }

    BinarySet
    Serialize(const Config& config = Config()) override;

    void
    BuildAll(const DatasetPtr& dataset_ptr, const Config& config) override {
        Train(dataset_ptr, config);
    }

    void
    Load(const BinarySet& index_binary) override;

    void
    Train(const DatasetPtr& dataset_ptr, const Config& config) override;

    void
    Add(const DatasetPtr& dataset_ptr, const Config& config) override {
        KNOWHERE_THROW_MSG("not support yet");
    }

    void
    AddWithoutIds(const DatasetPtr&, const Config&) override {
        KNOWHERE_THROW_MSG("AddWithoutIds is not supported");
    }

    DatasetPtr
    Query(const DatasetPtr& dataset_ptr, const Config& config) override;

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

#if 0
    DatasetPtr
    GetVectorById(const DatasetPtr& dataset_ptr, const Config& config);
#endif

 protected:
    virtual std::shared_ptr<faiss::IVFSearchParameters>
    GenParams(const Config& config);

    virtual void
    QueryImpl(int64_t n, const uint8_t* data, int64_t k, float* distances, int64_t* labels, const Config& config);

 protected:
    std::mutex mutex_;
};

using BinaryIVFIndexPtr = std::shared_ptr<BinaryIVF>;

}  // namespace knowhere
}  // namespace milvus
