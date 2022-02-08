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
#include <utility>

#include "knowhere/index/vector_index/FaissBaseIndex.h"
#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/helpers/DynamicResultSet.h"

namespace milvus {
namespace knowhere {

class IDMAP : public VecIndex, public FaissBaseIndex {
 public:
    IDMAP() : FaissBaseIndex(nullptr) {
        index_type_ = IndexEnum::INDEX_FAISS_IDMAP;
    }

    explicit IDMAP(std::shared_ptr<faiss::Index> index) : FaissBaseIndex(std::move(index)) {
        index_type_ = IndexEnum::INDEX_FAISS_IDMAP;
    }

    BinarySet
    Serialize(const Config&) override;

    void
    Load(const BinarySet&) override;

    void
    Train(const DatasetPtr&, const Config&) override;

    void
    AddWithoutIds(const DatasetPtr&, const Config&) override;

    DatasetPtr
    Query(const DatasetPtr&, const Config&, const faiss::BitsetView) override;

    DynamicResultSegment
    QueryByDistance(const DatasetPtr& dataset, const Config& config, const faiss::BitsetView bitset);

    int64_t
    Count() override;

    int64_t
    Dim() override;

    int64_t
    IndexSize() override {
        return Count() * Dim() * sizeof(FloatType);
    }

    VecIndexPtr
    CopyCpuToGpu(const int64_t, const Config&);

    virtual const float*
    GetRawVectors();

 protected:
    virtual void
    QueryImpl(int64_t, const float*, int64_t, float*, int64_t*, const Config&, const faiss::BitsetView);
};

using IDMAPPtr = std::shared_ptr<IDMAP>;

}  // namespace knowhere
}  // namespace milvus
