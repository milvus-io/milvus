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

#include "knowhere/index/vector_index/IndexIVF.h"

namespace milvus {
namespace knowhere {

class IVFHNSW : public IVF {
 public:
    IVFHNSW() : IVF() {
        index_type_ = IndexEnum::INDEX_FAISS_IVFHNSW;
    }

    explicit IVFHNSW(std::shared_ptr<faiss::Index> index) : IVF(std::move(index)) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFHNSW;
    }

    BinarySet
    Serialize(const Config&) override;

    void
    Load(const BinarySet&) override;

    void
    Train(const DatasetPtr&, const Config&) override;

    VecIndexPtr
    CopyCpuToGpu(const int64_t, const Config&) override;

    void
    UpdateIndexSize() override;

 protected:
    void
    QueryImpl(int64_t n,
              const float* data,
              int64_t k,
              float* distances,
              int64_t* labels,
              const Config& config,
              const faiss::BitsetView bitset) override;
};

using IVFHNSWPtr = std::shared_ptr<IVFHNSW>;

}  // namespace knowhere
}  // namespace milvus
