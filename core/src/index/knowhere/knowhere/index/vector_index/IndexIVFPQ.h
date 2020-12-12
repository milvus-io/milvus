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

class IVFPQ : public IVF {
 public:
    IVFPQ() : IVF() {
        index_type_ = IndexEnum::INDEX_FAISS_IVFPQ;
        stats = std::make_shared<milvus::knowhere::IVFStatistics>(index_type_);
    }

    explicit IVFPQ(std::shared_ptr<faiss::Index> index) : IVF(std::move(index)) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFPQ;
        stats = std::make_shared<milvus::knowhere::IVFStatistics>(index_type_);
    }

    void
    Train(const DatasetPtr&, const Config&) override;

    VecIndexPtr
    CopyCpuToGpu(const int64_t, const Config&) override;

    void
    UpdateIndexSize() override;

 protected:
    std::shared_ptr<faiss::IVFSearchParameters>
    GenParams(const Config& config) override;
};

using IVFPQPtr = std::shared_ptr<IVFPQ>;

}  // namespace knowhere
}  // namespace milvus
