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

#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"

namespace milvus {
namespace knowhere {

class GPUIVFPQ : public GPUIVF {
 public:
    explicit GPUIVFPQ(const int& device_id) : GPUIVF(device_id) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFPQ;
    }

    GPUIVFPQ(std::shared_ptr<faiss::Index> index, const int64_t device_id, ResPtr& res)
        : GPUIVF(std::move(index), device_id, res) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFPQ;
    }

    void
    Train(const DatasetPtr&, const Config&) override;

    VecIndexPtr
    CopyGpuToCpu(const Config&) override;

    virtual ~GPUIVFPQ() = default;

 protected:
    std::shared_ptr<faiss::IVFSearchParameters>
    GenParams(const Config& config) override;
};

using GPUIVFPQPtr = std::shared_ptr<GPUIVFPQ>;

}  // namespace knowhere
}  // namespace milvus
