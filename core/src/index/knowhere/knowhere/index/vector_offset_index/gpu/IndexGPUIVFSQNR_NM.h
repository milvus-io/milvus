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

#include "knowhere/index/vector_offset_index/gpu/IndexGPUIVF_NM.h"

namespace milvus {
namespace knowhere {

class GPUIVFSQNR_NM : public GPUIVF_NM {
 public:
    explicit GPUIVFSQNR_NM(const int& device_id) : GPUIVF_NM(device_id) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFSQ8;
    }

    explicit GPUIVFSQNR_NM(std::shared_ptr<faiss::Index> index, const int64_t device_id, ResPtr& res)
        : GPUIVF_NM(std::move(index), device_id, res) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFSQ8;
    }

    void
    Train(const DatasetPtr&, const Config&) override;

    VecIndexPtr
    CopyGpuToCpu(const Config&) override;
};

using GPUIVFSQNRNMPtr = std::shared_ptr<GPUIVFSQNR_NM>;

}  // namespace knowhere
}  // namespace milvus
