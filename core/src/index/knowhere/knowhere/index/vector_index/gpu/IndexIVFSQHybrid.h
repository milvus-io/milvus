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

#include <faiss/gpu/GpuIndexFlat.h>
#include <faiss/index_io.h>

#include <memory>
#include <utility>

#include "knowhere/index/vector_index/gpu/IndexGPUIVFSQ.h"
#include "knowhere/index/vector_index/gpu/Quantizer.h"

namespace milvus {
namespace knowhere {

#ifdef MILVUS_GPU_VERSION

struct FaissIVFQuantizer : public Quantizer {
    faiss::gpu::GpuIndexFlat* quantizer = nullptr;
    int64_t gpu_id;

    ~FaissIVFQuantizer() override;
};
using FaissIVFQuantizerPtr = std::shared_ptr<FaissIVFQuantizer>;

class IVFSQHybrid : public GPUIVFSQ {
 public:
    explicit IVFSQHybrid(const int& device_id) : GPUIVFSQ(device_id) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFSQ8H;
        gpu_mode_ = 0;
    }

    explicit IVFSQHybrid(std::shared_ptr<faiss::Index> index) : GPUIVFSQ(-1) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFSQ8H;
        index_ = index;
        gpu_mode_ = 0;
    }

    explicit IVFSQHybrid(std::shared_ptr<faiss::Index> index, const int64_t device_id, ResPtr& resource)
        : GPUIVFSQ(index, device_id, resource) {
        index_type_ = IndexEnum::INDEX_FAISS_IVFSQ8H;
        gpu_mode_ = 2;
    }

 public:
    void
    Train(const DatasetPtr&, const Config&) override;

    VecIndexPtr
    CopyGpuToCpu(const Config&) override;

    VecIndexPtr
    CopyCpuToGpu(const int64_t, const Config&) override;

    std::pair<VecIndexPtr, QuantizerPtr>
    CopyCpuToGpuWithQuantizer(const int64_t, const Config&);

    VecIndexPtr
    LoadData(const knowhere::QuantizerPtr&, const Config&);

    QuantizerPtr
    LoadQuantizer(const Config& conf);

    void
    SetQuantizer(const QuantizerPtr& q);

    void
    UnsetQuantizer();

 protected:
    BinarySet
    SerializeImpl(const IndexType&) override;

    void
    LoadImpl(const BinarySet&, const IndexType&) override;

    void
    QueryImpl(int64_t, const float*, int64_t, float*, int64_t*, const Config&) override;

 protected:
    int64_t gpu_mode_ = 0;  // 0,1,2
    int64_t quantizer_gpu_id_ = -1;
};

using IVFSQHybridPtr = std::shared_ptr<IVFSQHybrid>;

#endif

}  // namespace knowhere
}  // namespace milvus
