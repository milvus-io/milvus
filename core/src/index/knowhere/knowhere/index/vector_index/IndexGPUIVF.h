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
#include <utility>

#include "IndexIVF.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"

namespace knowhere {

class GPUIndex {
 public:
    explicit GPUIndex(const int& device_id) : gpu_id_(device_id) {
    }

    GPUIndex(const int& device_id, const ResPtr& resource) : gpu_id_(device_id), res_(resource) {
    }

    virtual VectorIndexPtr
    CopyGpuToCpu(const Config& config) = 0;

    virtual VectorIndexPtr
    CopyGpuToGpu(const int64_t& device_id, const Config& config) = 0;

    void
    SetGpuDevice(const int& gpu_id);

    const int64_t&
    GetGpuDevice();

 protected:
    int64_t gpu_id_;
    ResWPtr res_;
};

class GPUIVF : public IVF, public GPUIndex {
 public:
    explicit GPUIVF(const int& device_id) : IVF(), GPUIndex(device_id) {
    }

    explicit GPUIVF(std::shared_ptr<faiss::Index> index, const int64_t& device_id, ResPtr& resource)
        : IVF(std::move(index)), GPUIndex(device_id, resource) {
    }

    IndexModelPtr
    Train(const DatasetPtr& dataset, const Config& config) override;

    void
    Add(const DatasetPtr& dataset, const Config& config) override;

    void
    set_index_model(IndexModelPtr model) override;

    // DatasetPtr Search(const DatasetPtr &dataset, const Config &config) override;
    VectorIndexPtr
    CopyGpuToCpu(const Config& config) override;

    VectorIndexPtr
    CopyGpuToGpu(const int64_t& device_id, const Config& config) override;

    //    VectorIndexPtr
    //    Clone() final;

 protected:
    void
    search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& cfg) override;

    BinarySet
    SerializeImpl() override;

    void
    LoadImpl(const BinarySet& index_binary) override;
};

}  // namespace knowhere
