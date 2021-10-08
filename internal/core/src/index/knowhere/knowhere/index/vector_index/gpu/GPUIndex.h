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

#include "knowhere/index/vector_index/VecIndex.h"
#include "knowhere/index/vector_index/helpers/FaissGpuResourceMgr.h"

namespace milvus {
namespace knowhere {

class GPUIndex {
 public:
    explicit GPUIndex(const int& device_id) : gpu_id_(device_id) {
    }

    GPUIndex(const int& device_id, const ResPtr& resource) : gpu_id_(device_id), res_(resource) {
    }

    virtual VecIndexPtr
    CopyGpuToCpu(const Config&) = 0;

    virtual VecIndexPtr
    CopyGpuToGpu(const int64_t, const Config&) = 0;

    void
    SetGpuDevice(const int& gpu_id) {
        gpu_id_ = gpu_id;
    }

    const int64_t
    GetGpuDevice() {
        return gpu_id_;
    }

 protected:
    int64_t gpu_id_;
    ResWPtr res_;
};

}  // namespace knowhere
}  // namespace milvus
