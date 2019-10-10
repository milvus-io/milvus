// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <faiss/index_io.h>
#include <memory>

#include "IndexGPUIVFSQ.h"
#include "Quantizer.h"

namespace knowhere {

struct FaissIVFQuantizer : public Quantizer {
    faiss::gpu::GpuIndexFlat* quantizer = nullptr;
};
using FaissIVFQuantizerPtr = std::shared_ptr<FaissIVFQuantizer>;

class IVFSQHybrid : public GPUIVFSQ {
 public:
    explicit IVFSQHybrid(const int& device_id) : GPUIVFSQ(device_id) {
        gpu_mode = false;
    }

    explicit IVFSQHybrid(std::shared_ptr<faiss::Index> index) : GPUIVFSQ(-1) {
        index_ = index;
        gpu_mode = false;
    }

    explicit IVFSQHybrid(std::shared_ptr<faiss::Index> index, const int64_t& device_id, ResPtr& resource)
        : GPUIVFSQ(index, device_id, resource) {
        gpu_mode = true;
    }

 public:
    QuantizerPtr
    LoadQuantizer(const Config& conf);

    void
    SetQuantizer(const QuantizerPtr& q);

    void
    UnsetQuantizer();

    void
    LoadData(const knowhere::QuantizerPtr& q, const Config& conf);

    IndexModelPtr
    Train(const DatasetPtr& dataset, const Config& config) override;

    VectorIndexPtr
    CopyGpuToCpu(const Config& config) override;

    VectorIndexPtr
    CopyCpuToGpu(const int64_t& device_id, const Config& config) override;

 protected:
    void
    search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& cfg) override;

    void
    LoadImpl(const BinarySet& index_binary) override;

 protected:
    bool gpu_mode = false;
};

}  // namespace knowhere
