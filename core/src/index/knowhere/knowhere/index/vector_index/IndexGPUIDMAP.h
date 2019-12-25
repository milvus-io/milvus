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

#include "IndexGPUIVF.h"
#include "IndexIDMAP.h"
#include "IndexIVF.h"

#include <memory>
#include <utility>
#include <vector>

namespace knowhere {

class GPUIDMAP : public IDMAP, public GPUIndex {
 public:
    explicit GPUIDMAP(std::shared_ptr<faiss::Index> index, const int64_t& device_id, ResPtr& res)
        : IDMAP(std::move(index)), GPUIndex(device_id, res) {
    }

    VectorIndexPtr
    CopyGpuToCpu(const Config& config) override;

    float*
    GetRawVectors() override;

    int64_t*
    GetRawIds() override;

    //    VectorIndexPtr
    //    Clone() override;

    VectorIndexPtr
    CopyGpuToGpu(const int64_t& device_id, const Config& config) override;

    void
    GenGraph(const float* data, const int64_t& k, Graph& graph, const Config& config);

 protected:
    void
    search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& cfg) override;

    BinarySet
    SerializeImpl() override;

    void
    LoadImpl(const BinarySet& index_binary) override;
};

using GPUIDMAPPtr = std::shared_ptr<GPUIDMAP>;

}  // namespace knowhere
