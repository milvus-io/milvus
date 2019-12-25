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

#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "FaissBaseIndex.h"
#include "VectorIndex.h"
#include "faiss/IndexIVF.h"

namespace knowhere {

using Graph = std::vector<std::vector<int64_t>>;

class IVF : public VectorIndex, public FaissBaseIndex {
 public:
    IVF() : FaissBaseIndex(nullptr) {
    }

    explicit IVF(std::shared_ptr<faiss::Index> index) : FaissBaseIndex(std::move(index)) {
    }

    //    VectorIndexPtr
    //    Clone() override;

    IndexModelPtr
    Train(const DatasetPtr& dataset, const Config& config) override;

    void
    set_index_model(IndexModelPtr model) override;

    void
    Add(const DatasetPtr& dataset, const Config& config) override;

    void
    AddWithoutIds(const DatasetPtr& dataset, const Config& config);

    DatasetPtr
    Search(const DatasetPtr& dataset, const Config& config) override;

    void
    GenGraph(const float* data, const int64_t& k, Graph& graph, const Config& config);

    BinarySet
    Serialize() override;

    void
    Load(const BinarySet& index_binary) override;

    int64_t
    Count() override;

    int64_t
    Dimension() override;

    void
    Seal() override;

    virtual VectorIndexPtr
    CopyCpuToGpu(const int64_t& device_id, const Config& config);

 protected:
    virtual std::shared_ptr<faiss::IVFSearchParameters>
    GenParams(const Config& config);

    //    virtual VectorIndexPtr
    //    Clone_impl(const std::shared_ptr<faiss::Index>& index);

    virtual void
    search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& cfg);

 protected:
    std::mutex mutex_;
};

using IVFIndexPtr = std::shared_ptr<IVF>;

class GPUIVF;
class IVFIndexModel : public IndexModel, public FaissBaseIndex {
    friend IVF;
    friend GPUIVF;

 public:
    explicit IVFIndexModel(std::shared_ptr<faiss::Index> index);

    IVFIndexModel() : FaissBaseIndex(nullptr) {
    }

    BinarySet
    Serialize() override;

    void
    Load(const BinarySet& binary) override;

 protected:
    void
    SealImpl() override;

 protected:
    std::mutex mutex_;
};

using IVFIndexModelPtr = std::shared_ptr<IVFIndexModel>;

}  // namespace knowhere
