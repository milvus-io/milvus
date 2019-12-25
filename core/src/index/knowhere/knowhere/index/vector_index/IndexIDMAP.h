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

#include "IndexIVF.h"

#include <memory>
#include <utility>

namespace knowhere {

class IDMAP : public VectorIndex, public FaissBaseIndex {
 public:
    IDMAP() : FaissBaseIndex(nullptr) {
    }

    explicit IDMAP(std::shared_ptr<faiss::Index> index) : FaissBaseIndex(std::move(index)) {
    }

    BinarySet
    Serialize() override;

    void
    Load(const BinarySet& index_binary) override;

    void
    Train(const Config& config);

    DatasetPtr
    Search(const DatasetPtr& dataset, const Config& config) override;

    int64_t
    Count() override;

    //    VectorIndexPtr
    //    Clone() override;

    int64_t
    Dimension() override;

    void
    Add(const DatasetPtr& dataset, const Config& config) override;

    void
    AddWithoutId(const DatasetPtr& dataset, const Config& config);

    VectorIndexPtr
    CopyCpuToGpu(const int64_t& device_id, const Config& config);
    void
    Seal() override;

    virtual const float*
    GetRawVectors();

    virtual const int64_t*
    GetRawIds();

 protected:
    virtual void
    search_impl(int64_t n, const float* data, int64_t k, float* distances, int64_t* labels, const Config& cfg);

 protected:
    std::mutex mutex_;
};

using IDMAPPtr = std::shared_ptr<IDMAP>;

}  // namespace knowhere
