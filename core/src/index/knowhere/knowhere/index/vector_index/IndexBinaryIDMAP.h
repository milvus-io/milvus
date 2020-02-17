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
#include <mutex>
#include <utility>
#include <vector>

#include "FaissBaseBinaryIndex.h"
#include "VectorIndex.h"

namespace knowhere {

class BinaryIDMAP : public VectorIndex, public FaissBaseBinaryIndex {
 public:
    BinaryIDMAP() : FaissBaseBinaryIndex(nullptr) {
    }

    explicit BinaryIDMAP(std::shared_ptr<faiss::IndexBinary> index) : FaissBaseBinaryIndex(std::move(index)) {
    }

    BinarySet
    Serialize() override;

    void
    Load(const BinarySet& index_binary) override;

    DatasetPtr
    Search(const DatasetPtr& dataset, const Config& config) override;

    void
    Add(const DatasetPtr& dataset, const Config& config) override;

    void
    Train(const Config& config);

    int64_t
    Count() override;

    int64_t
    Dimension() override;

    void
    Seal() override;

    const uint8_t*
    GetRawVectors();

    const int64_t*
    GetRawIds();

 protected:
    virtual void
    search_impl(int64_t n, const uint8_t* data, int64_t k, float* distances, int64_t* labels, const Config& cfg);

 protected:
    std::mutex mutex_;
};

using BinaryIDMAPPtr = std::shared_ptr<BinaryIDMAP>;

}  // namespace knowhere
