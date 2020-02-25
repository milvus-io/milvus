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

#include "VecIndex.h"
#include "knowhere/index/vector_index/VectorIndex.h"

namespace milvus {
namespace engine {

class VecIndexImpl : public VecIndex {
 public:
    explicit VecIndexImpl(std::shared_ptr<knowhere::VectorIndex> index, const IndexType& type)
        : index_(std::move(index)), type(type) {
    }

    Status
    BuildAll(const int64_t& nb, const float* xb, const int64_t* ids, const Config& cfg, const int64_t& nt,
             const float* xt) override;

    VecIndexPtr
    CopyToGpu(const int64_t& device_id, const Config& cfg) override;

    VecIndexPtr
    CopyToCpu(const Config& cfg) override;

    IndexType
    GetType() const override;

    int64_t
    Dimension() override;

    int64_t
    Count() override;

    Status
    Add(const int64_t& nb, const float* xb, const int64_t* ids, const Config& cfg) override;

    knowhere::BinarySet
    Serialize() override;

    Status
    Load(const knowhere::BinarySet& index_binary) override;

    //    VecIndexPtr
    //    Clone() override;

    int64_t
    GetDeviceId() override;

    Status
    Search(const int64_t& nq, const float* xq, float* dist, int64_t* ids, const Config& cfg) override;

    Status
    GetVectorById(const int64_t n, const int64_t* xid, float* x, const Config& cfg) override;

    Status
    SearchById(const int64_t& nq, const int64_t* xq, float* dist, int64_t* ids, const Config& cfg) override;

    Status
    SetBlacklist(faiss::ConcurrentBitsetPtr list) override;

    Status
    GetBlacklist(faiss::ConcurrentBitsetPtr& list) override;

 protected:
    int64_t dim = 0;

    IndexType type = IndexType::INVALID;

    std::shared_ptr<knowhere::VectorIndex> index_ = nullptr;
};

class BFIndex : public VecIndexImpl {
 public:
    explicit BFIndex(std::shared_ptr<knowhere::VectorIndex> index)
        : VecIndexImpl(std::move(index), IndexType::FAISS_IDMAP) {
    }

    ErrorCode
    Build(const Config& cfg);

    const float*
    GetRawVectors();

    Status
    BuildAll(const int64_t& nb, const float* xb, const int64_t* ids, const Config& cfg, const int64_t& nt,
             const float* xt) override;

    const int64_t*
    GetRawIds();

    Status
    AddWithoutIds(const int64_t& nb, const float* xb, const Config& cfg);
};

class ToIndexData : public cache::DataObj {
 public:
    explicit ToIndexData(int64_t size) : size_(size) {
    }

    int64_t
    Size() override {
        return size_;
    }

 private:
    int64_t size_ = 0;
};

}  // namespace engine
}  // namespace milvus
