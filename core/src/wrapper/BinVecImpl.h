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
#include <utility>

#include "VecImpl.h"

namespace milvus {
namespace engine {

class BinVecImpl : public VecIndexImpl {
 public:
    explicit BinVecImpl(std::shared_ptr<knowhere::VectorIndex> index, const IndexType& type)
        : VecIndexImpl(std::move(index), type) {
    }

    Status
    BuildAll(const int64_t& nb, const uint8_t* xb, const int64_t* ids, const Config& cfg, const int64_t& nt,
             const uint8_t* xt) override;
    Status
    Search(const int64_t& nq, const uint8_t* xq, float* dist, int64_t* ids, const Config& cfg) override;

    Status
    Add(const int64_t& nb, const uint8_t* xb, const int64_t* ids, const Config& cfg) override;

    VecIndexPtr
    CopyToGpu(const int64_t& device_id, const Config& cfg) override;

    VecIndexPtr
    CopyToCpu(const Config& cfg) override;
};

class BinBFIndex : public BinVecImpl {
 public:
    explicit BinBFIndex(std::shared_ptr<knowhere::VectorIndex> index)
        : BinVecImpl(std::move(index), IndexType::FAISS_BIN_IDMAP) {
    }

    ErrorCode
    Build(const Config& cfg);

    Status
    BuildAll(const int64_t& nb, const uint8_t* xb, const int64_t* ids, const Config& cfg, const int64_t& nt,
             const uint8_t* xt) override;

    const uint8_t*
    GetRawVectors();

    const int64_t*
    GetRawIds();
};

}  // namespace engine
}  // namespace milvus
