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

#include "knowhere/index/vector_index/VectorIndex.h"
#include "wrapper/VecImpl.h"
#include "wrapper/VecIndex.h"

namespace milvus {
namespace engine {

class IVFMixIndex : public VecIndexImpl {
 public:
    explicit IVFMixIndex(std::shared_ptr<knowhere::VectorIndex> index, const IndexType& type)
        : VecIndexImpl(std::move(index), type) {
    }

    Status
    BuildAll(const int64_t& nb, const float* xb, const int64_t* ids, const Config& cfg, const int64_t& nt,
             const float* xt) override;

    Status
    Load(const knowhere::BinarySet& index_binary) override;
};

class IVFHybridIndex : public IVFMixIndex {
 public:
    explicit IVFHybridIndex(std::shared_ptr<knowhere::VectorIndex> index, const IndexType& type)
        : IVFMixIndex(std::move(index), type) {
    }

    knowhere::QuantizerPtr
    LoadQuantizer(const Config& conf) override;

    Status
    SetQuantizer(const knowhere::QuantizerPtr& q) override;

    Status
    UnsetQuantizer() override;

    std::pair<VecIndexPtr, knowhere::QuantizerPtr>
    CopyToGpuWithQuantizer(const int64_t& device_id, const Config& cfg) override;

    VecIndexPtr
    LoadData(const knowhere::QuantizerPtr& q, const Config& conf) override;
};

}  // namespace engine
}  // namespace milvus
