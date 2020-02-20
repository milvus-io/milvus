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
#include <string>
#include <utility>

#include "cache/DataObj.h"
#include "knowhere/common/BinarySet.h"
#include "knowhere/common/Config.h"
#include "knowhere/index/vector_index/Quantizer.h"
#include "utils/Log.h"
#include "utils/Status.h"

namespace milvus {
namespace engine {

using Config = knowhere::Config;

// TODO(linxj): replace with string, Do refactor serialization
enum class IndexType {
    INVALID = 0,
    FAISS_IDMAP = 1,
    FAISS_IVFFLAT_CPU,
    FAISS_IVFFLAT_GPU,
    FAISS_IVFFLAT_MIX,  // build on gpu and search on cpu
    FAISS_IVFPQ_CPU,
    FAISS_IVFPQ_GPU,
    SPTAG_KDT_RNT_CPU,
    FAISS_IVFSQ8_MIX,
    FAISS_IVFSQ8_CPU,
    FAISS_IVFSQ8_GPU,
    FAISS_IVFSQ8_HYBRID,  // only support build on gpu.
    NSG_MIX,
    FAISS_IVFPQ_MIX,
    SPTAG_BKT_RNT_CPU,
    HNSW,
    FAISS_BIN_IDMAP = 100,
    FAISS_BIN_IVFLAT_CPU = 101,
};

class VecIndex;

using VecIndexPtr = std::shared_ptr<VecIndex>;

class VecIndex : public cache::DataObj {
 public:
    virtual Status
    BuildAll(const int64_t& nb, const float* xb, const int64_t* ids, const Config& cfg, const int64_t& nt = 0,
             const float* xt = nullptr) = 0;

    virtual Status
    BuildAll(const int64_t& nb, const uint8_t* xb, const int64_t* ids, const Config& cfg, const int64_t& nt = 0,
             const uint8_t* xt = nullptr) {
        ENGINE_LOG_ERROR << "BuildAll with uint8_t not support";
        return Status::OK();
    }

    virtual Status
    Add(const int64_t& nb, const float* xb, const int64_t* ids, const Config& cfg = Config()) = 0;

    virtual Status
    Add(const int64_t& nb, const uint8_t* xb, const int64_t* ids, const Config& cfg = Config()) {
        ENGINE_LOG_ERROR << "Add with uint8_t not support";
        return Status::OK();
    }

    virtual Status
    Search(const int64_t& nq, const float* xq, float* dist, int64_t* ids, const Config& cfg = Config()) = 0;

    virtual Status
    Search(const int64_t& nq, const uint8_t* xq, float* dist, int64_t* ids, const Config& cfg = Config()) {
        ENGINE_LOG_ERROR << "Search with uint8_t not support";
        return Status::OK();
    }

    virtual VecIndexPtr
    CopyToGpu(const int64_t& device_id, const Config& cfg = Config()) = 0;

    virtual VecIndexPtr
    CopyToCpu(const Config& cfg = Config()) = 0;

    // TODO(linxj): Deprecated
    //    virtual VecIndexPtr
    //    Clone() = 0;

    virtual int64_t
    GetDeviceId() = 0;

    virtual IndexType
    GetType() const = 0;

    virtual int64_t
    Dimension() = 0;

    virtual int64_t
    Count() = 0;

    int64_t
    Size() override;

    void
    set_size(int64_t size);

    virtual knowhere::BinarySet
    Serialize() = 0;

    virtual Status
    Load(const knowhere::BinarySet& index_binary) = 0;

    // TODO(linxj): refactor later
    ////////////////
    virtual knowhere::QuantizerPtr
    LoadQuantizer(const Config& conf) {
        ENGINE_LOG_ERROR << "LoadQuantizer virtual funciton called.";
        return nullptr;
    }

    virtual VecIndexPtr
    LoadData(const knowhere::QuantizerPtr& q, const Config& conf) {
        return nullptr;
    }

    virtual Status
    SetQuantizer(const knowhere::QuantizerPtr& q) {
        return Status::OK();
    }

    virtual Status
    UnsetQuantizer() {
        return Status::OK();
    }

    virtual std::pair<VecIndexPtr, knowhere::QuantizerPtr>
    CopyToGpuWithQuantizer(const int64_t& device_id, const Config& cfg = Config()) {
        return std::make_pair(nullptr, nullptr);
    }
    ////////////////
 private:
    int64_t size_ = 0;
};

extern Status
write_index(VecIndexPtr index, const std::string& location);

extern VecIndexPtr
read_index(const std::string& location);

VecIndexPtr
read_index(const std::string& location, knowhere::BinarySet& index_binary);

extern VecIndexPtr
GetVecIndexFactory(const IndexType& type, const Config& cfg = Config());

extern VecIndexPtr
LoadVecIndex(const IndexType& index_type, const knowhere::BinarySet& index_binary, int64_t size);

extern IndexType
ConvertToCpuIndexType(const IndexType& type);

extern IndexType
ConvertToGpuIndexType(const IndexType& type);

}  // namespace engine
}  // namespace milvus
