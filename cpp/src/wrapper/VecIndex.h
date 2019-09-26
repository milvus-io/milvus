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

#include <string>
#include <memory>

#include "utils/Status.h"
#include "knowhere/common/Config.h"
#include "knowhere/common/BinarySet.h"

namespace zilliz {
namespace milvus {
namespace engine {

// TODO(linxj): jsoncons => rapidjson or other.
using Config = zilliz::knowhere::Config;

enum class IndexType {
    INVALID = 0,
    FAISS_IDMAP = 1,
    FAISS_IVFFLAT_CPU,
    FAISS_IVFFLAT_GPU,
    FAISS_IVFFLAT_MIX, // build on gpu and search on cpu
    FAISS_IVFPQ_CPU,
    FAISS_IVFPQ_GPU,
    SPTAG_KDT_RNT_CPU,
    FAISS_IVFSQ8_MIX,
    FAISS_IVFSQ8_CPU,
    FAISS_IVFSQ8_GPU,
    NSG_MIX,
};

class VecIndex;

using VecIndexPtr = std::shared_ptr<VecIndex>;

class VecIndex {
 public:
    virtual Status
    BuildAll(const int64_t &nb,
             const float *xb,
             const int64_t *ids,
             const Config &cfg,
             const int64_t &nt = 0,
             const float *xt = nullptr) = 0;

    virtual Status
    Add(const int64_t &nb,
        const float *xb,
        const int64_t *ids,
        const Config &cfg = Config()) = 0;

    virtual Status
    Search(const int64_t &nq,
           const float *xq,
           float *dist,
           int64_t *ids,
           const Config &cfg = Config()) = 0;

    virtual VecIndexPtr
    CopyToGpu(const int64_t &device_id,
              const Config &cfg = Config()) = 0;

    virtual VecIndexPtr
    CopyToCpu(const Config &cfg = Config()) = 0;

    virtual VecIndexPtr
    Clone() = 0;

    virtual int64_t
    GetDeviceId() = 0;

    virtual IndexType
    GetType() = 0;

    virtual int64_t
    Dimension() = 0;

    virtual int64_t
    Count() = 0;

    virtual zilliz::knowhere::BinarySet
    Serialize() = 0;

    virtual Status
    Load(const zilliz::knowhere::BinarySet &index_binary) = 0;
};

extern Status
write_index(VecIndexPtr index, const std::string &location);

extern VecIndexPtr
read_index(const std::string &location);

extern VecIndexPtr
GetVecIndexFactory(const IndexType &type, const Config &cfg = Config());

extern VecIndexPtr
LoadVecIndex(const IndexType &index_type, const zilliz::knowhere::BinarySet &index_binary);

extern void
AutoGenParams(const IndexType &type, const int64_t &size, Config &cfg);

extern void
ParameterValidation(const IndexType &type, Config &cfg);

extern IndexType
ConvertToCpuIndexType(const IndexType &type);

extern IndexType
ConvertToGpuIndexType(const IndexType &type);

} // namespace engine
} // namespace milvus
} // namespace zilliz
