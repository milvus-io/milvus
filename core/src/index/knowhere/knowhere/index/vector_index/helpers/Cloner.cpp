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

#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexGPUIVF.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/IndexIVFSQHybrid.h"

namespace knowhere {
namespace cloner {

VectorIndexPtr
CopyGpuToCpu(const VectorIndexPtr& index, const Config& config) {
    if (auto device_index = std::dynamic_pointer_cast<GPUIndex>(index)) {
        VectorIndexPtr result = device_index->CopyGpuToCpu(config);
        auto uids = index->GetUids();
        result->SetUids(uids);
        return result;
    } else {
        KNOWHERE_THROW_MSG("index type is not gpuindex");
    }
}

VectorIndexPtr
CopyCpuToGpu(const VectorIndexPtr& index, const int64_t& device_id, const Config& config) {
    VectorIndexPtr result;
    auto uids = index->GetUids();
#ifdef CUSTOMIZATION
    if (auto device_index = std::dynamic_pointer_cast<IVFSQHybrid>(index)) {
        result = device_index->CopyCpuToGpu(device_id, config);
        result->SetUids(uids);
        return result;
    }
#endif

    if (auto device_index = std::dynamic_pointer_cast<GPUIndex>(index)) {
        result = device_index->CopyGpuToGpu(device_id, config);
        result->SetUids(uids);
        return result;
    }

    if (auto cpu_index = std::dynamic_pointer_cast<IVFSQ>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IVFPQ>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IVF>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IDMAP>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else {
        KNOWHERE_THROW_MSG("this index type not support tranfer to gpu");
    }

    result->SetUids(uids);
    return result;
}

}  // namespace cloner
}  // namespace knowhere
