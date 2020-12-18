// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#ifdef MILVUS_GPU_VERSION
#include "knowhere/index/vector_index/helpers/Cloner.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexIDMAP.h"
#include "knowhere/index/vector_index/IndexIVF.h"
#include "knowhere/index/vector_index/IndexIVFPQ.h"
#include "knowhere/index/vector_index/IndexIVFSQ.h"
#include "knowhere/index/vector_index/gpu/GPUIndex.h"
#include "knowhere/index/vector_index/gpu/IndexGPUIVF.h"
#include "knowhere/index/vector_index/gpu/IndexIVFSQHybrid.h"
#include "knowhere/index/vector_offset_index/IndexIVF_NM.h"

namespace milvus {
namespace knowhere {
namespace cloner {

void
CopyIndexData(const VecIndexPtr& dst_index, const VecIndexPtr& src_index) {
    dst_index->SetUids(src_index->GetUids());
    dst_index->SetIndexSize(src_index->IndexSize());
}

VecIndexPtr
CopyGpuToCpu(const VecIndexPtr& index, const Config& config) {
    if (auto device_index = std::dynamic_pointer_cast<GPUIndex>(index)) {
        VecIndexPtr result = device_index->CopyGpuToCpu(config);
        CopyIndexData(result, index);
        return result;
    } else {
        KNOWHERE_THROW_MSG("index type is not gpuindex");
    }
}

VecIndexPtr
CopyCpuToGpu(const VecIndexPtr& index, const int64_t device_id, const Config& config) {
    VecIndexPtr result;
    if (auto device_index = std::dynamic_pointer_cast<IVFSQHybrid>(index)) {
        result = device_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IVF_NM>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto device_index = std::dynamic_pointer_cast<GPUIndex>(index)) {
        result = device_index->CopyGpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IVFSQ>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IVFPQ>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IVF>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IDMAP>(index)) {
        result = cpu_index->CopyCpuToGpu(device_id, config);
    } else {
        KNOWHERE_THROW_MSG("this index type not support transfer to gpu");
    }
    if (result != nullptr) {
        CopyIndexData(result, index);
    }
    return result;
}

}  // namespace cloner
}  // namespace knowhere
}  // namespace milvus
#endif
