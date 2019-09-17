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


#include "knowhere/common/exception.h"
#include "knowhere/index/vector_index/cloner.h"
#include "knowhere/index/vector_index/ivf.h"
#include "knowhere/index/vector_index/gpu_ivf.h"
#include "knowhere/index/vector_index/idmap.h"


namespace zilliz {
namespace knowhere {

VectorIndexPtr CopyGpuToCpu(const VectorIndexPtr &index, const Config &config) {
    if (auto device_index = std::dynamic_pointer_cast<GPUIndex>(index)) {
        return device_index->CopyGpuToCpu(config);
    } else {
        KNOWHERE_THROW_MSG("index type is not gpuindex");
    }
}

VectorIndexPtr CopyCpuToGpu(const VectorIndexPtr &index, const int64_t &device_id, const Config &config) {
    if (auto device_index = std::dynamic_pointer_cast<GPUIndex>(index)) {
        return device_index->CopyGpuToGpu(device_id, config);
    }

    if (auto cpu_index = std::dynamic_pointer_cast<IVFSQ>(index)) {
        return cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IVFPQ>(index)) {
        KNOWHERE_THROW_MSG("IVFPQ not support tranfer to gpu");
    } else if (auto cpu_index = std::dynamic_pointer_cast<IVF>(index)) {
        return cpu_index->CopyCpuToGpu(device_id, config);
    } else if (auto cpu_index = std::dynamic_pointer_cast<IDMAP>(index)) {
        return cpu_index->CopyCpuToGpu(device_id, config);
    } else {
        KNOWHERE_THROW_MSG("this index type not support tranfer to gpu");
    }
}

}
}
