/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

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
        //KNOWHERE_THROW_MSG("IVFSQ not support tranfer to gpu");
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
