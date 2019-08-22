/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "faiss/gpu/GpuResources.h"
#include "faiss/gpu/StandardGpuResources.h"

#include "server/ServerConfig.h"

namespace zilliz {
namespace milvus {
namespace engine {

class FaissGpuResources {

 public:
    using Ptr = std::shared_ptr<faiss::gpu::GpuResources>;

    static FaissGpuResources::Ptr& GetGpuResources(int device_id);

    void SelectGpu();

    int32_t GetGpu();

    FaissGpuResources() : gpu_num_(0) { SelectGpu(); }

 private:
    int32_t gpu_num_;
};

}
}
}