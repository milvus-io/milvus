////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "FaissGpuResources.h"
#include "map"

namespace zilliz {
namespace milvus {
namespace engine {

FaissGpuResources::Ptr& FaissGpuResources::GetGpuResources(int device_id) {
    static std::map<int, FaissGpuResources::Ptr> gpu_resources_map;
    auto search = gpu_resources_map.find(device_id);
    if (search != gpu_resources_map.end()) {
        return gpu_resources_map[device_id];
    } else {
        gpu_resources_map[device_id] = std::make_shared<faiss::gpu::StandardGpuResources>();
        return gpu_resources_map[device_id];
    }
}

void FaissGpuResources::SelectGpu() {
    using namespace zilliz::milvus::server;
    ServerConfig &config = ServerConfig::GetInstance();
    ConfigNode server_config = config.GetConfig(CONFIG_SERVER);
    gpu_num_ = server_config.GetInt32Value(server::CONFIG_GPU_INDEX, 0);
}

int32_t FaissGpuResources::GetGpu() {
    return gpu_num_;
}

}
}
}