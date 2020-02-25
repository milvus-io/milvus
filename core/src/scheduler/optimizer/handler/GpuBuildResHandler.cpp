//
// Created by yhz on 2020/2/25.
//

#include "scheduler/optimizer/handler/GpuBuildResHandler.h"

namespace milvus {
namespace scheduler {

GpuBuildResHandler::GpuBuildResHandler() {
    server::Config& config = server::Config::GetInstance();
    config.GetGpuResourceConfigBuildIndexResources(build_gpus_);
}

GpuBuildResHandler::~GpuBuildResHandler() {
    server::Config& config = server::Config::GetInstance();
    config.CancelCallBack(server::CONFIG_GPU_RESOURCE,
                          server::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES,
                          identity_);
}

////////////////////////////////////////////////////////////////
void GpuBuildResHandler::OnGpuBuildResChanged(const std::vector<int64_t>& gpus) {
    build_gpus_ = gpus;
}

void
GpuBuildResHandler::AddGpuBuildResListener() {
    server::Config& config = server::Config::GetInstance();
    server::ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        server::Config& config = server::Config::GetInstance();
        std::vector<int64_t> gpu_ids;
        auto status = config.GetGpuResourceConfigSearchResources(gpu_ids);
        if (status.ok()) {
            OnGpuBuildResChanged(gpu_ids);
        }

        return status;
    };
    config.RegisterCallBack(server::CONFIG_GPU_RESOURCE,
                            server::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES,
                            identity_,
                            lambda);
}

}
}
