//
// Created by yhz on 2020/2/25.
//

#pragma once

#include "scheduler/optimizer/handler/GpuResourcesHandler.h"

namespace milvus {
namespace scheduler {

class GpuBuildResHandler : virtual public GpuResourcesHandler {
 public:
    explicit
    GpuBuildResHandler();

    ~GpuBuildResHandler();

 public:
    virtual void
    OnGpuBuildResChanged(const std::vector<int64_t>& gpus);

 protected:
    void
    AddGpuBuildResListener();

 protected:
    std::vector<int64_t> build_gpus_;
};

}
}
