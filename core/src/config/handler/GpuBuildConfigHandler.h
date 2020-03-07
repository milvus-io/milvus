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
#ifdef MILVUS_GPU_VERSION
#pragma once

#include <vector>

#include "config/handler/GpuConfigHandler.h"

namespace milvus {
namespace server {

class GpuBuildConfigHandler : virtual public GpuConfigHandler {
 public:
    GpuBuildConfigHandler();

    ~GpuBuildConfigHandler();

 public:
    virtual void
    OnGpuBuildResChanged(const std::vector<int64_t>& gpus);

 protected:
    void
    AddGpuBuildResListener();

    void
    RemoveGpuBuildResListener();

 protected:
    std::vector<int64_t> build_gpus_;
};

}  // namespace server
}  // namespace milvus
#endif
