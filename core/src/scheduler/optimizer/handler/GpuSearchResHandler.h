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

#include "scheduler/optimizer/handler/GpuResourcesHandler.h"

#include <limits>
#include <vector>

namespace milvus {
namespace scheduler {

class GpuSearchResHandler : virtual public GpuResourcesHandler {
 public:
    GpuSearchResHandler();

    ~GpuSearchResHandler();

 public:
    virtual void
    OnGpuSearchThresholdChanged(int64_t threshold);

    virtual void
    OnGpuSearchResChanged(const std::vector<int64_t>& gpus);

 protected:
    void
    AddGpuSearchThresholdListener();

    void
    AddGpuSearchResListener();

 protected:
    int64_t threshold_ = std::numeric_limits<int64_t>::max();
    std::vector<int64_t> search_gpus_;
};

}  // namespace scheduler
}  // namespace milvus
#endif
