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

#include "cache/GpuCacheMgr.h"
#include "utils/Log.h"
#include "value/config/ServerConfig.h"

#include <fiu/fiu-local.h>
#include <sstream>
#include <utility>

namespace milvus {
namespace cache {

#ifdef MILVUS_GPU_VERSION
std::mutex GpuCacheMgr::global_mutex_;
std::unordered_map<int64_t, GpuCacheMgrPtr> GpuCacheMgr::instance_;

GpuCacheMgr::GpuCacheMgr(int64_t gpu_id) : gpu_id_(gpu_id) {
    std::string header = "[CACHE GPU" + std::to_string(gpu_id) + "]";
    cache_ = std::make_shared<Cache<DataObjPtr>>(config.gpu.cache_size(), 1UL << 32, header);

    if (config.gpu.cache_threshold() > 0.0) {
        cache_->set_freemem_percent(config.gpu.cache_threshold());
    }
    ConfigMgr::GetInstance().Attach("gpu.cache_threshold", this);
}

GpuCacheMgr::~GpuCacheMgr() {
    ConfigMgr::GetInstance().Detach("gpu.cache_threshold", this);
}

GpuCacheMgrPtr
GpuCacheMgr::GetInstance(int64_t gpu_id) {
    if (instance_.find(gpu_id) == instance_.end()) {
        std::lock_guard<std::mutex> lock(global_mutex_);
        if (instance_.find(gpu_id) == instance_.end()) {
            instance_[gpu_id] = std::make_shared<GpuCacheMgr>(gpu_id);
        }
    }
    return instance_[gpu_id];
}

void
GpuCacheMgr::ConfigUpdate(const std::string& name) {
    std::lock_guard<std::mutex> lock(global_mutex_);
    for (auto& it : instance_) {
        it.second->SetCapacity(config.gpu.cache_size());
    }
}

#endif

}  // namespace cache
}  // namespace milvus
