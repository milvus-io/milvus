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

#include "cache/GpuCacheMgr.h"
#include "server/Config.h"
#include "utils/Log.h"

#include <sstream>
#include <utility>

namespace milvus {
namespace cache {

#ifdef MILVUS_GPU_VERSION
std::mutex GpuCacheMgr::mutex_;
std::unordered_map<uint64_t, GpuCacheMgrPtr> GpuCacheMgr::instance_;

namespace {
constexpr int64_t G_BYTE = 1024 * 1024 * 1024;
}

GpuCacheMgr::GpuCacheMgr() {
    // All config values have been checked in Config::ValidateConfig()
    server::Config& config = server::Config::GetInstance();

    int64_t gpu_cache_cap;
    config.GetGpuResourceConfigCacheCapacity(gpu_cache_cap);
    int64_t cap = gpu_cache_cap * G_BYTE;
    cache_ = std::make_shared<Cache<DataObjPtr>>(cap, 1UL << 32);

    float gpu_mem_threshold;
    config.GetGpuResourceConfigCacheThreshold(gpu_mem_threshold);
    cache_->set_freemem_percent(gpu_mem_threshold);
}

GpuCacheMgr*
GpuCacheMgr::GetInstance(uint64_t gpu_id) {
    if (instance_.find(gpu_id) == instance_.end()) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (instance_.find(gpu_id) == instance_.end()) {
            instance_.insert(std::pair<uint64_t, GpuCacheMgrPtr>(gpu_id, std::make_shared<GpuCacheMgr>()));
        }
        return instance_[gpu_id].get();
    } else {
        std::lock_guard<std::mutex> lock(mutex_);
        return instance_[gpu_id].get();
    }
}

DataObjPtr
GpuCacheMgr::GetIndex(const std::string& key) {
    DataObjPtr obj = GetItem(key);
    return obj;
}
#endif

}  // namespace cache
}  // namespace milvus
