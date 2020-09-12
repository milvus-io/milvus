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

#include "cache/CpuCacheMgr.h"

#include <utility>

#include <fiu-local.h>

#include "config/Config.h"
#include "utils/Log.h"

namespace milvus {
namespace cache {

namespace {
// constexpr int64_t unit = 1024 * 1024 * 1024;
constexpr int64_t unit = 1;
}  // namespace

CpuCacheMgr::CpuCacheMgr() {
    // All config values have been checked in Config::ValidateConfig()
    server::Config& config = server::Config::GetInstance();

    int64_t cpu_cache_cap;
    config.GetCacheConfigCpuCacheCapacity(cpu_cache_cap);
    int64_t cap = cpu_cache_cap * unit;
    LOG_SERVER_DEBUG_ << "cpu cache.size: " << cap;
    LOG_SERVER_INFO_ << "cpu cache.size: " << cap;
    cache_ = std::make_shared<Cache<DataObjPtr>>(cap, 1UL << 32, "[CACHE CPU]");

    float cpu_cache_threshold;
    config.GetCacheConfigCpuCacheThreshold(cpu_cache_threshold);
    cache_->set_freemem_percent(cpu_cache_threshold);

    SetIdentity("CpuCacheMgr");
    AddCpuCacheCapacityListener();
}

CpuCacheMgr*
CpuCacheMgr::GetInstance() {
    static CpuCacheMgr s_mgr;
    return &s_mgr;
}

DataObjPtr
CpuCacheMgr::GetIndex(const std::string& key) {
    DataObjPtr obj = GetItem(key);
    return obj;
}

void
CpuCacheMgr::OnCpuCacheCapacityChanged(int64_t value) {
    SetCapacity(value * unit);
}

}  // namespace cache
}  // namespace milvus
