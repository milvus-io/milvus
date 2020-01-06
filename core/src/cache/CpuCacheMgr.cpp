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

#include "cache/CpuCacheMgr.h"
#include "server/Config.h"
#include "utils/Log.h"

#include <utility>

namespace milvus {
namespace cache {

namespace {
constexpr int64_t unit = 1024 * 1024 * 1024;
}

CpuCacheMgr::CpuCacheMgr() {
    // All config values have been checked in Config::ValidateConfig()
    server::Config& config = server::Config::GetInstance();

    int64_t cpu_cache_cap;
    config.GetCacheConfigCpuCacheCapacity(cpu_cache_cap);
    int64_t cap = cpu_cache_cap * unit;
    cache_ = std::make_shared<Cache<DataObjPtr>>(cap, 1UL << 32);

    float cpu_cache_threshold;
    config.GetCacheConfigCpuCacheThreshold(cpu_cache_threshold);
    cache_->set_freemem_percent(cpu_cache_threshold);
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

}  // namespace cache
}  // namespace milvus
