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

#include <fiu/fiu-local.h>

#include "utils/Log.h"
#include "value/config/ServerConfig.h"

namespace milvus {
namespace cache {

CpuCacheMgr&
CpuCacheMgr::GetInstance() {
    static CpuCacheMgr s_mgr;
    return s_mgr;
}

CpuCacheMgr::CpuCacheMgr() {
    cache_ = std::make_shared<Cache<DataObjPtr>>(config.cache.cache_size(), 1UL << 32, "[CACHE CPU]");

    if (config.cache.cpu_cache_threshold() > 0.0) {
        cache_->set_freemem_percent(config.cache.cpu_cache_threshold());
    }
    ConfigMgr::GetInstance().Attach("cache.cache_size", this);
}

CpuCacheMgr::~CpuCacheMgr() {
    ConfigMgr::GetInstance().Detach("cache.cache_size", this);
}

DataObjPtr
CpuCacheMgr::GetItem(const std::string& key) {
    auto ret = CacheMgr<DataObjPtr>::GetItem(key);
    return ret;
}

void
CpuCacheMgr::ConfigUpdate(const std::string& name) {
    SetCapacity(config.cache.cache_size());
}

}  // namespace cache
}  // namespace milvus
