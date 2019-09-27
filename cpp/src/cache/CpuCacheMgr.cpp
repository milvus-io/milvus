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

namespace zilliz {
namespace milvus {
namespace cache {

namespace {
constexpr int64_t unit = 1024 * 1024 * 1024;
}

CpuCacheMgr::CpuCacheMgr() {
    server::Config &config = server::Config::GetInstance();
    Status s;

    int32_t cpu_mem_cap;
    s = config.GetCacheConfigCpuMemCapacity(cpu_mem_cap);
    if (!s.ok()) {
        SERVER_LOG_ERROR << s.message();
    }
    int64_t cap = cpu_mem_cap * unit;
    cache_ = std::make_shared<Cache<DataObjPtr>>(cap, 1UL << 32);

    float cpu_mem_threshold;
    s = config.GetCacheConfigCpuMemThreshold(cpu_mem_threshold);
    if (!s.ok()) {
        SERVER_LOG_ERROR << s.message();
    }
    if (cpu_mem_threshold > 0.0 && cpu_mem_threshold <= 1.0) {
        cache_->set_freemem_percent(cpu_mem_threshold);
    } else {
        SERVER_LOG_ERROR << "Invalid cpu_mem_threshold: " << cpu_mem_threshold
                         << ", by default set to " << cache_->freemem_percent();
    }
}

CpuCacheMgr *
CpuCacheMgr::GetInstance() {
    static CpuCacheMgr s_mgr;
    return &s_mgr;
}

engine::VecIndexPtr
CpuCacheMgr::GetIndex(const std::string &key) {
    DataObjPtr obj = GetItem(key);
    if (obj != nullptr) {
        return obj->data();
    }

    return nullptr;
}

} // namespace cache
} // namespace milvus
} // namespace zilliz
