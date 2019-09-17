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


#include "CpuCacheMgr.h"
#include "server/ServerConfig.h"
#include "utils/Log.h"

namespace zilliz {
namespace milvus {
namespace cache {

namespace {
    constexpr int64_t unit = 1024 * 1024 * 1024;
}

CpuCacheMgr::CpuCacheMgr() {
    server::ConfigNode& config = server::ServerConfig::GetInstance().GetConfig(server::CONFIG_CACHE);
    int64_t cap = config.GetInt64Value(server::CONFIG_CPU_CACHE_CAPACITY, 16);
    cap *= unit;
    cache_ = std::make_shared<Cache<DataObjPtr>>(cap, 1UL<<32);

    double free_percent = config.GetDoubleValue(server::CACHE_FREE_PERCENT, 0.85);
    if(free_percent > 0.0 && free_percent <= 1.0) {
        cache_->set_freemem_percent(free_percent);
    } else {
        SERVER_LOG_ERROR << "Invalid cache_free_percent: " << free_percent <<
         ", defaultly set to " << cache_->freemem_percent();
    }
}

CpuCacheMgr* CpuCacheMgr::GetInstance() {
    static CpuCacheMgr s_mgr;
    return &s_mgr;
}

engine::VecIndexPtr CpuCacheMgr::GetIndex(const std::string& key) {
    DataObjPtr obj = GetItem(key);
    if(obj != nullptr) {
        return obj->data();
    }

    return nullptr;
}

}
}
}