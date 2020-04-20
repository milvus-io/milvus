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

#include "config/handler/CacheConfigHandler.h"
#include "config/Config.h"

namespace milvus {
namespace server {

CacheConfigHandler::CacheConfigHandler() {
    auto& config = Config::GetInstance();
    config.GetCacheConfigCpuCacheCapacity(cpu_cache_capacity_);
    config.GetCacheConfigInsertBufferSize(insert_buffer_size_);
    config.GetCacheConfigCacheInsertData(cache_insert_data_);
}

CacheConfigHandler::~CacheConfigHandler() {
    RemoveCpuCacheCapacityListener();
    RemoveInsertBufferSizeListener();
    RemoveCacheInsertDataListener();
}

//////////////////////////// Listener methods //////////////////////////////////
void
CacheConfigHandler::AddCpuCacheCapacityListener() {
    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        auto status = config.GetCacheConfigCpuCacheCapacity(cpu_cache_capacity_);
        if (status.ok()) {
            OnCpuCacheCapacityChanged(cpu_cache_capacity_);
        }

        return status;
    };

    auto& config = Config::GetInstance();
    config.RegisterCallBack(CONFIG_CACHE, CONFIG_CACHE_CPU_CACHE_CAPACITY, identity_, lambda);
}

void
CacheConfigHandler::AddInsertBufferSizeListener() {
    ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        auto status = config.GetCacheConfigInsertBufferSize(insert_buffer_size_);
        if (status.ok()) {
            OnInsertBufferSizeChanged(insert_buffer_size_);
        }
        return status;
    };

    auto& config = Config::GetInstance();
    config.RegisterCallBack(CONFIG_CACHE, CONFIG_CACHE_INSERT_BUFFER_SIZE, identity_, lambda);
}

void
CacheConfigHandler::AddCacheInsertDataListener() {
    server::ConfigCallBackF lambda = [this](const std::string& value) -> Status {
        auto& config = Config::GetInstance();
        auto status = config.GetCacheConfigCacheInsertData(cache_insert_data_);
        if (status.ok()) {
            OnCacheInsertDataChanged(cache_insert_data_);
        }
        return status;
    };

    auto& config = Config::GetInstance();
    config.RegisterCallBack(CONFIG_CACHE, CONFIG_CACHE_CACHE_INSERT_DATA, identity_, lambda);
}

void
CacheConfigHandler::RemoveCpuCacheCapacityListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_CACHE, CONFIG_CACHE_CPU_CACHE_CAPACITY, identity_);
}

void
CacheConfigHandler::RemoveInsertBufferSizeListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(CONFIG_CACHE, CONFIG_CACHE_INSERT_BUFFER_SIZE, identity_);
}

void
CacheConfigHandler::RemoveCacheInsertDataListener() {
    auto& config = Config::GetInstance();
    config.CancelCallBack(server::CONFIG_CACHE, server::CONFIG_CACHE_CACHE_INSERT_DATA, identity_);
}
}  // namespace server
}  // namespace milvus
