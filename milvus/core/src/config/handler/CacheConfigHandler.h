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
#pragma once

#include <string>

#include "config/handler/ConfigHandler.h"

namespace milvus {
namespace server {

class CacheConfigHandler : virtual public ConfigHandler {
 public:
    CacheConfigHandler();
    virtual ~CacheConfigHandler();

 protected:
    virtual void
    OnCpuCacheCapacityChanged(int64_t value) {
    }

    virtual void
    OnInsertBufferSizeChanged(int64_t value) {
    }

    virtual void
    OnCacheInsertDataChanged(bool value) {
    }

 protected:
    void
    AddCpuCacheCapacityListener();

    void
    AddInsertBufferSizeListener();

    void
    AddCacheInsertDataListener();

    void
    RemoveCpuCacheCapacityListener();

    void
    RemoveInsertBufferSizeListener();

    void
    RemoveCacheInsertDataListener();

 private:
    int64_t cpu_cache_capacity_ = std::stoll(CONFIG_CACHE_CPU_CACHE_CAPACITY_DEFAULT) /*GiB*/;
    int64_t insert_buffer_size_ = std::stoll(CONFIG_CACHE_INSERT_BUFFER_SIZE_DEFAULT) /*GiB*/;
    bool cache_insert_data_ = false;
};

}  // namespace server
}  // namespace milvus
