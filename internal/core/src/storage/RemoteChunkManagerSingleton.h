// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <shared_mutex>

#include "storage/Util.h"

namespace milvus::storage {

class RemoteChunkManagerSingleton {
 private:
    RemoteChunkManagerSingleton() {
    }

 public:
    RemoteChunkManagerSingleton(const RemoteChunkManagerSingleton&) = delete;
    RemoteChunkManagerSingleton&
    operator=(const RemoteChunkManagerSingleton&) = delete;

    static RemoteChunkManagerSingleton&
    GetInstance() {
        static RemoteChunkManagerSingleton instance;
        return instance;
    }

    void
    Init(const StorageConfig& storage_config) {
        std::unique_lock lck(mutex_);
        if (rcm_ == nullptr) {
            rcm_ = CreateChunkManager(storage_config);
        }
    }

    void
    Release() {
        std::unique_lock lck(mutex_);
        rcm_ = nullptr;
    }

    ChunkManagerPtr
    GetRemoteChunkManager() {
        return rcm_;
    }

 private:
    mutable std::shared_mutex mutex_;
    ChunkManagerPtr rcm_ = nullptr;
};

}  // namespace milvus::storage
