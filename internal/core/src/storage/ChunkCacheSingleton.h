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

#include <shared_mutex>
#include "ChunkCache.h"
#include "RemoteChunkManagerSingleton.h"

namespace milvus::storage {

class ChunkCacheSingleton {
 private:
    ChunkCacheSingleton() {
    }

 public:
    ChunkCacheSingleton(const ChunkCacheSingleton&) = delete;
    ChunkCacheSingleton&
    operator=(const ChunkCacheSingleton&) = delete;

    static ChunkCacheSingleton&
    GetInstance() {
        static ChunkCacheSingleton instance;
        return instance;
    }

    void
    Init(std::string root_path, std::string read_ahead_policy) {
        if (cc_ == nullptr) {
            auto rcm = RemoteChunkManagerSingleton::GetInstance()
                           .GetRemoteChunkManager();
            cc_ = std::make_shared<ChunkCache>(
                std::move(root_path), std::move(read_ahead_policy), rcm);
        }
    }

    ChunkCachePtr
    GetChunkCache() {
        return cc_;
    }

 private:
    ChunkCachePtr cc_ = nullptr;
};

}  // namespace milvus::storage