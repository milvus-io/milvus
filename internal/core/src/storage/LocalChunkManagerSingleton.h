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
#include <mutex>
#include <shared_mutex>

#include "storage/ChunkManager.h"
#include "storage/LocalChunkManager.h"

namespace milvus::storage {

class LocalChunkManagerSingleton {
 private:
    LocalChunkManagerSingleton() {
    }

 public:
    LocalChunkManagerSingleton(const LocalChunkManagerSingleton&) = delete;
    LocalChunkManagerSingleton&
    operator=(const LocalChunkManagerSingleton&) = delete;

    static LocalChunkManagerSingleton&
    GetInstance() {
        static LocalChunkManagerSingleton instance;
        return instance;
    }

    void
    Init(std::string root_path) {
        std::unique_lock lck(mutex_);
        if (lcm_ == nullptr) {
            lcm_ = std::make_shared<LocalChunkManager>(root_path);
        }
    }

    void
    Release() {
        std::unique_lock lck(mutex_);
        lcm_ = nullptr;
    }

    LocalChunkManagerSPtr
    GetChunkManager() {
        return lcm_;
    }

 private:
    mutable std::shared_mutex mutex_;
    LocalChunkManagerSPtr lcm_ = nullptr;
};

}  // namespace milvus::storage