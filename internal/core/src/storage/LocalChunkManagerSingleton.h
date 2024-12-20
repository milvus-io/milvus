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

#include "common/Common.h"
#include "storage/ChunkManager.h"
#include "storage/LocalChunkManager.h"
#include "log/Log.h"

namespace milvus::storage {

class LocalChunkManagerFactory {
 public:
    static LocalChunkManagerFactory&
    GetInstance() {
        static LocalChunkManagerFactory instance;
        return instance;
    }

    void
    AddChunkManager(Role role, std::string root_path) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (chunk_managers_.find(role) != chunk_managers_.end()) {
            PanicInfo(UnexpectedError,
                      "chunk manager for role {} already exists",
                      ToString(role));
        }
        LOG_INFO("add chunk manager for role {}", ToString(role));
        chunk_managers_[role] = std::make_shared<LocalChunkManager>(root_path);
    }

    LocalChunkManagerSPtr
    GetChunkManager(Role role) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = chunk_managers_.find(role);
        if (it == chunk_managers_.end()) {
            PanicInfo(UnexpectedError,
                      "local chunk manager for role:{} not found",
                      ToString(role));
        }
        return it->second;
    }

    // some situations not need to specify the role
    // just randomly choose one chunk manager
    // because local chunk manager no need root_path
    // and interface use abs paths params
    LocalChunkManagerSPtr
    GetChunkManager() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        Assert(chunk_managers_.size() != 0);
        return chunk_managers_.begin()->second;
    }

    mutable std::shared_mutex mutex_;
    std::unordered_map<Role, LocalChunkManagerSPtr> chunk_managers_;
};

}  // namespace milvus::storage