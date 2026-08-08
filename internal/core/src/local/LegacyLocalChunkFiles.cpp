// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "local/LegacyLocalChunkFiles.h"

#include <filesystem>

#include "common/EasyAssert.h"
#include "storage/LocalChunkManagerSingleton.h"

namespace milvus::local {

FileSystem
LegacyLocalChunkFiles() {
    auto chunk_manager =
        storage::LocalChunkManagerSingleton::GetInstance().GetChunkManager();
    if (chunk_manager == nullptr) {
        ThrowInfo(ErrorCode::FileOpenFailed,
                  "legacy local chunk manager is not initialized");
    }

    auto root = std::filesystem::absolute(chunk_manager->GetRootPath());
    return FileSystem::Open(root.lexically_normal());
}

}  // namespace milvus::local
