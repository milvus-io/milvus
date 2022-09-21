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

#include "storage/storage_c.h"
#include "config/ConfigChunkManager.h"
#include "common/CGoHelper.h"

#ifdef BUILD_DISK_ANN
#include "storage/LocalChunkManager.h"
#endif

CStatus
GetLocalUsedSize(int64_t* size) {
    try {
#ifdef BUILD_DISK_ANN
        auto& local_chunk_manager = milvus::storage::LocalChunkManager::GetInstance();
        auto dir = milvus::ChunkMangerConfig::GetLocalRootPath();
        if (local_chunk_manager.DirExist(dir)) {
            *size = local_chunk_manager.GetSizeOfDir(dir);
        } else {
            *size = 0;
        }
#endif
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}
