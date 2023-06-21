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
#include "common/CGoHelper.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/LocalChunkManagerSingleton.h"

CStatus
GetLocalUsedSize(const char* c_dir, int64_t* size) {
    try {
        auto local_chunk_manager =
            milvus::storage::LocalChunkManagerSingleton::GetInstance()
                .GetChunkManager();
        std::string dir(c_dir);
        if (local_chunk_manager->DirExist(dir)) {
            *size = local_chunk_manager->GetSizeOfDir(dir);
        } else {
            *size = 0;
        }
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
InitLocalChunkManagerSingleton(const char* c_path) {
    try {
        std::string path(c_path);
        milvus::storage::LocalChunkManagerSingleton::GetInstance().Init(path);

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

CStatus
InitRemoteChunkManagerSingleton(CStorageConfig c_storage_config) {
    try {
        milvus::storage::StorageConfig storage_config;
        storage_config.address = std::string(c_storage_config.address);
        storage_config.bucket_name = std::string(c_storage_config.bucket_name);
        storage_config.access_key_id =
            std::string(c_storage_config.access_key_id);
        storage_config.access_key_value =
            std::string(c_storage_config.access_key_value);
        storage_config.root_path = std::string(c_storage_config.root_path);
        storage_config.storage_type =
            std::string(c_storage_config.storage_type);
        storage_config.iam_endpoint =
            std::string(c_storage_config.iam_endpoint);
        storage_config.useSSL = c_storage_config.useSSL;
        storage_config.useIAM = c_storage_config.useIAM;
        milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(
            storage_config);

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(UnexpectedError, e.what());
    }
}

void
CleanRemoteChunkManagerSingleton() {
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Release();
}
