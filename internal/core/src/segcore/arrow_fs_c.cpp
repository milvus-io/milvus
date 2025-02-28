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

#include "segcore/arrow_fs_c.h"
#include "milvus-storage/filesystem/fs.h"
#include "common/EasyAssert.h"
#include "common/type_c.h"

CStatus
InitLocalArrowFileSystemSingleton(const char* c_path) {
    try {
        std::string path(c_path);
        milvus_storage::ArrowFileSystemConfig conf;
        conf.root_path = path;
        conf.storage_type = "local";
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
CleanLocalArrowFileSystemSingleton() {
    milvus_storage::ArrowFileSystemSingleton::GetInstance().Release();
}

CStatus
InitRemoteArrowFileSystemSingleton(CStorageConfig c_storage_config) {
    try {
        milvus_storage::ArrowFileSystemConfig conf;
        conf.address = std::string(c_storage_config.address);
        conf.bucket_name = std::string(c_storage_config.bucket_name);
        conf.access_key_id = std::string(c_storage_config.access_key_id);
        conf.access_key_value = std::string(c_storage_config.access_key_value);
        conf.root_path = std::string(c_storage_config.root_path);
        conf.storage_type = std::string(c_storage_config.storage_type);
        conf.cloud_provider = std::string(c_storage_config.cloud_provider);
        conf.iam_endpoint = std::string(c_storage_config.iam_endpoint);
        conf.log_level = std::string(c_storage_config.log_level);
        conf.region = std::string(c_storage_config.region);
        conf.useSSL = c_storage_config.useSSL;
        conf.sslCACert = std::string(c_storage_config.sslCACert);
        conf.useIAM = c_storage_config.useIAM;
        conf.useVirtualHost = c_storage_config.useVirtualHost;
        conf.requestTimeoutMs = c_storage_config.requestTimeoutMs;
        conf.gcp_credential_json =
            std::string(c_storage_config.gcp_credential_json);
        conf.use_custom_part_upload = c_storage_config.use_custom_part_upload;
        milvus_storage::ArrowFileSystemSingleton::GetInstance().Init(conf);

        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
CleanRemoteArrowFileSystemSingleton() {
    milvus_storage::ArrowFileSystemSingleton::GetInstance().Release();
}
