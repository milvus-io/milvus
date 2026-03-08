// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <string>
#include "google/cloud/storage/client.h"
#include "google/cloud/options.h"
#include "common/EasyAssert.h"

namespace gcpnative {
/**
 * @brief This GcpNativeClientManager is used to interact with 
 * Google Cloud Storage (GCS) services.
   */
class GcpNativeClientManager {
 public:
    explicit GcpNativeClientManager(const std::string& address,
                                    const std::string& gcp_credential_json,
                                    bool use_ssl,
                                    bool gcp_native_without_auth,
                                    int64_t request_timeout_ms = 0);

    GcpNativeClientManager(const GcpNativeClientManager&);
    GcpNativeClientManager&
    operator=(const GcpNativeClientManager&);

    ~GcpNativeClientManager();

 public:
    bool
    BucketExists(const std::string& bucket_name);
    bool
    CreateBucket(const std::string& bucket_name);
    bool
    DeleteBucket(const std::string& bucket_name);
    std::vector<std::string>
    ListBuckets();
    bool
    ObjectExists(const std::string& bucket_name,
                 const std::string& object_name);
    int64_t
    GetObjectSize(const std::string& bucket_name,
                  const std::string& object_name);
    bool
    DeleteObject(const std::string& bucket_name,
                 const std::string& object_name);
    bool
    PutObjectBuffer(const std::string& bucket_name,
                    const std::string& object_name,
                    void* buf,
                    uint64_t size);
    uint64_t
    GetObjectBuffer(const std::string& bucket_name,
                    const std::string& object_name,
                    void* buf,
                    uint64_t size);
    std::vector<std::string>
    ListObjects(const std::string& bucket_name,
                const std::string& prefix = nullptr);

 private:
    inline std::string
    GetGcpNativeError(google::cloud::Status&& status) {
        return GetGcpNativeError(status);
    }

    inline std::string
    GetGcpNativeError(const google::cloud::Status& status) {
        return fmt::format("Gcp native error: {} ({})",
                           status.message(),
                           static_cast<int>(status.code()));
    }

 private:
    std::unique_ptr<google::cloud::storage::Client> client_;
};

}  // namespace gcpnative
