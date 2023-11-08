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

#include <iostream>
#include <sstream>
#include <azure/identity/workload_identity_credential.hpp>
#include "AzureBlobChunkManager.h"

namespace azure {

std::string
GetTenantId() {
    return std::getenv("AZURE_TENANT_ID");
}
std::string
GetClientId() {
    return std::getenv("AZURE_CLIENT_ID");
}
std::string
GetTokenFilePath() {
    return std::getenv("AZURE_FEDERATED_TOKEN_FILE");
}
std::string
GetConnectionString(const std::string& access_key_id,
                    const std::string& access_key_value,
                    const std::string& address) {
    char const* tmp = getenv("AZURE_STORAGE_CONNECTION_STRING");
    if (tmp != NULL) {
        std::string envConnectionString(tmp);
        if (!envConnectionString.empty()) {
            return envConnectionString;
        }
    }
    return "DefaultEndpointsProtocol=https;AccountName=" + access_key_id +
           ";AccountKey=" + access_key_value + ";EndpointSuffix=" + address;
}

void
AzureBlobChunkManager::InitLog(
    std::string level_str,
    std::function<void(Azure::Core::Diagnostics::Logger::Level level,
                       std::string const& message)> listener) {
    // SetListener accepts std::function<>, which can be either lambda or a function pointer.
    Azure::Core::Diagnostics::Logger::SetListener(listener);
    Azure::Core::Diagnostics::Logger::Level level =
        Azure::Core::Diagnostics::Logger::Level::Verbose;
    if (level_str == "fatal" || level_str == "error") {
        level = Azure::Core::Diagnostics::Logger::Level::Error;
    } else if (level_str == "warn") {
        level = Azure::Core::Diagnostics::Logger::Level::Warning;
    } else if (level_str == "info") {
        level = Azure::Core::Diagnostics::Logger::Level::Informational;
    } else if (level_str == "debug" || level_str == "trace") {
        level = Azure::Core::Diagnostics::Logger::Level::Verbose;
    }
    // See above for the level descriptions.
    Azure::Core::Diagnostics::Logger::SetLevel(level);
}

AzureBlobChunkManager::AzureBlobChunkManager(
    const std::string& access_key_id,
    const std::string& access_key_value,
    const std::string& address,
    int64_t requestTimeoutMs,
    bool useIAM) {
    requestTimeoutMs_ = requestTimeoutMs;
    if (useIAM) {
        auto workloadIdentityCredential =
            std::make_shared<Azure::Identity::WorkloadIdentityCredential>(
                GetTenantId(), GetClientId(), GetTokenFilePath());
        client_ = std::make_shared<Azure::Storage::Blobs::BlobServiceClient>(
            "https://" + access_key_id + ".blob." + address + "/",
            workloadIdentityCredential);
    } else {
        client_ = std::make_shared<Azure::Storage::Blobs::BlobServiceClient>(
            Azure::Storage::Blobs::BlobServiceClient::
                CreateFromConnectionString(GetConnectionString(
                    access_key_id, access_key_value, address)));
    }
}

AzureBlobChunkManager::~AzureBlobChunkManager() {
}

bool
AzureBlobChunkManager::BucketExists(const std::string& bucket_name) {
    try {
        Azure::Core::Context context;
        if (requestTimeoutMs_ > 0) {
            context = context.WithDeadline(
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(requestTimeoutMs_));
        }
        client_->GetBlobContainerClient(bucket_name)
            .GetProperties(
                Azure::Storage::Blobs::GetBlobContainerPropertiesOptions(),
                context);
        return true;
    } catch (const Azure::Storage::StorageException& e) {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound &&
            e.ErrorCode == "ContainerNotFound") {
            return false;
        }
        throw;
    }
}

std::vector<std::string>
AzureBlobChunkManager::ListBuckets() {
    Azure::Core::Context context;
    if (requestTimeoutMs_ > 0) {
        context =
            context.WithDeadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(requestTimeoutMs_));
    }
    std::vector<std::string> buckets;
    for (auto containerPage = client_->ListBlobContainers(
             Azure::Storage::Blobs::ListBlobContainersOptions(), context);
         containerPage.HasPage();
         containerPage.MoveToNextPage()) {
        for (auto& container : containerPage.BlobContainers) {
            buckets.emplace_back(container.Name);
        }
    }
    return buckets;
}

bool
AzureBlobChunkManager::CreateBucket(const std::string& bucket_name) {
    Azure::Core::Context context;
    if (requestTimeoutMs_ > 0) {
        context =
            context.WithDeadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(requestTimeoutMs_));
    }
    return client_->GetBlobContainerClient(bucket_name)
        .CreateIfNotExists(Azure::Storage::Blobs::CreateBlobContainerOptions(),
                           context)
        .Value.Created;
}

bool
AzureBlobChunkManager::DeleteBucket(const std::string& bucket_name) {
    Azure::Core::Context context;
    if (requestTimeoutMs_ > 0) {
        context =
            context.WithDeadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(requestTimeoutMs_));
    }
    return client_->GetBlobContainerClient(bucket_name)
        .DeleteIfExists(Azure::Storage::Blobs::DeleteBlobContainerOptions(),
                        context)
        .Value.Deleted;
}

bool
AzureBlobChunkManager::ObjectExists(const std::string& bucket_name,
                                    const std::string& object_name) {
    try {
        Azure::Core::Context context;
        if (requestTimeoutMs_ > 0) {
            context = context.WithDeadline(
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(requestTimeoutMs_));
        }
        client_->GetBlobContainerClient(bucket_name)
            .GetBlockBlobClient(object_name)
            .GetProperties(Azure::Storage::Blobs::GetBlobPropertiesOptions(),
                           context);
        return true;
    } catch (const Azure::Storage::StorageException& e) {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
            return false;
        }
        throw;
    }
}

int64_t
AzureBlobChunkManager::GetObjectSize(const std::string& bucket_name,
                                     const std::string& object_name) {
    Azure::Core::Context context;
    if (requestTimeoutMs_ > 0) {
        context =
            context.WithDeadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(requestTimeoutMs_));
    }
    return client_->GetBlobContainerClient(bucket_name)
        .GetBlockBlobClient(object_name)
        .GetProperties(Azure::Storage::Blobs::GetBlobPropertiesOptions(),
                       context)
        .Value.BlobSize;
}

bool
AzureBlobChunkManager::DeleteObject(const std::string& bucket_name,
                                    const std::string& object_name) {
    Azure::Core::Context context;
    if (requestTimeoutMs_ > 0) {
        context =
            context.WithDeadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(requestTimeoutMs_));
    }
    return client_->GetBlobContainerClient(bucket_name)
        .GetBlockBlobClient(object_name)
        .DeleteIfExists(Azure::Storage::Blobs::DeleteBlobOptions(), context)
        .Value.Deleted;
}

bool
AzureBlobChunkManager::PutObjectBuffer(const std::string& bucket_name,
                                       const std::string& object_name,
                                       void* buf,
                                       uint64_t size) {
    std::vector<unsigned char> str(static_cast<char*>(buf),
                                   static_cast<char*>(buf) + size);
    Azure::Core::Context context;
    if (requestTimeoutMs_ > 0) {
        context =
            context.WithDeadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(requestTimeoutMs_));
    }
    client_->GetBlobContainerClient(bucket_name)
        .GetBlockBlobClient(object_name)
        .UploadFrom(str.data(),
                    str.size(),
                    Azure::Storage::Blobs::UploadBlockBlobFromOptions(),
                    context);
    return true;
}

uint64_t
AzureBlobChunkManager::GetObjectBuffer(const std::string& bucket_name,
                                       const std::string& object_name,
                                       void* buf,
                                       uint64_t size) {
    Azure::Storage::Blobs::DownloadBlobOptions downloadOptions;
    downloadOptions.Range = Azure::Core::Http::HttpRange();
    downloadOptions.Range.Value().Offset = 0;
    downloadOptions.Range.Value().Length = size;
    Azure::Core::Context context;
    if (requestTimeoutMs_ > 0) {
        context =
            context.WithDeadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(requestTimeoutMs_));
    }
    auto downloadResponse = client_->GetBlobContainerClient(bucket_name)
                                .GetBlockBlobClient(object_name)
                                .Download(downloadOptions, context);
    auto bodyStream = downloadResponse.Value.BodyStream.get();
    uint64_t totalBytesRead = 0;
    uint64_t bytesRead = 0;
    do {
        bytesRead = bodyStream->Read(
            static_cast<uint8_t*>(buf) + totalBytesRead, size - totalBytesRead);
        totalBytesRead += bytesRead;
    } while (bytesRead != 0 && totalBytesRead < size);
    return totalBytesRead;
}

std::vector<std::string>
AzureBlobChunkManager::ListObjects(const std::string& bucket_name,
                                   const std::string& prefix) {
    std::vector<std::string> objects_vec;
    Azure::Storage::Blobs::ListBlobsOptions listOptions;
    listOptions.Prefix = prefix;
    Azure::Core::Context context;
    if (requestTimeoutMs_ > 0) {
        context =
            context.WithDeadline(std::chrono::system_clock::now() +
                                 std::chrono::milliseconds(requestTimeoutMs_));
    }
    for (auto blobPage = client_->GetBlobContainerClient(bucket_name)
                             .ListBlobs(listOptions, context);
         blobPage.HasPage();
         blobPage.MoveToNextPage()) {
        for (auto& blob : blobPage.Blobs) {
            objects_vec.emplace_back(blob.Name);
        }
    }
    return objects_vec;
}

}  // namespace azure
