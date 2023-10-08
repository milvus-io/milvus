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

AzureBlobChunkManager::AzureBlobChunkManager(
    const std::string& access_key_id,
    const std::string& access_key_value,
    const std::string& address,
    bool useIAM) {
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
    std::vector<std::string> buckets;
    for (auto containerPage = client_->ListBlobContainers();
         containerPage.HasPage();
         containerPage.MoveToNextPage()) {
        for (auto& container : containerPage.BlobContainers) {
            if (container.Name == bucket_name) {
                return true;
            }
        }
    }
    return false;
}

std::vector<std::string>
AzureBlobChunkManager::ListBuckets() {
    std::vector<std::string> buckets;
    for (auto containerPage = client_->ListBlobContainers();
         containerPage.HasPage();
         containerPage.MoveToNextPage()) {
        for (auto& container : containerPage.BlobContainers) {
            buckets.emplace_back(container.Name);
        }
    }
    return buckets;
}

void
AzureBlobChunkManager::CreateBucket(const std::string& bucket_name) {
    client_->GetBlobContainerClient(bucket_name).Create();
}

void
AzureBlobChunkManager::DeleteBucket(const std::string& bucket_name) {
    client_->GetBlobContainerClient(bucket_name).Delete();
}

bool
AzureBlobChunkManager::ObjectExists(const std::string& bucket_name,
                                    const std::string& object_name) {
    for (auto blobPage =
             client_->GetBlobContainerClient(bucket_name).ListBlobs();
         blobPage.HasPage();
         blobPage.MoveToNextPage()) {
        for (auto& blob : blobPage.Blobs) {
            if (blob.Name == object_name) {
                return true;
            }
        }
    }
    return false;
}

int64_t
AzureBlobChunkManager::GetObjectSize(const std::string& bucket_name,
                                     const std::string& object_name) {
    for (auto blobPage =
             client_->GetBlobContainerClient(bucket_name).ListBlobs();
         blobPage.HasPage();
         blobPage.MoveToNextPage()) {
        for (auto& blob : blobPage.Blobs) {
            if (blob.Name == object_name) {
                return blob.BlobSize;
            }
        }
    }
    std::stringstream err_msg;
    err_msg << "object('" << bucket_name << "', '" << object_name
            << "') not exists";
    throw std::runtime_error(err_msg.str());
}

void
AzureBlobChunkManager::DeleteObject(const std::string& bucket_name,
                                    const std::string& object_name) {
    client_->GetBlobContainerClient(bucket_name)
        .GetBlockBlobClient(object_name)
        .Delete();
}

bool
AzureBlobChunkManager::PutObjectBuffer(const std::string& bucket_name,
                                       const std::string& object_name,
                                       void* buf,
                                       uint64_t size) {
    std::vector<unsigned char> str(static_cast<char*>(buf),
                                   static_cast<char*>(buf) + size);
    client_->GetBlobContainerClient(bucket_name)
        .GetBlockBlobClient(object_name)
        .UploadFrom(str.data(), str.size());
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
    auto downloadResponse = client_->GetBlobContainerClient(bucket_name)
                                .GetBlockBlobClient(object_name)
                                .Download(downloadOptions);
    std::vector<unsigned char> str =
        downloadResponse.Value.BodyStream->ReadToEnd();
    memcpy(static_cast<char*>(buf), &str[0], str.size() * sizeof(str[0]));
    return str.size();
}

std::vector<std::string>
AzureBlobChunkManager::ListObjects(const char* bucket_name,
                                   const char* prefix) {
    std::vector<std::string> objects_vec;
    for (auto blobPage =
             client_->GetBlobContainerClient(bucket_name).ListBlobs();
         blobPage.HasPage();
         blobPage.MoveToNextPage()) {
        for (auto& blob : blobPage.Blobs) {
            if (blob.Name.rfind(prefix, 0) == 0) {
                objects_vec.emplace_back(blob.Name);
            }
        }
    }
    return objects_vec;
}

}  // namespace azure

int
main() {
    const char* containerName = "default";
    const char* blobName = "sample-blob";
    using namespace azure;
    AzureBlobChunkManager chunkManager = AzureBlobChunkManager("", "", "");
    std::vector<std::string> buckets = chunkManager.ListBuckets();
    for (const auto& bucket : buckets) {
        std::cout << bucket << std::endl;
    }
    std::vector<std::string> objects =
        chunkManager.ListObjects(containerName, blobName);
    for (const auto& object : objects) {
        std::cout << object << std::endl;
    }
    std::cout << chunkManager.GetObjectSize(containerName, blobName)
              << std::endl;
    std::cout << chunkManager.ObjectExists(containerName, blobName)
              << std::endl;
    std::cout << chunkManager.ObjectExists(containerName, "blobName")
              << std::endl;
    std::cout << chunkManager.BucketExists(containerName) << std::endl;
    char buffer[1024 * 1024];
    chunkManager.GetObjectBuffer(containerName, blobName, buffer, 1024 * 1024);
    std::cout << buffer << std::endl;

    char msg[12];
    memcpy(msg, "Azure hello!", 12);
    if (!chunkManager.ObjectExists(containerName, "blobName")) {
        chunkManager.PutObjectBuffer(containerName, "blobName", msg, 12);
    }
    char buffer0[1024 * 1024];
    chunkManager.GetObjectBuffer(
        containerName, "blobName", buffer0, 1024 * 1024);
    std::cout << buffer0 << std::endl;
    chunkManager.DeleteObject(containerName, "blobName");
    chunkManager.CreateBucket("sample-container1");
    chunkManager.DeleteBucket("sample-container1");
    exit(EXIT_SUCCESS);
}