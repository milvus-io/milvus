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

#include <cstdlib>
#include <filesystem>
#include "google/cloud/storage/retry_policy.h"
#include "GcpNativeClientManager.h"

namespace gcs = google::cloud::storage;

namespace gcpnative {

GcpNativeClientManager::GcpNativeClientManager(
    const std::string& address,
    const std::string& gcp_credential_json,
    bool use_ssl,
    bool gcp_native_without_auth,
    int64_t request_timeout_ms) {
    auto options = google::cloud::Options{};
    if (gcp_native_without_auth) {
        // Create an anonymous credential for unit testing with GCS emulation.
        auto credentials =
            google::cloud::storage::oauth2::CreateAnonymousCredentials();
        options.set<gcs::Oauth2CredentialsOption>(std::move(credentials));
    } else {
        if (gcp_credential_json.empty()) {
            throw std::runtime_error(
                "GCP native error: GCS service account credentials are "
                "missing.");
        }
        auto credentials =
            google::cloud::MakeServiceAccountCredentials(gcp_credential_json);
        options.set<google::cloud::UnifiedCredentialsOption>(credentials);
    }
    if (request_timeout_ms > 0) {
        options.set<gcs::RetryPolicyOption>(
            gcs::LimitedTimeRetryPolicy(
                std::chrono::milliseconds(request_timeout_ms))
                .clone());
    }
    if (!address.empty()) {
        std::string complete_address = "http://";
        if (use_ssl) {
            complete_address = "https://";
        }
        complete_address += address;
        options.set<gcs::RestEndpointOption>(complete_address);
    }

    client_ =
        std::make_unique<google::cloud::storage::Client>(std::move(options));
}

GcpNativeClientManager::~GcpNativeClientManager() {
}

bool
GcpNativeClientManager::BucketExists(const std::string& bucket_name) {
    auto metadata = client_->GetBucketMetadata(bucket_name);
    if (!metadata) {
        if (metadata.status().code() == google::cloud::StatusCode::kNotFound) {
            return false;
        } else {
            throw std::runtime_error(
                GetGcpNativeError(std::move(metadata).status()));
        }
    }
    return true;
}

std::vector<std::string>
GcpNativeClientManager::ListBuckets() {
    std::vector<std::string> buckets;
    for (auto&& metadata : client_->ListBuckets()) {
        if (!metadata) {
            throw std::runtime_error(
                GetGcpNativeError(std::move(metadata).status()));
        }

        buckets.emplace_back(metadata->name());
    }
    return buckets;
}

bool
GcpNativeClientManager::CreateBucket(const std::string& bucket_name) {
    if (BucketExists(bucket_name)) {
        return false;
    }
    auto metadata = client_->CreateBucket(bucket_name, gcs::BucketMetadata());
    if (!metadata) {
        throw std::runtime_error(
            GetGcpNativeError(std::move(metadata).status()));
    }
    return true;
}

bool
GcpNativeClientManager::DeleteBucket(const std::string& bucket_name) {
    auto status = client_->DeleteBucket(bucket_name);
    if (!status.ok()) {
        if (status.code() == google::cloud::StatusCode::kNotFound) {
            return false;
        } else {
            throw std::runtime_error(GetGcpNativeError(std::move(status)));
        }
    }
    return true;
}

bool
GcpNativeClientManager::ObjectExists(const std::string& bucket_name,
                                     const std::string& object_name) {
    auto metadata = client_->GetObjectMetadata(bucket_name, object_name);
    if (!metadata) {
        if (metadata.status().code() == google::cloud::StatusCode::kNotFound) {
            return false;
        } else {
            throw std::runtime_error(
                GetGcpNativeError(std::move(metadata).status()));
        }
    }
    return true;
}

int64_t
GcpNativeClientManager::GetObjectSize(const std::string& bucket_name,
                                      const std::string& object_name) {
    auto metadata = client_->GetObjectMetadata(bucket_name, object_name);
    if (!metadata) {
        throw std::runtime_error(
            GetGcpNativeError(std::move(metadata).status()));
    }
    return metadata->size();
}

bool
GcpNativeClientManager::DeleteObject(const std::string& bucket_name,
                                     const std::string& object_name) {
    google::cloud::Status status =
        client_->DeleteObject(bucket_name, object_name);
    if (!status.ok()) {
        if (status.code() == google::cloud::StatusCode::kNotFound) {
            return false;
        } else {
            throw std::runtime_error(GetGcpNativeError(std::move(status)));
        }
    }
    return true;
}

bool
GcpNativeClientManager::PutObjectBuffer(const std::string& bucket_name,
                                        const std::string& object_name,
                                        void* buf,
                                        uint64_t size) {
    std::string buffer(reinterpret_cast<char*>(buf), size);
    auto stream = client_->WriteObject(bucket_name, object_name);
    stream << buffer;
    stream.Close();
    if (stream.bad()) {
        throw std::runtime_error(GetGcpNativeError(stream.metadata().status()));
    }
    return true;
}

uint64_t
GcpNativeClientManager::GetObjectBuffer(const std::string& bucket_name,
                                        const std::string& object_name,
                                        void* buf,
                                        uint64_t size) {
    auto stream = client_->ReadObject(bucket_name, object_name);
    if (stream.bad()) {
        throw std::runtime_error(GetGcpNativeError(stream.status()));
    }

    stream.read(reinterpret_cast<char*>(buf), size);
    auto bytes_read = stream.gcount();
    stream.Close();

    return bytes_read;
}

std::vector<std::string>
GcpNativeClientManager::ListObjects(const std::string& bucket_name,
                                    const std::string& prefix) {
    std::vector<std::string> objects_vec;
    for (auto&& metadata :
         client_->ListObjects(bucket_name, gcs::Prefix(prefix))) {
        if (!metadata) {
            throw std::runtime_error(
                GetGcpNativeError(std::move(metadata).status()));
        }
        objects_vec.emplace_back(metadata->name());
    }

    return objects_vec;
}

}  // namespace gcpnative
