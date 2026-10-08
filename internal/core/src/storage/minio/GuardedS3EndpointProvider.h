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

#include <aws/core/client/CoreErrors.h>
#include <aws/core/utils/Outcome.h>
#include <aws/s3/S3Client.h>

#include <memory>
#include <utility>

namespace milvus::storage {

// Decorate the client's already initialized provider so region discovery,
// path-style addressing, ARN handling and provider-specific settings stay intact.
// Install before publishing the client or issuing its first request.
class GuardedS3EndpointProvider final
    : public Aws::S3::Endpoint::S3EndpointProviderBase {
 public:
    explicit GuardedS3EndpointProvider(
        std::shared_ptr<Aws::S3::Endpoint::S3EndpointProviderBase> delegate)
        : delegate_(std::move(delegate)) {
    }

    void
    InitBuiltInParameters(
        const Aws::S3::S3ClientConfiguration& config) override {
        delegate_->InitBuiltInParameters(config);
    }

    void
    InitBuiltInParameters(const Aws::S3::S3ClientConfiguration& config,
                          const Aws::String& service_name) override {
        delegate_->InitBuiltInParameters(config, service_name);
    }

    void
    OverrideEndpoint(const Aws::String& endpoint) override {
        delegate_->OverrideEndpoint(endpoint);
    }

    Aws::S3::Endpoint::S3ClientContextParameters&
    AccessClientContextParameters() override {
        return delegate_->AccessClientContextParameters();
    }

    const Aws::S3::Endpoint::S3ClientContextParameters&
    GetClientContextParameters() const override {
        return delegate_->GetClientContextParameters();
    }

    Aws::Endpoint::ResolveEndpointOutcome
    ResolveEndpoint(
        const Aws::Endpoint::EndpointParameters& parameters) const override {
        auto outcome = delegate_->ResolveEndpoint(parameters);
        if (!outcome.IsSuccess()) {
            return outcome;
        }
        const auto& attributes = outcome.GetResult().GetAttributes();
        // The SDK can return a successful endpoint with an empty auth scheme
        // after failing to parse its properties JSON. AWSClient then replaces
        // its default signer with this empty name and dereferences a null signer
        // (or asserts in Debug). Reject it before the request reaches signing.
        // No attributes means use the SDK's default signer; an explicit
        // NullSigner is also distinct from a missing name. Preserve both.
        if (attributes && attributes->authScheme.GetName().empty()) {
            return Aws::Client::AWSError<Aws::Client::CoreErrors>(
                Aws::Client::CoreErrors::ENDPOINT_RESOLUTION_FAILURE,
                "InvalidS3EndpointAuthScheme",
                "S3 endpoint authentication resolved to an empty signer; "
                "check the effective region and endpoint configuration "
                "(minio.region, minio.address)",
                false);
        }
        return outcome;
    }

 private:
    // Non-null: InstallS3EndpointAuthGuard leaves a missing provider to the
    // SDK's existing ENDPOINT_RESOLUTION_FAILURE handling.
    std::shared_ptr<Aws::S3::Endpoint::S3EndpointProviderBase> delegate_;
};

inline void
InstallS3EndpointAuthGuard(Aws::S3::S3Client& client) {
    auto& provider = client.accessEndpointProvider();
    if (provider &&
        !std::dynamic_pointer_cast<GuardedS3EndpointProvider>(provider)) {
        provider = std::make_shared<GuardedS3EndpointProvider>(provider);
    }
}

}  // namespace milvus::storage
