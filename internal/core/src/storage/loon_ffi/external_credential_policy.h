// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace milvus::storage {

// Process-wide policy for the credentials an external collection may use.
// Mirrors common.security.externalCollection.allowServerIdentity and
// common.security.externalCollection.credentialBrokerEndpoints, both read at
// process start and published once through SetExternalCollectionCredentialPolicy.
//
// Enforced by InjectExternalSpecProperties, so it applies to every external
// filesystem this process opens: segcore loads, index builds and the Go FFI
// explore path alike, including specs persisted before the settings changed.
struct ExternalCredentialPolicy {
    // False until the owning process published its configuration. A spec
    // that needs a policy decision is rejected while this is false, so a
    // process that forgot to publish fails closed instead of using the
    // defaults of whichever binary it happens to be.
    bool initialized = false;
    // Whether specs may read with this cluster's own cloud identity
    // (use_iam=true, role_arn, or gcp_target_service_account without a
    // credential Broker).
    bool allow_server_identity = false;
    // Credential Broker URLs a spec may name in credential_endpoint. Stored
    // normalized (surrounding whitespace and trailing '/' removed) and
    // compared exactly.
    std::vector<std::string> broker_endpoints;
};

// Trims surrounding whitespace and trailing '/' so that a spec value and an
// allowlist entry that differ only in those compare equal.
std::string
NormalizeBrokerEndpoint(const std::string& endpoint);

class ExternalCredentialPolicyHolder {
 private:
    ExternalCredentialPolicyHolder() = default;

 public:
    // Defined out of line (util.cpp) so every image in the process shares
    // the one instance that lives in the core library.
    static ExternalCredentialPolicyHolder&
    GetInstance();

    // Publishes the configuration and marks the policy initialized.
    void
    Set(bool allow_server_identity, std::vector<std::string> broker_endpoints) {
        std::lock_guard<std::mutex> lock(mutex_);
        policy_.initialized = true;
        policy_.allow_server_identity = allow_server_identity;
        policy_.broker_endpoints.clear();
        for (auto& endpoint : broker_endpoints) {
            auto normalized = NormalizeBrokerEndpoint(endpoint);
            if (!normalized.empty()) {
                policy_.broker_endpoints.push_back(std::move(normalized));
            }
        }
    }

    ExternalCredentialPolicy
    Get() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return policy_;
    }

    // Reinstates a snapshot taken with Get(), including its initialized
    // flag. Production code only ever calls Set(); tests use this to leave
    // the process-wide state as they found it.
    void
    Restore(const ExternalCredentialPolicy& policy) {
        std::lock_guard<std::mutex> lock(mutex_);
        policy_ = policy;
    }

 private:
    mutable std::mutex mutex_;
    ExternalCredentialPolicy policy_{};
};

}  // namespace milvus::storage
