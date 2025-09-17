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

#include "AzureBlobChunkManager.h"
#include <azure/identity/workload_identity_credential.hpp>
#include <gtest/gtest.h>

using namespace azure;

void
SetTenantId(const char* value) {
    setenv("AZURE_TENANT_ID", value, 1);
}
void
SetClientId(const char* value) {
    setenv("AZURE_CLIENT_ID", value, 1);
}
void
SetTokenFilePath(const char* value) {
    setenv("AZURE_FEDERATED_TOKEN_FILE", value, 1);
}

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

class AzureBlobChunkManagerTest : public testing::Test {
 protected:
    void
    SetUp() override {
    }

    // void TearDown() override {}
};

TEST(AzureBlobChunkManagerTest, Options) {
    SetTenantId("tenant_id");
    SetClientId("client_id");
    SetTokenFilePath("token_file_path");

    Azure::Identity::WorkloadIdentityCredentialOptions options;
    Azure::Identity::WorkloadIdentityCredential const cred(options);
    EXPECT_EQ(cred.GetCredentialName(), "WorkloadIdentityCredential");

    EXPECT_EQ(options.TenantId, GetTenantId());
    EXPECT_EQ(options.TenantId, "tenant_id");
    EXPECT_EQ(options.ClientId, GetClientId());
    EXPECT_EQ(options.AuthorityHost, "https://login.microsoftonline.com/");
    EXPECT_EQ(options.TokenFilePath, GetTokenFilePath());
}
