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

#include <cstdlib>
#include <memory>
#include <string>

#include "common/common_type_c.h"
#include "gtest/gtest.h"
#include "milvus-storage/common/extend_status.h"
#include "segcore/arrow_fs_c.h"
#include "test_utils/Constants.h"

namespace {

void
FreeStatusMessage(CStatus& status) {
    free(const_cast<char*>(status.error_msg));
    status.error_msg = nullptr;
}

}  // namespace

TEST(StorageStatus, ConvertsToCStatusWithoutLosingClassification) {
    auto transient = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::StorageTransientTimeout,
        "storage timeout",
        "request timed out");
    auto transient_error = milvus_storage::ToSegcoreError(transient);
    auto transient_status = milvus::FailureCStatus(&transient_error);
    EXPECT_EQ(transient_status.error_code,
              milvus::ErrorCode::StorageTransientError);
    EXPECT_NE(std::string(transient_status.error_msg).find("storage timeout"),
              std::string::npos);
    FreeStatusMessage(transient_status);

    auto permanent = milvus_storage::MakeExtendError(
        milvus_storage::ExtendStatusCode::AwsErrorAccessDenied,
        "access denied",
        "invalid credentials");
    auto permanent_error = milvus_storage::ToSegcoreError(permanent);
    auto permanent_status = milvus::FailureCStatus(&permanent_error);
    EXPECT_EQ(permanent_status.error_code, milvus::ErrorCode::StorageError);
    EXPECT_NE(std::string(permanent_status.error_msg).find("access denied"),
              std::string::npos);
    FreeStatusMessage(permanent_status);
}

TEST(ArrowFileSystem, InitAndClean) {
    CStorageConfig config = {};
    config.root_path = TestLocalPath.c_str();
    config.storage_type = "local";

    CStatus status = InitArrowFileSystem(config);
    EXPECT_EQ(status.error_code, 0);

    CleanArrowFileSystem();

    // Reinitialize so subsequent tests can use it
    status = InitArrowFileSystem(config);
    EXPECT_EQ(status.error_code, 0);
}
