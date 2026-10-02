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
#include <string>

#include <gtest/gtest.h>

#include "common/EasyAssert.h"
#include "storage/storage_c.h"

TEST(PluginLoader, MissingLibraryProtectsConfigurationInOutputAndStatus) {
    const std::string canary = "plugin-path-secret-canary";
    const auto path = "/" + canary + "/missing-plugin.so";
    // ThrowInfo writes to stdout before the exception reaches the C boundary.
    // Check both that output and the status later logged by Go startup callers.
    testing::internal::CaptureStdout();
    const auto status = InitPluginLoader(path.c_str());
    const auto output = testing::internal::GetCapturedStdout();
    EXPECT_EQ(status.error_code, milvus::UnexpectedError);
    EXPECT_EQ(output.find(canary), std::string::npos);
    const std::string message = status.error_msg;
    EXPECT_EQ(message.find(canary), std::string::npos);
    EXPECT_NE(message.find("plugin"), std::string::npos);
    std::free(const_cast<char*>(status.error_msg));
}
