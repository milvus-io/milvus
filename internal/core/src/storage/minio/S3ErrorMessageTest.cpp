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

#include <gtest/gtest.h>
#include <string>

#include "storage/minio/MinioChunkManager.h"

using namespace milvus::storage;

// These formatting tests do not create a client or require a storage service.
TEST(S3ErrorMessageTest, SDKRequestId) {
    Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
        Aws::S3::S3Errors::ACCESS_DENIED, false));
    error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
    error.SetExceptionName("AccessDenied");
    error.SetMessage("No response body.");
    error.SetRequestId("sdk-request-id");
    error.SetResponseHeaders({{"x-cos-request-id", "header-request-id"}});

    EXPECT_EQ(
        S3ErrorMessage("GetObjectSize", error, "bucket={}", "test-bucket"),
        "Error in GetObjectSize[errcode:403, exception:AccessDenied, "
        "errmessage:No response body., request_id:sdk-request-id, "
        "params:bucket=test-bucket]");
}

TEST(S3ErrorMessageTest, RequestIdFromResponseHeaders) {
    for (const auto& header : {"x-amz-request-id",
                               "x-cos-request-id",
                               "x-oss-request-id",
                               "x-obs-request-id",
                               "X-Cos-Request-Id"}) {
        SCOPED_TRACE(header);
        Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
            Aws::S3::S3Errors::ACCESS_DENIED, false));
        error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
        error.SetMessage("No response body.");
        error.SetResponseHeaders({{header, "server-request-id"},
                                  {"authorization", "must-not-be-logged"}});
        const auto message = S3ErrorMessage("GetObjectSize", error, "params");
        EXPECT_NE(message.find("request_id:server-request-id"),
                  std::string::npos);
        EXPECT_EQ(message.find("must-not-be-logged"), std::string::npos);
    }
}

TEST(S3ErrorMessageTest, MissingRequestId) {
    Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
        Aws::S3::S3Errors::ACCESS_DENIED, false));
    error.SetResponseCode(Aws::Http::HttpResponseCode::FORBIDDEN);
    error.SetMessage("No response body.");
    error.SetResponseHeaders(
        {{"x-cos-request-id", ""}, {"content-type", "application/xml"}});
    EXPECT_EQ(S3ErrorMessage("GetObjectSize", error, "params"),
              "Error in GetObjectSize[errcode:403, exception:, "
              "errmessage:No response body., params:params]");
}

TEST(S3ErrorMessageTest, ClientErrorWithoutResponse) {
    Aws::S3::S3Error error(Aws::Client::AWSError<Aws::S3::S3Errors>(
        Aws::S3::S3Errors::NETWORK_CONNECTION, true));
    error.SetMessage("Connection timed out");
    const auto message = S3ErrorMessage("GetObjectBuffer", error, "params");
    EXPECT_NE(message.find("Connection timed out"), std::string::npos);
    EXPECT_EQ(message.find("request_id:"), std::string::npos);
}
