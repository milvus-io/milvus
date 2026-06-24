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

#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "knowhere/expected.h"

// KnowhereStatusToErrorCode bridges knowhere's status category (added in
// zilliztech/knowhere#1675) into the segcore ErrorCode space so input errors
// stay non-retriable and engine/internal/transient errors stay retriable.
TEST(KnowhereStatusMapping, InputErrorsMapToInvalidParameter) {
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(knowhere::Status::invalid_args),
              milvus::ErrorCode::InvalidParameter);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::invalid_metric_type),
              milvus::ErrorCode::InvalidParameter);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::invalid_param_in_json),
              milvus::ErrorCode::InvalidParameter);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::out_of_range_in_json),
              milvus::ErrorCode::InvalidParameter);
}

TEST(KnowhereStatusMapping, InnerErrorsMapToKnowhereError) {
    EXPECT_EQ(
        milvus::KnowhereStatusToErrorCode(knowhere::Status::faiss_inner_error),
        milvus::ErrorCode::KnowhereError);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::diskann_inner_error),
              milvus::ErrorCode::KnowhereError);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(knowhere::Status::malloc_error),
              milvus::ErrorCode::KnowhereError);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(knowhere::Status::timeout),
              milvus::ErrorCode::KnowhereError);
    // knowhere_inner_error is the catch-all for swallowed C++ exceptions at the
    // knowhere facade; it must be treated as a retriable system error, never as
    // user input.
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::knowhere_inner_error),
              milvus::ErrorCode::KnowhereError);
}
