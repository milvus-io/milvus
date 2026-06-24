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

// A few statuses knowhere classifies as input_error are really capability / data
// errors, not malformed caller input, and are pulled out before the IsInputError
// check so they do not get blamed on the user's request.
TEST(KnowhereStatusMapping, CapabilityAndDataErrorsAreNotInputErrors) {
    // Feature / CPU capability gaps (e.g. SCANN needs AVX2) -> Unsupported.
    EXPECT_EQ(
        milvus::KnowhereStatusToErrorCode(knowhere::Status::not_implemented),
        milvus::ErrorCode::Unsupported);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::invalid_instruction_set),
              milvus::ErrorCode::Unsupported);
    // Incompatible-version / corrupt serialized index -> DataFormatBroken.
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::invalid_serialized_index_type),
              milvus::ErrorCode::DataFormatBroken);
}

// Engine/internal failures with no retry value map to the permanent
// KnowhereError(2099), including knowhere_inner_error -- the catch-all for
// swallowed C++ exceptions at the knowhere facade.
TEST(KnowhereStatusMapping, PermanentInnerErrorsMapToKnowhereError) {
    EXPECT_EQ(
        milvus::KnowhereStatusToErrorCode(knowhere::Status::faiss_inner_error),
        milvus::ErrorCode::KnowhereError);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::diskann_inner_error),
              milvus::ErrorCode::KnowhereError);
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(
                  knowhere::Status::knowhere_inner_error),
              milvus::ErrorCode::KnowhereError);
    // timeout is Cardinal-only (BuildAsync cancel-or-timeout) and conflates
    // cancellation with timeout, so it stays a permanent system error rather
    // than mapping to a retriable code.
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(knowhere::Status::timeout),
              milvus::ErrorCode::KnowhereError);
}

// Transient failures must map to retriable segcore codes so a retry / reroute
// to another replica can succeed, instead of being lumped into the permanent
// KnowhereError. knowhere keeps these in its inner_error category, so the
// segcore mapper picks them out by status value.
TEST(KnowhereStatusMapping, TransientErrorsMapToRetriableCodes) {
    EXPECT_EQ(milvus::KnowhereStatusToErrorCode(knowhere::Status::malloc_error),
              milvus::ErrorCode::MemAllocateFailed);
    EXPECT_EQ(
        milvus::KnowhereStatusToErrorCode(knowhere::Status::disk_file_error),
        milvus::ErrorCode::FileReadFailed);
}
