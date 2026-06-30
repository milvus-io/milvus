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
#include <memory>

#include "arrow/status.h"
#include "common/EasyAssert.h"
#include "storage/BinlogReader.h"
#include "storage/Util.h"

// A truncated / corrupt binlog must surface as DataFormatBroken (a permanent
// data error) rather than the generic UnexpectedError, so the Go classifier
// does not retry it. Covers all three BinlogReader read entry points.
TEST(StorageErrorCode, BinlogReaderOutOfRangeIsDataFormatBroken) {
    auto data = std::shared_ptr<uint8_t[]>(new uint8_t[4]);

    milvus::storage::BinlogReader reader(data, 4);
    uint8_t out[16];
    auto err = reader.Read(16, out);
    EXPECT_FALSE(err.ok());
    EXPECT_EQ(err.get_error_code(), milvus::ErrorCode::DataFormatBroken);

    milvus::storage::BinlogReader reader_single(data, 4);
    int64_t value = 0;  // needs 8 bytes, only 4 available
    auto err_single = reader_single.ReadSingleValue<int64_t>(value);
    EXPECT_FALSE(err_single.ok());
    EXPECT_EQ(err_single.get_error_code(), milvus::ErrorCode::DataFormatBroken);

    milvus::storage::BinlogReader reader_pair(data, 4);
    auto [err_pair, buf] = reader_pair.Read(16);
    EXPECT_FALSE(err_pair.ok());
    EXPECT_EQ(err_pair.get_error_code(), milvus::ErrorCode::DataFormatBroken);
}

// ArrowStatusToErrorCode delegates to the producer's classification
// (milvus_storage::ToSegcoreError). For a plain arrow status with no structured
// ExtendStatusDetail it falls back to a coarse mapping; these are the categories
// whose retry policy the Go classifier honors.
TEST(StorageErrorCode, ArrowStatusToErrorCodeMapping) {
    using milvus::storage::ArrowStatusToErrorCode;
    EXPECT_EQ(ArrowStatusToErrorCode(arrow::Status::OutOfMemory("oom")),
              milvus::ErrorCode::MemAllocateFailed);  // retriable
    EXPECT_EQ(ArrowStatusToErrorCode(arrow::Status::IOError("io")),
              milvus::ErrorCode::StorageTransientError);  // 2045, retriable
    EXPECT_EQ(ArrowStatusToErrorCode(arrow::Status::Invalid("bad")),
              milvus::ErrorCode::DataFormatBroken);  // permanent data error
    EXPECT_EQ(ArrowStatusToErrorCode(arrow::Status::TypeError("type")),
              milvus::ErrorCode::DataFormatBroken);
    EXPECT_EQ(ArrowStatusToErrorCode(arrow::Status::UnknownError("?")),
              milvus::ErrorCode::StorageError);  // 2044, permanent internal
}

// When the producer attaches a structured ExtendStatusDetail, the fine code
// survives: a transient PackedStorageIO must classify as the RETRIABLE
// StorageTransientError(2045), never the non-retriable StorageError(2044).
TEST(StorageErrorCode, ArrowStatusWithExtendDetailPreservesFineCode) {
    using milvus::storage::ArrowStatusToErrorCode;
    EXPECT_EQ(ArrowStatusToErrorCode(milvus_storage::MakeExtendError(
                  milvus_storage::ExtendStatusCode::PackedStorageIO, "io")),
              milvus::ErrorCode::StorageTransientError);
    EXPECT_EQ(ArrowStatusToErrorCode(milvus_storage::MakeExtendError(
                  milvus_storage::ExtendStatusCode::PackedFileCorrupted, "bad")),
              milvus::ErrorCode::DataFormatBroken);
    EXPECT_EQ(ArrowStatusToErrorCode(milvus_storage::MakeExtendError(
                  milvus_storage::ExtendStatusCode::PackedInvalidArgs, "args")),
              milvus::ErrorCode::InvalidParameter);
}
