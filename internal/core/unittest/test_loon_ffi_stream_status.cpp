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

#include <gtest/gtest.h>

#include <arrow/status.h>
#include <cerrno>

#include "milvus-storage/common/extend_status.h"
#include "storage/loon_ffi/ffi_reader_stream.h"

namespace {

constexpr milvus_storage::ExtendStatusCode kRetryableCodes[] = {
    milvus_storage::ExtendStatusCode::StorageTransientNetwork,
    milvus_storage::ExtendStatusCode::StorageTransientTimeout,
    milvus_storage::ExtendStatusCode::StorageTransientThrottling,
    milvus_storage::ExtendStatusCode::StorageTransientService,
    // The SDK exhausted the multipart-upload retries, but an outer retry can
    // start a fresh upload and succeed.
    milvus_storage::ExtendStatusCode::AwsErrorNoSuchUpload,
};

constexpr milvus_storage::ExtendStatusCode kPermanentCodes[] = {
    milvus_storage::ExtendStatusCode::PackedFileCorrupted,
    milvus_storage::ExtendStatusCode::PackedMetadataCorrupted,
    milvus_storage::ExtendStatusCode::AwsErrorNotFound,
    milvus_storage::ExtendStatusCode::AwsErrorAccessDenied,
    milvus_storage::ExtendStatusCode::AwsErrorNonRetryable,
    milvus_storage::ExtendStatusCode::AwsErrorConflict,
    milvus_storage::ExtendStatusCode::AwsErrorPreConditionFailed,
    milvus_storage::ExtendStatusCode::TxnExhaustedRetry,
};

}  // namespace

// The Arrow C data interface only carries (errno, message). GetFFIReaderStream
// must encode the producer's retryable classification in the errno, otherwise
// Arrow Go rebuilds a transient throttle as a plain EIO and the import layer
// treats it as permanent.
TEST(LoonFFIStreamStatus, RetryableStatusesMapToEAGAIN) {
    for (auto code : kRetryableCodes) {
        auto status =
            milvus_storage::MakeExtendError(code, "transient failure");
        EXPECT_EQ(milvus::storage::StreamErrnoForStatus(status), EAGAIN)
            << status.ToString();
    }
}

TEST(LoonFFIStreamStatus, PermanentStatusesMapToEIO) {
    for (auto code : kPermanentCodes) {
        auto status =
            milvus_storage::MakeExtendError(code, "permanent failure");
        EXPECT_EQ(milvus::storage::StreamErrnoForStatus(status), EIO)
            << status.ToString();
    }

    // A plain Arrow IO error has no producer detail and is permanent.
    EXPECT_EQ(milvus::storage::StreamErrnoForStatus(
                  arrow::Status::IOError("plain io error")),
              EIO);
}

// For statuses without a producer ExtendStatusDetail, the mapping must match
// arrow::ExportRecordBatchReader's ToCError so replacing it does not silently
// change errno semantics.
TEST(LoonFFIStreamStatus, PlainArrowStatusesKeepArrowErrno) {
    EXPECT_EQ(
        milvus::storage::StreamErrnoForStatus(arrow::Status::IOError("io")),
        EIO);
    EXPECT_EQ(milvus::storage::StreamErrnoForStatus(
                  arrow::Status::NotImplemented("not implemented")),
              ENOSYS);
    EXPECT_EQ(milvus::storage::StreamErrnoForStatus(
                  arrow::Status::OutOfMemory("oom")),
              ENOMEM);
    EXPECT_EQ(milvus::storage::StreamErrnoForStatus(
                  arrow::Status::Invalid("invalid")),
              EINVAL);
}

TEST(LoonFFIStreamStatus, ExportRejectsNullReader) {
    struct ArrowArrayStream stream;
    auto status =
        milvus::storage::ExportStatusAwareRecordBatchReader(nullptr, &stream);
    EXPECT_FALSE(status.ok());
}

TEST(LoonFFIStreamStatus, ExportRejectsNullOutput) {
    auto status =
        milvus::storage::ExportStatusAwareRecordBatchReader(nullptr, nullptr);
    EXPECT_FALSE(status.ok());
}
