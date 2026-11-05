// Copyright 2023 Zilliz
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

#include <arrow/status.h>
#include <cerrno>

#include "milvus-storage/common/extend_status.h"

namespace milvus::storage {

// The Arrow C data interface only carries (errno, message) across the stream
// boundary, so the producer's retryable classification has to travel in the
// errno. EAGAIN is the conventional "try again" errno; the Go side maps it
// back to packed.ErrLoonTransient. Everything else is permanent (EIO).
//
// Classification obligation: this helper only carries the producer's retryable
// bit. Every error that can reach the cgo boundary is classified in
// ffi_reader_c.cpp through milvus_storage::ToSegcoreError (open) and
// ArrowStatusToErrorCode (stream/export), which own the ErrorCode mapping.
constexpr int kRetryableStreamErrno = EAGAIN;

inline int
StreamErrnoForStatus(const arrow::Status& status) {
    // Keep the producer's retryable classification, which arrow's own
    // ExportRecordBatchReader drops. Everything else mirrors its ToCError:
    // IOError -> EIO, NotImplemented -> ENOSYS, OutOfMemory -> ENOMEM, and
    // the remaining codes -> EINVAL.
    auto detail = milvus_storage::ExtendStatusDetail::UnwrapStatus(status);
    if (detail != nullptr && detail->retryable()) {
        return kRetryableStreamErrno;
    }
    switch (status.code()) {
        case arrow::StatusCode::IOError:
            return EIO;
        case arrow::StatusCode::NotImplemented:
            return ENOSYS;
        case arrow::StatusCode::OutOfMemory:
            return ENOMEM;
        default:
            return EINVAL;
    }
}

}  // namespace milvus::storage
