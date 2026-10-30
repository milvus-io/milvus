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

#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>

#include <cstring>
#include <memory>
#include <new>
#include <string>
#include <utility>

#include "storage/loon_ffi/ffi_reader_status.h"

namespace milvus::storage {
namespace detail {

// Arrow Go rebuilds stream errors from (errno, message), so the producer's
// RetryableStatusDetail would be lost at this boundary. Encode it in the errno
// (EAGAIN = retryable, EIO = permanent) and keep the message for
// get_last_error.
struct StatusAwareArrayStream {
    std::shared_ptr<arrow::RecordBatchReader> reader;
    std::string last_error;
};

inline int
StatusAwareGetSchema(struct ArrowArrayStream* stream, struct ArrowSchema* out) {
    auto* state = static_cast<StatusAwareArrayStream*>(stream->private_data);
    auto status = arrow::ExportSchema(*state->reader->schema(), out);
    if (!status.ok()) {
        state->last_error = status.ToString();
        return EIO;
    }
    return 0;
}

inline int
StatusAwareGetNext(struct ArrowArrayStream* stream, struct ArrowArray* out) {
    auto* state = static_cast<StatusAwareArrayStream*>(stream->private_data);
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = state->reader->ReadNext(&batch);
    if (!status.ok()) {
        state->last_error = status.ToString();
        return StreamErrnoForStatus(status);
    }
    if (batch == nullptr) {
        out->release = nullptr;
        return 0;
    }
    status = arrow::ExportRecordBatch(*batch, out);
    if (!status.ok()) {
        state->last_error = status.ToString();
        return EIO;
    }
    return 0;
}

inline const char*
StatusAwareGetLastError(struct ArrowArrayStream* stream) {
    auto* state = static_cast<StatusAwareArrayStream*>(stream->private_data);
    return state->last_error.c_str();
}

inline void
StatusAwareRelease(struct ArrowArrayStream* stream) {
    delete static_cast<StatusAwareArrayStream*>(stream->private_data);
    stream->private_data = nullptr;
    stream->release = nullptr;
}

}  // namespace detail

// Export `reader` as an ArrowArrayStream that keeps the producer's retryable
// classification in the stream errno (EAGAIN for retryable, EIO otherwise).
// The Status return mirrors arrow::ExportRecordBatchReader so callers keep the
// same error path.
//
// Classification obligation: the returned arrow::Status is consumed by
// ffi_reader_c.cpp, which owns the ErrorCode mapping (ArrowStatusToErrorCode /
// milvus_storage::ToSegcoreError); this header only translates the retryable
// bit into the stream errno.
inline arrow::Status
ExportStatusAwareRecordBatchReader(
    std::shared_ptr<arrow::RecordBatchReader> reader,
    struct ArrowArrayStream* out) {
    if (out == nullptr) {
        return arrow::Status::Invalid("null ArrowArrayStream output");
    }
    std::memset(out, 0, sizeof(struct ArrowArrayStream));
    if (reader == nullptr) {
        return arrow::Status::Invalid("null RecordBatchReader");
    }
    auto* state = new (std::nothrow)
        detail::StatusAwareArrayStream{std::move(reader), {}};
    if (state == nullptr) {
        return arrow::Status::OutOfMemory(
            "failed to allocate status-aware ArrowArrayStream");
    }
    out->get_schema = detail::StatusAwareGetSchema;
    out->get_next = detail::StatusAwareGetNext;
    out->get_last_error = detail::StatusAwareGetLastError;
    out->release = detail::StatusAwareRelease;
    out->private_data = state;
    return arrow::Status::OK();
}

}  // namespace milvus::storage
