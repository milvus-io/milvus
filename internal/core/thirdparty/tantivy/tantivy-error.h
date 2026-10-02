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

#pragma once

#include <cstdint>

#include "common/EasyAssert.h"
#include "tantivy-binding.h"

namespace milvus::tantivy {

// The rust binding carries a stable discriminant over the FFI boundary
// (RustResult.error_code, values of TantivyBindingErrorCode in
// tantivy-binding/src/error.rs) precisely so this classification does not
// depend on parsing the human-facing error text. TantivyError /
// TantivyErrorV5 are further discriminated on the rust side (IoError ->
// Io, DataCorruption/IncompatibleIndex -> DataCorruption, ...), so an I/O
// failure inside the engine is not lumped into the generic bucket.
//
// Producer audit (tantivy-binding/src, non-test):
//   InvalidArgument  94 sites, 91 of them under analyzer/* -- they parse the
//                    user-supplied analyzer_params from the collection schema
//                    ("unsupported tokenizer: X", unknown filter, bad option
//                    value), so this really is the caller's input.
//   Io               std::io::Error while reading/writing the local index
//                    files, plus tantivy's own IoError/OpenDirectoryError/
//                    OpenReadError: a retry or a reroute to another replica
//                    can succeed.
//   DataCorruption   tantivy failed to validate / deserialize persisted index
//                    data (DataCorruption, IncompatibleIndex).
//   Json             serde_json failed on a payload handed to the binding.
//   Tantivy/Internal server-side, not retriable; there is no finer code with
//                    a consumer today, so they keep the generic
//                    UnexpectedError rather than inventing one.
inline ErrorCode
TantivyErrorToErrorCode(TantivyBindingErrorCode code) {
    switch (code) {
        case TantivyBindingErrorCode::InvalidArgument:
            return ErrorCode::InvalidParameter;
        case TantivyBindingErrorCode::Io:
            return ErrorCode::FileReadFailed;
        case TantivyBindingErrorCode::DataCorruption:
            return ErrorCode::DataFormatBroken;
        case TantivyBindingErrorCode::Json:
            return ErrorCode::DataFormatBroken;
        case TantivyBindingErrorCode::Ok:
        case TantivyBindingErrorCode::Tantivy:
        case TantivyBindingErrorCode::Internal:
            break;
    }
    // Tantivy / Internal, and any discriminant the binding adds before this
    // switch learns about it: server-side, non-retriable, unclassified.
    return ErrorCode::UnexpectedError;
}

}  // namespace milvus::tantivy

// Check a RustResultWrapper and, on failure, throw a SegcoreError carrying the
// code implied by the rust discriminant. AssertInfo cannot be used here: it
// has no code parameter, so every rust failure -- including a malformed
// analyzer_params, which is the caller's fault -- reached the cgo boundary as
// UnexpectedError(2001).
//
// `res` must expose `result_->success`, `result_->error_code` and
// `result_->error`; the message and its arguments are formatted exactly as
// AssertInfo did.
#define AssertTantivyOk(res, info, args...)                     \
    do {                                                        \
        if (!(res).result_->success) {                          \
            ThrowInfo(milvus::tantivy::TantivyErrorToErrorCode( \
                          (res).result_->error_code),           \
                      info,                                     \
                      ##args);                                  \
        }                                                       \
    } while (0)
