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

#include <string>
#include <string_view>

#include "common/EasyAssert.h"

namespace milvus::tantivy {

// The rust binding renders every failure as "<Variant>: <detail>" -- see the
// Display impl of TantivyBindingError in
// tantivy-binding/src/error.rs -- so the variant prefix is a stable
// classification signal that survives the FFI boundary as a plain string.
//
// Producer audit (tantivy-binding/src, non-test):
//   InvalidArgument  94 sites, 91 of them under analyzer/* -- they parse the
//                    user-supplied analyzer_params from the collection schema
//                    ("unsupported tokenizer: X", unknown filter, bad option
//                    value), so this really is the caller's input.
//   IOError          produced from std::io::Error while reading/writing the
//                    local index files: a retry or a reroute to another
//                    replica can succeed.
//   JsonError        serde_json failed on a payload handed to the binding.
//   TantivyError /   tantivy's own error enum (corruption, schema mismatch,
//   TantivyErrorV5   lock contention, ...) and the binding's own catch-all.
//   InternalError    Both are server-side and not retriable; there is no
//                    finer code with a consumer today, so they keep the
//                    generic UnexpectedError rather than inventing one.
inline ErrorCode
TantivyErrorToErrorCode(std::string_view err) {
    auto starts_with = [err](std::string_view prefix) {
        return err.size() >= prefix.size() &&
               err.compare(0, prefix.size(), prefix) == 0;
    };

    if (starts_with("InvalidArgument:")) {
        return ErrorCode::InvalidParameter;
    }
    if (starts_with("IOError:")) {
        return ErrorCode::FileReadFailed;
    }
    if (starts_with("JsonError:")) {
        return ErrorCode::DataFormatBroken;
    }
    // TantivyError / TantivyErrorV5 / InternalError, and anything the binding
    // adds later: server-side, non-retriable, unclassified.
    return ErrorCode::UnexpectedError;
}

}  // namespace milvus::tantivy

// Check a RustResultWrapper and, on failure, throw a SegcoreError carrying the
// code implied by the rust error string. AssertInfo cannot be used here: it
// has no code parameter, so every rust failure -- including a malformed
// analyzer_params, which is the caller's fault -- reached the cgo boundary as
// UnexpectedError(2001).
//
// `res` must expose `result_->success` and `result_->error`; the message and
// its arguments are formatted exactly as AssertInfo did.
#define AssertTantivyOk(res, info, args...)                     \
    do {                                                        \
        if (!(res).result_->success) {                          \
            ThrowInfo(milvus::tantivy::TantivyErrorToErrorCode( \
                          (res).result_->error),                \
                      info,                                     \
                      ##args);                                  \
        }                                                       \
    } while (0)
