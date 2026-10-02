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

#include "common/EasyAssert.h"
#include "milvus-storage/common/extend_status.h"
#include "milvus-storage/ffi_c.h"
#include "milvus-storage/ffi_internal/ffi_error_code.h"

namespace milvus::storage {

// Map a LoonFFIResult::err_code to a segcore ErrorCode.
//
// The producer owns the classification wherever it has one: the values of
// milvus_storage::ExtendStatusCode ARE the LOON_* codes (extend_status.h
// spells them as `AwsErrorNotFound = LOON_AWS_ERROR_NOT_FOUND`, ...), so any
// code in that range is handed straight to the producer's own
// ExtendStatusCode -> ErrorCode mapping. Do not duplicate that table here; it
// is maintained with a no-default switch on the storage side precisely so the
// two cannot drift.
//
// What ExtendStatusCode does NOT cover is the low FFI band (LOON_SUCCESS=0 ..
// LOON_FILE_NOT_FOUND=12), which the C layer raises for its own generic
// failures. Those are classified here, once.
inline ErrorCode
LoonErrCodeToErrorCode(int err_code) {
    switch (err_code) {
        case LOON_INVALID_ARGS:
        case LOON_INVALID_PROPERTIES:
            // Neither is the API caller's fault: the args are milvus's own FFI
            // call arguments and the properties are built by
            // MakePropertiesFromStorageConfig out of indexpb.StorageConfig.
            // InvalidParameter carries InputError, which would make the proxy
            // abort cross-replica failover and blame the user for a milvus-side
            // or deployment defect.
            return ErrorCode::ConfigInvalid;
        case LOON_MEMORY_ERROR:
            return ErrorCode::MemAllocateFailed;
        case LOON_FILE_NOT_FOUND:
            return ErrorCode::ObjectNotExist;
        case LOON_NOT_SUPPORT:
            return ErrorCode::Unsupported;
        case LOON_ARROW_ERROR:
        case LOON_LOGICAL_ERROR:
        case LOON_GOT_EXCEPTION:
        case LOON_UNREACHABLE_ERROR:
        case LOON_FAULT_INJECT_ERROR:
            return ErrorCode::StorageError;
        default:
            break;
    }

    // Outside the hand-classified low band: if it is an ExtendStatusCode,
    // the producer's table classifies it; otherwise fall back to the
    // producer's own retryability probe rather than guessing -- an unknown
    // transient code must stay retriable and an unknown permanent one must
    // not masquerade as transient.
    auto extend = milvus_storage::ExtendStatusCodeFromInt(err_code);
    if (extend.has_value()) {
        return milvus_storage::ToSegcoreErrorCode(extend.value());
    }
    return loon_ffi_is_retryable_errcode(err_code) != 0
               ? ErrorCode::StorageTransientError
               : ErrorCode::StorageError;
}

}  // namespace milvus::storage
