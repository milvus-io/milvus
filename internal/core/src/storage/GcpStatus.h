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
#include "google/cloud/status.h"

namespace milvus::storage {

inline bool
IsRetryableGcpStatus(google::cloud::StatusCode code) {
    switch (code) {
        case google::cloud::StatusCode::kDeadlineExceeded:
        case google::cloud::StatusCode::kResourceExhausted:
        case google::cloud::StatusCode::kInternal:
        case google::cloud::StatusCode::kUnavailable:
            return true;
        default:
            return false;
    }
}

inline ErrorCode
GcpNativeStatusToErrorCode(google::cloud::StatusCode code) {
    if (IsRetryableGcpStatus(code)) {
        return ErrorCode::GcpNativeError;
    }
    if (code == google::cloud::StatusCode::kNotFound) {
        return ErrorCode::ObjectNotExist;
    }
    return ErrorCode::UnexpectedError;
}

}  // namespace milvus::storage
