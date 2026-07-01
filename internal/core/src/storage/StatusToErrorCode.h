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

#include "arrow/status.h"
#include "common/EasyAssert.h"
#include "milvus-storage/common/extend_status.h"

namespace milvus::storage {

// Map an arrow::Status failure to a segcore ErrorCode so the policy survives to
// Go (pkg/util/merr/segcore.go) instead of every storage failure collapsing to
// UnexpectedError(2001) via AssertInfo.
//
// "Producer owns classification": milvus-storage classifies its own statuses in
// milvus_storage::ToSegcoreError, which first unwraps the structured
// ExtendStatusDetail to read the fine ExtendStatusCode (so a transient
// PackedStorageIO becomes the retriable StorageTransientError(2045), corrupt
// packed metadata/file becomes DataFormatBroken, etc.) and otherwise falls back
// to a coarse arrow-status mapping (IO -> retriable transient, OOM ->
// MemAllocateFailed, Invalid/Type/Key -> DataFormatBroken). We delegate here
// rather than keep a second, drifting copy of that mapping in milvus.
//
// This lives in its own light header (not Util.h, which pulls in the file
// managers and forms an include cycle with FileManager.h) so every storage
// site that turns an arrow::Status into an error can route through the single
// mapper.
inline ErrorCode
ArrowStatusToErrorCode(const arrow::Status& status) {
    return milvus_storage::ToSegcoreError(status).get_error_code();
}

}  // namespace milvus::storage
