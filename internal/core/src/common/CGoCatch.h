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

#include <exception>

#include "common/EasyAssert.h"

// Shared catch tail for every extern "C" entry point that returns a CStatus.
//
// Two reasons it must be uniform:
//   1. FailureCStatus(&e) dynamic_casts to SegcoreError and keeps its typed
//      error code; hand-writing `status.error_code = UnexpectedError` instead
//      throws that code away and collapses every failure to 2001 at the cgo
//      boundary, defeating the knowhere/storage code mapping upstream of it.
//   2. catch (...) ensures a non-std::exception can never escape across the C
//      ABI boundary, which would otherwise terminate the process.
#define CGO_CATCH_AND_RETURN_CSTATUS                           \
    catch (const std::exception& e) {                          \
        return milvus::FailureCStatus(&e);                     \
    }                                                          \
    catch (...) {                                              \
        return milvus::FailureCStatus(milvus::UnexpectedError, \
                                      "unknown exception");    \
    }
