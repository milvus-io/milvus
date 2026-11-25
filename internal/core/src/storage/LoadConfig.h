// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>

namespace milvus::storage {

inline constexpr int64_t kDefaultLoadFieldDataReadWindowSizeBytes =
    16LL * 1024 * 1024;

// Selects asynchronous storage reads for supported field-data and index loaders.
[[nodiscard]] bool
LoadAsyncReadEnabled();

// Selects the load mode and refreshes its overhead policy.
// Call only while loading and resource-limit updates are quiescent; rebuild
// readers that captured the previous mode before resuming loading.
void
SetLoadAsyncReadEnabled(bool enabled);

// Returns the process-wide Storage V3 field-data read-window size in bytes.
[[nodiscard]] int64_t
LoadFieldDataReadWindowSizeBytes();

// Sets the read-window size; non-positive values restore the default.
void
SetLoadFieldDataReadWindowSizeBytes(int64_t bytes);

}  // namespace milvus::storage
