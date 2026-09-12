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

namespace milvus::segcore::storagev2translator {

inline constexpr int64_t kDefaultStorageV2AsyncLoadReadWindowSizeBytes =
    16LL * 1024 * 1024;

// Historical StorageV2 names are retained for configuration compatibility;
// these accessors control the Storage V3 async-load pipeline.
[[nodiscard]] bool
StorageV2AsyncLoadEnabled();

// Atomically enables or disables async loading for subsequently built readers.
void
SetStorageV2AsyncLoadEnabled(bool enabled);

// Returns the process-wide Storage V3 read-window size in bytes.
[[nodiscard]] int64_t
StorageV2AsyncLoadReadWindowSizeBytes();

// Sets the read-window size; non-positive values restore the default.
void
SetStorageV2AsyncLoadReadWindowSizeBytes(int64_t bytes);

}  // namespace milvus::segcore::storagev2translator
