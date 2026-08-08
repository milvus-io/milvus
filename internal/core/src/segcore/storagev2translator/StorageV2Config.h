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

#include <atomic>
#include <cstdint>

namespace milvus::segcore::storagev2translator {

inline constexpr int64_t kDefaultStorageV2AsyncLoadReadWindowSizeBytes =
    16LL * 1024 * 1024;

inline std::atomic<bool>&
storage_v2_async_load_enabled_atomic() {
    static std::atomic<bool> instance{false};
    return instance;
}

inline bool
StorageV2AsyncLoadEnabled() {
    return storage_v2_async_load_enabled_atomic().load(
        std::memory_order_acquire);
}

inline void
SetStorageV2AsyncLoadEnabled(bool enabled) {
    storage_v2_async_load_enabled_atomic().store(enabled,
                                                 std::memory_order_release);
}

inline std::atomic<int64_t>&
storage_v2_async_load_read_window_size_bytes_atomic() {
    static std::atomic<int64_t> instance{
        kDefaultStorageV2AsyncLoadReadWindowSizeBytes};
    return instance;
}

inline int64_t
StorageV2AsyncLoadReadWindowSizeBytes() {
    return storage_v2_async_load_read_window_size_bytes_atomic().load(
        std::memory_order_acquire);
}

inline void
SetStorageV2AsyncLoadReadWindowSizeBytes(int64_t bytes) {
    if (bytes < 0) {
        bytes = kDefaultStorageV2AsyncLoadReadWindowSizeBytes;
    }
    storage_v2_async_load_read_window_size_bytes_atomic().store(
        bytes, std::memory_order_release);
}

}  // namespace milvus::segcore::storagev2translator
