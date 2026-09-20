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

#include "segcore/storagev2translator/StorageV2Config.h"

#include <atomic>

namespace milvus::segcore::storagev2translator {
namespace {

std::atomic<bool> g_async_load_enabled{false};
std::atomic<int64_t> g_async_load_read_window_size_bytes{
    kDefaultStorageV2AsyncLoadReadWindowSizeBytes};

}  // namespace

bool
StorageV2AsyncLoadEnabled() {
    return g_async_load_enabled.load(std::memory_order_acquire);
}

void
SetStorageV2AsyncLoadEnabled(const bool enabled) {
    g_async_load_enabled.store(enabled, std::memory_order_release);
}

int64_t
StorageV2AsyncLoadReadWindowSizeBytes() {
    return g_async_load_read_window_size_bytes.load(std::memory_order_acquire);
}

void
SetStorageV2AsyncLoadReadWindowSizeBytes(const int64_t bytes) {
    const auto normalized_bytes =
        bytes > 0 ? bytes : kDefaultStorageV2AsyncLoadReadWindowSizeBytes;
    g_async_load_read_window_size_bytes.store(normalized_bytes,
                                              std::memory_order_release);
}

}  // namespace milvus::segcore::storagev2translator
