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

#include "storage/LoadConfig.h"

#include <atomic>
#include <mutex>
#include "storage/LoadAdmissionController.h"
#include "storage/LoadOverheadController.h"
#include "storage/ThreadPools.h"
#include "common/EasyAssert.h"

namespace milvus::storage {
namespace {

std::atomic<bool> g_async_load_enabled{false};
std::atomic<int64_t> g_async_load_read_window_size_bytes{
    kDefaultLoadFieldDataReadWindowSizeBytes};

}  // namespace

bool
LoadAsyncReadEnabled() {
    return g_async_load_enabled.load(std::memory_order_acquire);
}

void
SetLoadAsyncReadEnabled(const bool enabled) {
    static std::mutex mode_update_mutex;
    std::lock_guard lock(mode_update_mutex);
    if (enabled == LoadAsyncReadEnabled()) {
        return;
    }
    // Mode changes require quiescent loading; live mixed-mode rollout is unsupported.
    auto& admission = LoadAdmissionController::GetInstance();
    const auto limit =
        enabled ? admission.CapacitySlots()
                : static_cast<size_t>(ThreadPools::GetLoadExecutorWorkers());
    if (!ConfigureLoadOverheadControllers(
            limit, enabled ? admission.CapacityBytes() : 0)) {
        ThrowInfo(ErrorCode::UnexpectedError,
                  "Failed to configure loading overhead for load mode");
    }
    g_async_load_enabled.store(enabled, std::memory_order_release);
}

int64_t
LoadFieldDataReadWindowSizeBytes() {
    return g_async_load_read_window_size_bytes.load(std::memory_order_acquire);
}

void
SetLoadFieldDataReadWindowSizeBytes(const int64_t bytes) {
    const auto normalized_bytes =
        bytes > 0 ? bytes : kDefaultLoadFieldDataReadWindowSizeBytes;
    g_async_load_read_window_size_bytes.store(normalized_bytes,
                                              std::memory_order_release);
}

}  // namespace milvus::storage
