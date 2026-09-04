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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <vector>

#include "common/Common.h"
#include "common/EasyAssert.h"
#include "common/Utils.h"
#include "folly/CancellationToken.h"
#include "storage/ThreadPools.h"
#include "storage/TransientMemoryBudget.h"

namespace milvus::storage {

constexpr size_t kMinStreamSliceSize = 64 * 1024;
constexpr size_t kStreamSliceAlignment = 4 * 1024;
constexpr size_t kTailMergeGrace = 1 * 1024 * 1024;
constexpr size_t kFileStreamBufferMultiplier = 2;
// Encrypted reads may simultaneously retain ciphertext, decrypted plaintext,
// and the returned plaintext buffer.
constexpr size_t kEncryptedStreamBufferMultiplier = 3;

// Returns whether a positive slice size satisfies the stream alignment.
[[nodiscard]] constexpr bool
IsStreamSliceSizeAligned(size_t slice_size) noexcept {
    return slice_size > 0 && slice_size % kStreamSliceAlignment == 0;
}

// Returns the configured default size of one entry-stream slice.
[[nodiscard]] inline size_t
DefaultStreamSliceSize() {
    return DEFAULT_INDEX_FILE_SLICE_SIZE;
}

// Throws FollyCancel when the token requests cancellation.
inline void
ThrowIfCancelled(const folly::CancellationToken& cancellation_token,
                 const std::string& operation) {
    if (cancellation_token.isCancellationRequested()) {
        ThrowInfo(ErrorCode::FollyCancel, "{} cancelled", operation);
    }
}

// A slice read from a V3 entry. `error` carries an exception captured in the
// producer task so the consumer can rethrow instead of hanging.
struct StreamSliceResult {
    size_t slice_transient_bytes{0};
    std::vector<uint8_t> data;
    std::exception_ptr error = nullptr;
};

// Returns the largest plaintext task size after the tail-merge allowance.
[[nodiscard]] inline size_t
MaxEntryStreamTaskBytes() {
    return DefaultStreamSliceSize() + kTailMergeGrace;
}

// Estimates buffers retained while processing one entry-stream slice.
[[nodiscard]] inline size_t
EntryStreamTransientBytes(size_t stream_bytes, bool encrypted) {
    // This is the compatibility fallback for callers that cannot inspect a
    // concrete encrypted V3 directory. File-aware planning uses persisted
    // ciphertext slice sizes instead.
    const auto buffer_multiplier =
        encrypted ? kEncryptedStreamBufferMultiplier : size_t{1};
    return SaturatingMultiply(stream_bytes, buffer_multiplier);
}

// Caps the aggregate stream reservation by active workers and global budget.
[[nodiscard]] inline size_t
EntryStreamMaxTransientBytes(size_t total_transient_bytes,
                             size_t max_task_transient_bytes,
                             size_t live_worker_count = 0) {
    if (total_transient_bytes == 0 || max_task_transient_bytes == 0) {
        return 0;
    }

    const auto configured_threads =
        std::max(milvus::ComputeThreadPoolMaxThreads(
                     milvus::HIGH_PRIORITY_THREAD_CORE_COEFFICIENT.load()),
                 milvus::ComputeThreadPoolMaxThreads(
                     milvus::LOW_PRIORITY_THREAD_CORE_COEFFICIENT.load()));
    auto& high_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::HIGH);
    auto& low_pool =
        milvus::ThreadPools::GetThreadPool(milvus::ThreadPoolPriority::LOW);
    const auto max_tasks =
        std::max({static_cast<size_t>(configured_threads),
                  std::max(high_pool.GetThreadNum(), low_pool.GetThreadNum()),
                  live_worker_count});
    const auto pool_bound =
        SaturatingMultiply(max_task_transient_bytes, max_tasks);
    const auto capacity =
        TransientMemoryBudget::GetLoadTransientBudget().CapacityBytes();
    const auto budget_bound =
        capacity == 0 ? pool_bound
                      : std::min(std::max(capacity, max_task_transient_bytes),
                                 pool_bound);
    return std::min(total_transient_bytes, budget_bound);
}

}  // namespace milvus::storage
