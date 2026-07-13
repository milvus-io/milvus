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

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace milvus {

// Bounded handoff buffer for Milvus-owned C++ storage boundaries. The Go
// receiver immediately folds these observations into the fixed histogram
// schema used by distributed storage profiles; raw observations never cross
// an internal RPC boundary.
struct StorageProfileSnapshot {
    static constexpr size_t kMaxReadObservations = 64;

    std::array<uint64_t, kMaxReadObservations> read_duration_nanos{};
    uint32_t read_count = 0;
    uint64_t read_completed_bytes = 0;
    uint64_t dropped_read_observations = 0;

    void
    ObserveRead(std::chrono::steady_clock::duration duration,
                uint64_t completed_bytes) {
        auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
                         duration)
                         .count();
        if (nanos <= 0) {
            nanos = 1;
        }
        read_completed_bytes += completed_bytes;
        if (read_count >= kMaxReadObservations) {
            ++dropped_read_observations;
            return;
        }
        read_duration_nanos[read_count++] = static_cast<uint64_t>(nanos);
    }

    void
    Merge(const StorageProfileSnapshot& other) {
        read_completed_bytes += other.read_completed_bytes;
        dropped_read_observations += other.dropped_read_observations;
        for (uint32_t i = 0; i < other.read_count; ++i) {
            if (read_count >= kMaxReadObservations) {
                ++dropped_read_observations;
                continue;
            }
            read_duration_nanos[read_count++] =
                other.read_duration_nanos[i];
        }
    }
};

}  // namespace milvus
