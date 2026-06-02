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

// ═══════════════════════════════════════════════════════════════════════════
// Internal header — included ONLY from per-arch .cpp files (SimdFilterAvx2.cpp,
// SimdFilterAvx512.cpp, etc.). Each .cpp is compiled with arch-specific flags
// (e.g. -mavx2), so xsimd generates the correct instructions.
//
// Do NOT include this header from other headers or general .cpp files.
// ═══════════════════════════════════════════════════════════════════════════

#pragma once

#include <algorithm>
#include <cstring>
#include <vector>
#include <xsimd/xsimd.hpp>
#include "common/SimdUtil.h"

namespace milvus {
namespace exec {
namespace detail {

// Core SIMD filter kernel, parameterized on architecture.
// For each data[i], checks if data[i] is in vals[0..num_vals) and OR's
// the result bit into bitmap. vals must be sorted.
template <typename T, typename Arch>
void
filterChunkImpl(
    const T* data, int size, uint8_t* bitmap, const T* vals, int num_vals) {
    if (num_vals <= 0 || size <= 0) {
        return;
    }

    using Batch = xsimd::batch<T, Arch>;
    using BatchBool = xsimd::batch_bool<T, Arch>;
    constexpr int kLanes = Batch::size;

    // Pre-compute broadcast vectors (one per IN value).
    // Heap-allocated once per FilterChunk call. Acceptable because:
    //   - FilterChunk is called once per segment, not per row
    //   - num_vals is typically small (< 256, bounded by kSimdThreshold)
    //   - broadcast() is one SIMD instruction each
    std::vector<Batch> broadcast;
    broadcast.reserve(num_vals);
    for (int j = 0; j < num_vals; ++j) {
        broadcast.emplace_back(Batch::broadcast(vals[j]));
    }

    // Choose unroll factor to target 64-bit (8-byte) bitmap writes.
    // Cap at 8 to avoid excessive register pressure.
    constexpr int kMaxUnroll = 8;
    constexpr int kIdealUnroll = (kLanes >= 64) ? 1 : 64 / kLanes;
    constexpr int kUnroll =
        (kIdealUnroll <= kMaxUnroll) ? kIdealUnroll : kMaxUnroll;
    constexpr int kStep = kLanes * kUnroll;

    int i = 0;

    // ── Unrolled main loop ──────────────────────────────────────────────
    for (; i + kStep <= size; i += kStep) {
        Batch d[kUnroll];
        BatchBool acc[kUnroll];
        for (int u = 0; u < kUnroll; ++u) {
            d[u] = Batch::load_unaligned(data + i + u * kLanes);
            acc[u] = BatchBool(false);
        }

        for (const auto& bcast : broadcast) {
            for (int u = 0; u < kUnroll; ++u) {
                acc[u] = acc[u] | (d[u] == bcast);
            }
        }

        uint64_t combined = 0;
        for (int u = 0; u < kUnroll; ++u) {
            uint64_t m = milvus::toBitMask(acc[u]);
            combined |= (m << (u * kLanes));
        }

        if (combined == 0) {
            continue;
        }

        const int byte_pos = i / 8;
        if constexpr (kStep >= 64) {
            uint64_t word;
            std::memcpy(&word, bitmap + byte_pos, sizeof(word));
            word |= combined;
            std::memcpy(bitmap + byte_pos, &word, sizeof(word));
        } else if constexpr (kStep >= 32) {
            uint32_t word;
            std::memcpy(&word, bitmap + byte_pos, sizeof(word));
            word |= static_cast<uint32_t>(combined);
            std::memcpy(bitmap + byte_pos, &word, sizeof(word));
        } else if constexpr (kStep >= 16) {
            uint16_t word;
            std::memcpy(&word, bitmap + byte_pos, sizeof(word));
            word |= static_cast<uint16_t>(combined);
            std::memcpy(bitmap + byte_pos, &word, sizeof(word));
        } else {
            bitmap[byte_pos] |= static_cast<uint8_t>(combined);
        }
    }

    // ── Single-register remainder loop ──────────────────────────────────
    if constexpr (kUnroll > 1) {
        for (; i + kLanes <= size; i += kLanes) {
            auto d0 = Batch::load_unaligned(data + i);
            BatchBool acc0(false);
            for (const auto& bcast : broadcast) {
                acc0 = acc0 | (d0 == bcast);
            }
            uint64_t mask = milvus::toBitMask(acc0);
            if (mask) {
                const int byte_pos = i / 8;
                if constexpr (kLanes >= 32) {
                    uint32_t word;
                    std::memcpy(&word, bitmap + byte_pos, sizeof(word));
                    word |= static_cast<uint32_t>(mask);
                    std::memcpy(bitmap + byte_pos, &word, sizeof(word));
                } else if constexpr (kLanes >= 16) {
                    uint16_t word;
                    std::memcpy(&word, bitmap + byte_pos, sizeof(word));
                    word |= static_cast<uint16_t>(mask);
                    std::memcpy(bitmap + byte_pos, &word, sizeof(word));
                } else if constexpr (kLanes >= 8) {
                    bitmap[byte_pos] |= static_cast<uint8_t>(mask);
                } else {
                    const int bit_shift = i % 8;
                    uint64_t shifted = mask << bit_shift;
                    bitmap[byte_pos] |= static_cast<uint8_t>(shifted);
                    if ((kLanes + bit_shift) > 8) {
                        bitmap[byte_pos + 1] |=
                            static_cast<uint8_t>(shifted >> 8);
                    }
                }
            }
        }
    }

    // ── Scalar tail ─────────────────────────────────────────────────────
    for (; i < size; ++i) {
        if (std::binary_search(vals, vals + num_vals, data[i])) {
            bitmap[i / 8] |= static_cast<uint8_t>(1 << (i % 8));
        }
    }
}

}  // namespace detail
}  // namespace exec
}  // namespace milvus
